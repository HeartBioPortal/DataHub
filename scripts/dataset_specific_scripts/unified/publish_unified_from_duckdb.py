#!/usr/bin/env python3
"""Publish unified legacy-compatible association JSON directly from DuckDB points.

This script avoids re-reading raw CSV files by streaming rows from DuckDB and
publishing staged JSON outputs with resume support.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.models import CanonicalRecord  # noqa: E402
from datahub.publishers import (  # noqa: E402
    LegacyAssociationPublisher,
    LegacyRedisPublisher,
    PhenotypeRollupPublisher,
)


@dataclass(frozen=True)
class GeneWorkUnit:
    """A publish unit for resumable staging."""

    dataset_type: str
    gene_id: str
    point_rows: int
    shard_id: int | None = None


class UnifiedPublishCheckpoint:
    """Checkpoint tracking completed publish units."""

    VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.payload = self._default_payload()

    @classmethod
    def _default_payload(cls) -> dict[str, Any]:
        return {
            "version": cls.VERSION,
            "completed_units": {},
            "updated_at": None,
        }

    def load(self) -> None:
        if not self.path.exists():
            self.payload = self._default_payload()
            return

        loaded = json.loads(self.path.read_text())
        if not isinstance(loaded, dict):
            self.payload = self._default_payload()
            return

        if int(loaded.get("version", 0)) != self.VERSION:
            self.payload = self._default_payload()
            return

        if not isinstance(loaded.get("completed_units"), dict):
            loaded["completed_units"] = {}

        self.payload = loaded

    def save(self) -> None:
        self.payload["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self.payload, indent=2, sort_keys=True))
        temp.replace(self.path)

    def reset(self) -> None:
        self.payload = self._default_payload()
        if self.path.exists():
            self.path.unlink()

    def completed(self) -> set[str]:
        completed = self.payload.get("completed_units", {})
        if not isinstance(completed, dict):
            return set()
        return set(completed.keys())

    def mark_completed(self, unit: GeneWorkUnit) -> None:
        key = _unit_key(unit)
        completed = self.payload.setdefault("completed_units", {})
        completed[key] = {
            "dataset_type": unit.dataset_type,
            "gene_id": unit.gene_id,
            "shard_id": int(unit.shard_id) if unit.shard_id is not None else None,
            "point_rows": int(unit.point_rows),
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self.save()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build unified legacy association JSON directly from DuckDB points "
            "(legacy + MVP) with source-priority dedup."
        )
    )
    parser.add_argument("--db-path", required=True, help="DuckDB database path.")
    parser.add_argument(
        "--source-table",
        default="mvp_association_points",
        help="Input points table containing MVP and legacy rows.",
    )
    parser.add_argument(
        "--working-table",
        default="association_points_unified",
        help="Materialized deduplicated working table name (used by global_table mode).",
    )
    parser.add_argument(
        "--dedup-mode",
        default="per_gene",
        choices=["per_gene", "global_table"],
        help=(
            "Dedup strategy: per_gene avoids full-table materialization and uses much less disk. "
            "global_table materializes a full working table first."
        ),
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Output root for legacy JSON payloads.",
    )
    parser.add_argument(
        "--source-priority",
        default="legacy_cvd_raw,legacy_trait_raw,million_veteran_program",
        help=(
            "Comma-separated source precedence (left is highest priority). "
            "Used when the same (dataset,gene,variant,phenotype,ancestry) appears in multiple sources."
        ),
    )
    parser.add_argument(
        "--dataset-types",
        default="CVD,TRAIT",
        help="Optional comma-separated dataset-type filter.",
    )
    parser.add_argument(
        "--rollup-tree-json",
        default=None,
        help="Optional phenotype tree JSON path for rollup grouping.",
    )
    parser.add_argument(
        "--disable-rollup",
        action="store_true",
        help="Skip rollup output generation.",
    )
    parser.add_argument(
        "--json-compression",
        default="gzip",
        choices=["none", "gzip"],
        help="Final JSON file compression mode.",
    )
    parser.add_argument(
        "--json-gzip-level",
        type=int,
        default=6,
        help="Gzip level for compressed JSON output.",
    )
    parser.add_argument(
        "--json-indent",
        type=int,
        default=None,
        help="Pretty-print indent; use 0 or negative for compact JSON.",
    )
    parser.add_argument(
        "--ancestry-precision",
        type=int,
        default=6,
        help="Round ancestry values to this precision.",
    )
    parser.add_argument(
        "--publish-batch-size",
        type=int,
        default=50_000,
        help="Canonical record batch size for staged publishers.",
    )
    parser.add_argument(
        "--query-chunk-rows",
        type=int,
        default=200_000,
        help="DuckDB fetchmany chunk size while streaming point rows.",
    )
    parser.add_argument(
        "--per-gene-shards",
        type=int,
        default=256,
        help=(
            "Shard count for per_gene dedup mode. Higher values reduce per-query payload "
            "size and increase unit count."
        ),
    )
    parser.add_argument(
        "--unit-partitions",
        type=int,
        default=1,
        help=(
            "Split publish units into this many deterministic partitions for parallel runs. "
            "Use >1 with multiple jobs."
        ),
    )
    parser.add_argument(
        "--unit-partition-index",
        type=int,
        default=0,
        help="0-based partition index to process when --unit-partitions > 1.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=None,
        help=(
            "Optional checkpoint path. "
            "Defaults to <output-root>/_datahub_state/unified_publish/checkpoint.json"
        ),
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help=(
            "Optional state directory for staging and checkpoint metadata. "
            "Defaults to <output-root>/_datahub_state/unified_publish"
        ),
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint-based resume.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset checkpoint metadata and remove stage state before run.",
    )
    parser.add_argument(
        "--keep-stage-files",
        action="store_true",
        help="Keep per-gene staged payload directories after merge.",
    )
    parser.add_argument(
        "--reset-output",
        action="store_true",
        help="Remove existing <output-root>/association/final before publishing.",
    )
    parser.add_argument(
        "--publish-redis",
        action="store_true",
        help="Publish generated legacy payloads into Redis.",
    )
    parser.add_argument(
        "--redis-strict",
        action="store_true",
        help="Fail fast if Redis publishing fails.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=0,
        help="DuckDB thread count (0 keeps DuckDB default).",
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="DuckDB memory limit. Use empty string to keep DuckDB default.",
    )
    parser.add_argument(
        "--temp-directory",
        default="/data/hbp/datamart/duckdb_tmp",
        help="DuckDB temp spill directory.",
    )
    parser.add_argument(
        "--max-temp-directory-size",
        default=None,
        help="Optional DuckDB PRAGMA max_temp_directory_size (for example: 100GiB).",
    )
    parser.add_argument(
        "--preserve-insertion-order",
        action="store_true",
        help="Keep DuckDB insertion-order preservation enabled (uses more memory).",
    )
    parser.add_argument(
        "--duckdb-progress-bar",
        dest="duckdb_progress_bar",
        action="store_true",
        help="Enable DuckDB internal progress bar output.",
    )
    parser.add_argument(
        "--no-duckdb-progress-bar",
        dest="duckdb_progress_bar",
        action="store_false",
        help="Disable DuckDB internal progress bar output.",
    )
    parser.set_defaults(duckdb_progress_bar=True)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )
    return parser.parse_args()


def _safe_table_name(table_name: str) -> str:
    if not table_name:
        raise ValueError("table name cannot be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if table_name[0].isdigit() or any(char not in allowed for char in table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    return table_name


def _unit_key(unit: GeneWorkUnit) -> str:
    if unit.shard_id is not None:
        return f"{unit.dataset_type}::shard::{int(unit.shard_id)}"
    return f"{unit.dataset_type}::gene::{unit.gene_id}"


def _unit_token(unit: GeneWorkUnit) -> str:
    return hashlib.sha1(_unit_key(unit).encode("utf-8")).hexdigest()[:16]


def _unit_bucket(unit: GeneWorkUnit, *, partitions: int) -> int:
    digest = hashlib.sha1(_unit_key(unit).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) % int(partitions)


def _select_partition_units(
    units: list[GeneWorkUnit],
    *,
    partitions: int,
    partition_index: int,
) -> list[GeneWorkUnit]:
    if int(partitions) <= 1:
        return units
    return [
        unit
        for unit in units
        if _unit_bucket(unit, partitions=partitions) == int(partition_index)
    ]


def _source_priority_rows(source_priority: str) -> list[tuple[str, int]]:
    rows: list[tuple[str, int]] = []
    rank = 1
    for source in source_priority.split(","):
        normalized = source.strip().lower()
        if not normalized:
            continue
        rows.append((normalized, rank))
        rank += 1
    return rows


def _dataset_type_filter(dataset_types: str | None) -> set[str] | None:
    if not dataset_types:
        return None
    parsed = {item.strip().upper() for item in dataset_types.split(",") if item.strip()}
    return parsed if parsed else None


def _unit_label(unit: GeneWorkUnit) -> str:
    if unit.shard_id is not None:
        return f"shard={int(unit.shard_id)}"
    return f"gene={unit.gene_id}"


def _gene_shard_expression(*, total_shards: int, dataset_alias: str = "p") -> str:
    if int(total_shards) < 1:
        raise ValueError("total_shards must be >= 1")
    return (
        f"CAST(mod(hash(trim(coalesce({dataset_alias}.gene_id, ''))), {int(total_shards)}) AS INTEGER)"
    )


def _ensure_db_runtime(connection: Any, args: argparse.Namespace, logger: logging.Logger) -> None:
    if args.threads > 0:
        connection.execute(f"PRAGMA threads={int(args.threads)}")
    if args.memory_limit:
        mem = str(args.memory_limit).strip().replace("'", "")
        if mem:
            connection.execute(f"SET memory_limit='{mem}'")
    if not args.preserve_insertion_order:
        connection.execute("SET preserve_insertion_order=false")
    if args.temp_directory:
        temp_dir = Path(args.temp_directory)
        temp_dir.mkdir(parents=True, exist_ok=True)
        safe_temp = str(temp_dir).replace("'", "")
        connection.execute(f"SET temp_directory='{safe_temp}'")
    if args.max_temp_directory_size:
        size_value = str(args.max_temp_directory_size).strip().replace("'", "")
        if size_value:
            connection.execute(f"PRAGMA max_temp_directory_size='{size_value}'")
    if args.duckdb_progress_bar:
        try:
            connection.execute("SET enable_progress_bar = true")
            try:
                connection.execute("SET enable_progress_bar_print = true")
            except Exception:
                pass
            logger.info("DuckDB internal progress bar enabled.")
        except Exception as exc:
            logger.warning("Could not enable DuckDB progress bar: %s", exc)


def _create_source_priority_table(
    connection: Any,
    *,
    source_priority: list[tuple[str, int]],
) -> None:
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE __source_priority(source VARCHAR, source_rank INTEGER)"
    )
    if source_priority:
        connection.executemany("INSERT INTO __source_priority VALUES (?, ?)", source_priority)


def _base_source_where_clause(*, dataset_types: set[str] | None) -> tuple[str, list[str]]:
    where_filters: list[str] = [
        "coalesce(trim(p.dataset_type), '') <> ''",
        "coalesce(trim(p.gene_id), '') <> ''",
        "coalesce(trim(p.variant_id), '') <> ''",
        "coalesce(trim(p.phenotype), '') <> ''",
    ]
    filter_params: list[str] = []

    if dataset_types:
        placeholders = ",".join("?" for _ in sorted(dataset_types))
        where_filters.append(f"upper(trim(p.dataset_type)) IN ({placeholders})")
        filter_params.extend(sorted(dataset_types))

    return " AND ".join(where_filters), filter_params


def _build_working_table(
    connection: Any,
    *,
    source_table: str,
    working_table: str,
    dataset_types: set[str] | None,
    logger: logging.Logger,
) -> tuple[int, int]:
    where_sql, filter_params = _base_source_where_clause(dataset_types=dataset_types)

    sql = f"""
CREATE OR REPLACE TABLE {working_table} AS
WITH ranked AS (
    SELECT
        p.dataset_id,
        upper(trim(p.dataset_type)) AS dataset_type,
        p.source,
        trim(p.gene_id) AS gene_id,
        trim(p.variant_id) AS variant_id,
        lower(regexp_replace(replace(trim(p.phenotype), '/', '_'), '\\s+', '_', 'g')) AS phenotype,
        coalesce(trim(p.disease_category), '') AS disease_category,
        nullif(trim(p.variation_type), '') AS variation_type,
        nullif(trim(p.clinical_significance), '') AS clinical_significance,
        nullif(trim(p.most_severe_consequence), '') AS most_severe_consequence,
        p.p_value,
        nullif(trim(coalesce(p.ancestry, '')), '') AS ancestry,
        p.ancestry_af,
        nullif(trim(coalesce(p.phenotype_key, '')), '') AS phenotype_key,
        p.source_file,
        p.ingested_at,
        coalesce(sp.source_rank, 999999) AS source_rank,
        row_number() OVER (
            PARTITION BY
                upper(trim(p.dataset_type)),
                trim(p.gene_id),
                trim(p.variant_id),
                lower(regexp_replace(replace(trim(p.phenotype), '/', '_'), '\\s+', '_', 'g')),
                nullif(trim(coalesce(p.ancestry, '')), '')
            ORDER BY
                coalesce(sp.source_rank, 999999),
                CASE WHEN p.p_value IS NULL THEN 1 ELSE 0 END,
                p.p_value,
                p.ingested_at DESC,
                p.source ASC
        ) AS rn
    FROM {source_table} p
    LEFT JOIN __source_priority sp
      ON lower(trim(p.source)) = sp.source
    WHERE {where_sql}
)
SELECT
    dataset_id,
    dataset_type,
    source,
    gene_id,
    variant_id,
    phenotype,
    disease_category,
    variation_type,
    clinical_significance,
    most_severe_consequence,
    p_value,
    ancestry,
    ancestry_af,
    phenotype_key,
    source_file,
    ingested_at
FROM ranked
WHERE rn = 1
ORDER BY dataset_type, gene_id, phenotype, variant_id, coalesce(ancestry, '');
"""

    connection.execute(sql, filter_params)

    row_count = int(connection.execute(f"SELECT COUNT(*) FROM {working_table}").fetchone()[0])
    gene_count = int(
        connection.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT dataset_type, gene_id FROM {working_table})"
        ).fetchone()[0]
    )
    logger.info(
        "Working table ready: table=%s rows=%d genes=%d",
        working_table,
        row_count,
        gene_count,
    )
    return row_count, gene_count


def _load_work_units(connection: Any, *, working_table: str) -> list[GeneWorkUnit]:
    rows = connection.execute(
        f"""
SELECT dataset_type, gene_id, COUNT(*) AS point_rows
FROM {working_table}
GROUP BY dataset_type, gene_id
ORDER BY dataset_type, gene_id
"""
    ).fetchall()

    return [
        GeneWorkUnit(
            dataset_type=str(item[0]),
            gene_id=str(item[1]),
            point_rows=int(item[2]),
        )
        for item in rows
    ]


def _load_work_units_from_source(
    connection: Any,
    *,
    source_table: str,
    dataset_types: set[str] | None,
    per_gene_shards: int,
) -> list[GeneWorkUnit]:
    where_sql, params = _base_source_where_clause(dataset_types=dataset_types)
    shard_expression = _gene_shard_expression(total_shards=per_gene_shards, dataset_alias="p")
    shard_padding = max(2, len(str(max(per_gene_shards - 1, 0))))
    rows = connection.execute(
        f"""
SELECT
    upper(trim(p.dataset_type)) AS dataset_type,
    {shard_expression} AS shard_id,
    COUNT(*) AS point_rows
FROM {source_table} p
WHERE {where_sql}
GROUP BY upper(trim(p.dataset_type)), {shard_expression}
ORDER BY upper(trim(p.dataset_type)), shard_id
""",
        params,
    ).fetchall()

    return [
        GeneWorkUnit(
            dataset_type=str(item[0]),
            gene_id=f"shard_{int(item[1]):0{shard_padding}d}",
            point_rows=int(item[2]),
            shard_id=int(item[1]),
        )
        for item in rows
    ]


def _open_unit_cursor_from_source(
    connection: Any,
    *,
    source_table: str,
    dataset_types: set[str] | None,
    unit: GeneWorkUnit,
    per_gene_shards: int,
) -> Any:
    where_sql, params = _base_source_where_clause(dataset_types=dataset_types)
    unit_filters = ["upper(trim(p.dataset_type)) = ?"]
    unit_params: list[Any] = [unit.dataset_type]

    if unit.shard_id is not None:
        shard_expression = _gene_shard_expression(total_shards=per_gene_shards, dataset_alias="p")
        unit_filters.append(f"{shard_expression} = ?")
        unit_params.append(int(unit.shard_id))
    else:
        unit_filters.append("trim(p.gene_id) = ?")
        unit_params.append(unit.gene_id)

    unit_where_sql = " AND ".join(unit_filters)

    query = f"""
WITH ranked AS (
    SELECT
        p.dataset_id,
        upper(trim(p.dataset_type)) AS dataset_type,
        p.source,
        trim(p.gene_id) AS gene_id,
        trim(p.variant_id) AS variant_id,
        lower(regexp_replace(replace(trim(p.phenotype), '/', '_'), '\\s+', '_', 'g')) AS phenotype,
        coalesce(trim(p.disease_category), '') AS disease_category,
        nullif(trim(p.variation_type), '') AS variation_type,
        nullif(trim(p.clinical_significance), '') AS clinical_significance,
        nullif(trim(p.most_severe_consequence), '') AS most_severe_consequence,
        p.p_value,
        nullif(trim(coalesce(p.ancestry, '')), '') AS ancestry,
        p.ancestry_af,
        nullif(trim(coalesce(p.phenotype_key, '')), '') AS phenotype_key,
        p.source_file,
        p.ingested_at,
        coalesce(sp.source_rank, 999999) AS source_rank,
        row_number() OVER (
            PARTITION BY
                upper(trim(p.dataset_type)),
                trim(p.gene_id),
                trim(p.variant_id),
                lower(regexp_replace(replace(trim(p.phenotype), '/', '_'), '\\s+', '_', 'g')),
                nullif(trim(coalesce(p.ancestry, '')), '')
            ORDER BY
                coalesce(sp.source_rank, 999999),
                CASE WHEN p.p_value IS NULL THEN 1 ELSE 0 END,
                p.p_value,
                p.ingested_at DESC,
                p.source ASC
        ) AS rn
    FROM {source_table} p
    LEFT JOIN __source_priority sp
      ON lower(trim(p.source)) = sp.source
    WHERE {where_sql}
      AND {unit_where_sql}
)
SELECT
    dataset_id,
    dataset_type,
    source,
    gene_id,
    variant_id,
    phenotype,
    disease_category,
    variation_type,
    clinical_significance,
    most_severe_consequence,
    p_value,
    ancestry,
    ancestry_af,
    phenotype_key,
    source_file
FROM ranked
WHERE rn = 1
ORDER BY gene_id, phenotype, variant_id, coalesce(ancestry, '');
"""

    return connection.execute(query, [*params, *unit_params])


def _build_stage_publishers(
    *,
    output_root: Path,
    disable_rollup: bool,
    rollup_tree_json: str | None,
    ancestry_precision: int,
    json_compression: str,
    json_gzip_level: int,
    json_indent: int | None,
) -> list[Any]:
    publishers: list[Any] = [
        LegacyAssociationPublisher(
            output_root=output_root,
            skip_unknown_axis_values=True,
            ancestry_value_precision=ancestry_precision,
            deduplicate_ancestry_points=True,
            incremental_merge=True,
            json_indent=json_indent,
            json_compression=json_compression,
            json_gzip_level=json_gzip_level,
        )
    ]

    if not disable_rollup:
        publishers.append(
            PhenotypeRollupPublisher(
                output_root=output_root,
                tree_json_path=rollup_tree_json,
                skip_unknown_axis_values=True,
                deduplicate_variants=True,
                ancestry_value_precision=ancestry_precision,
                incremental_merge=True,
                json_indent=json_indent,
                json_compression=json_compression,
                json_gzip_level=json_gzip_level,
            )
        )

    return publishers


def _apply_stage_output(*, stage_root: Path, output_root: Path) -> int:
    copied = 0
    for source_file in sorted(stage_root.rglob("*")):
        if not source_file.is_file():
            continue
        if not (source_file.name.endswith(".json") or source_file.name.endswith(".json.gz")):
            continue
        relative = source_file.relative_to(stage_root)
        target_file = output_root / relative
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        copied += 1
    return copied


def _record_key(row: tuple[Any, ...]) -> tuple[str, str, str, str]:
    return (
        str(row[1]),
        str(row[3]),
        str(row[4]),
        str(row[5]),
    )


def _new_record(row: tuple[Any, ...]) -> CanonicalRecord:
    metadata: dict[str, Any] = {}
    if row[13] is not None and str(row[13]).strip():
        metadata["phenotype_key"] = str(row[13]).strip()
    if row[14] is not None and str(row[14]).strip():
        metadata["source_file"] = str(row[14]).strip()

    record = CanonicalRecord(
        dataset_id=str(row[0]),
        dataset_type=str(row[1]),
        source=str(row[2]),
        gene_id=str(row[3]),
        variant_id=str(row[4]),
        phenotype=str(row[5]),
        disease_category=str(row[6]) if row[6] is not None else "",
        variation_type=str(row[7]) if row[7] is not None else None,
        clinical_significance=str(row[8]) if row[8] is not None else None,
        most_severe_consequence=str(row[9]) if row[9] is not None else None,
        p_value=float(row[10]) if row[10] is not None else None,
        ancestry={},
        metadata=metadata,
    )

    _merge_row_into_record(record, row)
    return record


def _merge_row_into_record(record: CanonicalRecord, row: tuple[Any, ...]) -> None:
    pval = float(row[10]) if row[10] is not None else None
    if record.p_value is None:
        record.p_value = pval
    elif pval is not None and pval < record.p_value:
        record.p_value = pval

    for attr_index, attr_name in (
        (6, "disease_category"),
        (7, "variation_type"),
        (8, "clinical_significance"),
        (9, "most_severe_consequence"),
    ):
        current = getattr(record, attr_name)
        if current is None or (isinstance(current, str) and not current.strip()):
            candidate = row[attr_index]
            if candidate is not None and str(candidate).strip():
                setattr(record, attr_name, str(candidate).strip())

    ancestry = str(row[11]).strip() if row[11] is not None else ""
    ancestry_af = row[12]
    if ancestry and ancestry_af is not None:
        try:
            record.ancestry[ancestry] = float(ancestry_af)
        except (TypeError, ValueError):
            pass


def _batched_publish(
    *,
    records: list[CanonicalRecord],
    publishers: Iterable[Any],
) -> None:
    if not records:
        return
    for publisher in publishers:
        publisher.publish(records)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("datahub.unified.publish")

    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "duckdb is required for publish_unified_from_duckdb.py. Install DataHub requirements first."
        ) from exc

    if args.publish_batch_size < 1:
        raise ValueError("publish batch size must be >= 1")
    if args.query_chunk_rows < 1:
        raise ValueError("query chunk rows must be >= 1")
    json_indent = args.json_indent if args.json_indent is not None and args.json_indent > 0 else None

    source_table = _safe_table_name(args.source_table)
    working_table = _safe_table_name(args.working_table)
    source_priority = _source_priority_rows(args.source_priority)
    dataset_types = _dataset_type_filter(args.dataset_types)

    if args.per_gene_shards < 1:
        raise ValueError("--per-gene-shards must be >= 1")
    if args.unit_partitions < 1:
        raise ValueError("--unit-partitions must be >= 1")
    if args.unit_partition_index < 0 or args.unit_partition_index >= args.unit_partitions:
        raise ValueError(
            "--unit-partition-index must be in [0, --unit-partitions)."
        )
    if args.unit_partitions > 1 and args.reset_output:
        raise ValueError(
            "--reset-output cannot be used with --unit-partitions > 1. "
            "Clear output once before launching parallel partition jobs."
        )

    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    state_root_base = (
        Path(args.state_dir)
        if args.state_dir
        else output_root / "_datahub_state" / "unified_publish"
    )
    partition_suffix = (
        f"part{int(args.unit_partition_index):03d}of{int(args.unit_partitions):03d}"
    )
    state_root = (
        state_root_base / partition_suffix
        if args.unit_partitions > 1
        else state_root_base
    )
    if args.checkpoint_path:
        checkpoint_base = Path(args.checkpoint_path)
        if args.unit_partitions > 1:
            checkpoint_path = checkpoint_base.with_name(
                f"{checkpoint_base.stem}.{partition_suffix}{checkpoint_base.suffix}"
            )
        else:
            checkpoint_path = checkpoint_base
    else:
        checkpoint_path = state_root / "checkpoint.json"
    staging_root = state_root / "staging"

    checkpoint = UnifiedPublishCheckpoint(checkpoint_path)
    checkpoint.load()
    if args.reset_checkpoint:
        checkpoint.reset()
        if staging_root.exists():
            shutil.rmtree(staging_root)
        logger.info("Checkpoint reset: %s", checkpoint_path)

    final_root = output_root / "association" / "final"
    if args.reset_output and final_root.exists():
        shutil.rmtree(final_root)
        logger.warning("Removed existing output tree: %s", final_root)

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    started = time.perf_counter()
    connection = duckdb.connect(str(db_path))
    _ensure_db_runtime(connection, args, logger)

    logger.info(
        "Unified publish start: db=%s source_table=%s dedup_mode=%s working_table=%s output_root=%s",
        db_path,
        source_table,
        args.dedup_mode,
        working_table,
        output_root,
    )
    logger.info(
        "Unit partitioning: partitions=%d index=%d",
        args.unit_partitions,
        args.unit_partition_index,
    )
    logger.info(
        "Source priority: %s",
        [source for source, _rank in source_priority],
    )
    logger.info(
        "Dataset filter: %s",
        sorted(dataset_types) if dataset_types else "ALL",
    )
    logger.info(
        "JSON output settings: compression=%s gzip_level=%d indent=%s",
        args.json_compression,
        args.json_gzip_level,
        str(json_indent),
    )

    _create_source_priority_table(connection, source_priority=source_priority)

    row_count: int | None = None
    if args.dedup_mode == "global_table":
        row_count, gene_count = _build_working_table(
            connection,
            source_table=source_table,
            working_table=working_table,
            dataset_types=dataset_types,
            logger=logger,
        )
        if row_count == 0 or gene_count == 0:
            logger.warning("No rows found in working table after filters. Nothing to publish.")
            connection.close()
            return 0
        all_units = _load_work_units(connection, working_table=working_table)
    else:
        all_units = _load_work_units_from_source(
            connection,
            source_table=source_table,
            dataset_types=dataset_types,
            per_gene_shards=args.per_gene_shards,
        )
        if not all_units:
            logger.warning("No rows found in source table after filters. Nothing to publish.")
            connection.close()
            return 0
        logger.info(
            (
                "Per-gene dedup mode enabled (sharded): skipping materialized working "
                "table build. units=%d shards=%d"
            ),
            len(all_units),
            args.per_gene_shards,
        )
    selected_units = _select_partition_units(
        all_units,
        partitions=args.unit_partitions,
        partition_index=args.unit_partition_index,
    )
    completed = checkpoint.completed() if not args.no_resume else set()
    pending_units = [unit for unit in selected_units if _unit_key(unit) not in completed]
    skipped_units = len(selected_units) - len(pending_units)

    logger.info(
        "Publish units: selected_total=%d pending=%d skipped=%d (all_units=%d)",
        len(selected_units),
        len(pending_units),
        skipped_units,
        len(all_units),
    )

    processed_units = 0
    total_point_rows = 0
    total_canonical_records = 0
    total_stage_files = 0

    try:
        for index, unit in enumerate(pending_units, start=1):
            token = _unit_token(unit)
            stage_root = staging_root / token
            if stage_root.exists():
                logger.warning("Removing stale stage directory: %s", stage_root)
                shutil.rmtree(stage_root)
            stage_root.mkdir(parents=True, exist_ok=True)

            logger.info(
                (
                    "Unit %d/%d start: dataset_type=%s %s point_rows=%d token=%s"
                ),
                index,
                len(pending_units),
                unit.dataset_type,
                _unit_label(unit),
                unit.point_rows,
                token,
            )

            staged_publishers = _build_stage_publishers(
                output_root=stage_root,
                disable_rollup=args.disable_rollup,
                rollup_tree_json=args.rollup_tree_json,
                ancestry_precision=args.ancestry_precision,
                json_compression=args.json_compression,
                json_gzip_level=args.json_gzip_level,
                json_indent=json_indent,
            )

            if args.dedup_mode == "global_table":
                cursor = connection.execute(
                    f"""
SELECT
    dataset_id,
    dataset_type,
    source,
    gene_id,
    variant_id,
    phenotype,
    disease_category,
    variation_type,
    clinical_significance,
    most_severe_consequence,
    p_value,
    ancestry,
    ancestry_af,
    phenotype_key,
    source_file
FROM {working_table}
WHERE dataset_type = ? AND gene_id = ?
ORDER BY phenotype, variant_id, coalesce(ancestry, '')
""",
                    [unit.dataset_type, unit.gene_id],
                )
            else:
                cursor = _open_unit_cursor_from_source(
                    connection,
                    source_table=source_table,
                    dataset_types=dataset_types,
                    unit=unit,
                    per_gene_shards=args.per_gene_shards,
                )

            current_record: CanonicalRecord | None = None
            current_key: tuple[str, str, str, str] | None = None
            publish_batch: list[CanonicalRecord] = []
            scanned_rows = 0
            published_for_unit = 0

            while True:
                rows = cursor.fetchmany(args.query_chunk_rows)
                if not rows:
                    break

                scanned_rows += len(rows)
                for row in rows:
                    row_key = _record_key(row)
                    if current_key is None:
                        current_key = row_key
                        current_record = _new_record(row)
                        continue

                    if row_key != current_key:
                        if current_record is not None:
                            publish_batch.append(current_record)
                            if len(publish_batch) >= args.publish_batch_size:
                                _batched_publish(records=publish_batch, publishers=staged_publishers)
                                published_for_unit += len(publish_batch)
                                total_canonical_records += len(publish_batch)
                                publish_batch = []
                        current_key = row_key
                        current_record = _new_record(row)
                    else:
                        if current_record is not None:
                            _merge_row_into_record(current_record, row)

                if scanned_rows % max(args.query_chunk_rows, 1) == 0:
                    logger.info(
                        "Unit %d/%d progress: %s scanned_rows=%d published=%d",
                        index,
                        len(pending_units),
                        _unit_label(unit),
                        scanned_rows,
                        published_for_unit,
                    )

            if current_record is not None:
                publish_batch.append(current_record)

            if publish_batch:
                _batched_publish(records=publish_batch, publishers=staged_publishers)
                published_for_unit += len(publish_batch)
                total_canonical_records += len(publish_batch)

            stage_files = _apply_stage_output(stage_root=stage_root, output_root=output_root)
            total_stage_files += stage_files

            if not args.no_resume:
                checkpoint.mark_completed(unit)

            processed_units += 1
            total_point_rows += scanned_rows

            logger.info(
                (
                    "Unit %d/%d complete: dataset_type=%s %s scanned_rows=%d "
                    "canonical_records=%d stage_files=%d"
                ),
                index,
                len(pending_units),
                unit.dataset_type,
                _unit_label(unit),
                scanned_rows,
                published_for_unit,
                stage_files,
            )

            if not args.keep_stage_files and stage_root.exists():
                shutil.rmtree(stage_root, ignore_errors=True)
    finally:
        connection.close()

    if args.publish_redis:
        logger.info("Redis publish start (final output snapshot).")
        LegacyRedisPublisher(output_root=output_root, strict=args.redis_strict).publish([])
        logger.info("Redis publish complete.")

    elapsed = time.perf_counter() - started
    logger.info(
        (
            "Unified publish complete in %.2fs | units=%d processed=%d skipped=%d "
            "point_rows=%d canonical_records=%d stage_files=%d"
        ),
        elapsed,
        len(selected_units),
        processed_units,
        skipped_units,
        total_point_rows,
        total_canonical_records,
        total_stage_files,
    )

    print(
        json.dumps(
            {
                "engine": "duckdb_unified_publish",
                "db_path": str(db_path),
                "source_table": source_table,
                "working_table": working_table,
                "dedup_mode": args.dedup_mode,
                "output_root": str(output_root),
                "json_compression": args.json_compression,
                "json_gzip_level": args.json_gzip_level,
                "json_indent": json_indent,
                "source_priority": [source for source, _rank in source_priority],
                "dataset_filter": sorted(dataset_types) if dataset_types else [],
                "working_table_rows": row_count,
                "unit_count": len(selected_units),
                "processed_units": processed_units,
                "skipped_units": skipped_units,
                "gene_count": len(selected_units),
                "processed_genes": processed_units,
                "skipped_genes": skipped_units,
                "per_gene_shards": args.per_gene_shards if args.dedup_mode == "per_gene" else None,
                "unit_partitions": args.unit_partitions,
                "unit_partition_index": args.unit_partition_index,
                "all_unit_count": len(all_units),
                "point_rows_scanned": total_point_rows,
                "canonical_records_published": total_canonical_records,
                "stage_files_applied": total_stage_files,
                "resume_enabled": not args.no_resume,
                "checkpoint_path": str(checkpoint_path),
                "state_dir": str(state_root),
                "elapsed_seconds": round(elapsed, 2),
            },
            indent=2,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
