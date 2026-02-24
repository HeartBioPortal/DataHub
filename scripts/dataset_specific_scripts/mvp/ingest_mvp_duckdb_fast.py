#!/usr/bin/env python3
"""High-throughput MVP ingest directly into DuckDB.

This script is optimized for very large MVP drops. It avoids Python row-by-row
record conversion and executes normalization/dedup in DuckDB SQL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.adapters import PhenotypeMapper  # noqa: E402
from datahub.adapters.common import expand_input_paths  # noqa: E402


ANCESTRY_SQL = """
CASE ancestry_code
    WHEN 'ALL' THEN 'Total'
    WHEN 'AFR' THEN 'African'
    WHEN 'AMR' THEN 'Admixed American'
    WHEN 'ASJ' THEN 'Ashkenazi Jewish'
    WHEN 'EAS' THEN 'East Asian'
    WHEN 'EUR' THEN 'European'
    WHEN 'FIN' THEN 'Finnish'
    WHEN 'SAS' THEN 'South Asian'
    WHEN 'OTH' THEN 'Other'
    WHEN '' THEN NULL
    ELSE ancestry_code
END
"""


class MVPFastCheckpoint:
    """File-level checkpoint for resumable fast ingest runs."""

    VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.payload = self._default_payload()

    @classmethod
    def _default_payload(cls) -> dict[str, Any]:
        return {
            "version": cls.VERSION,
            "completed_files": {},
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
        if not isinstance(loaded.get("completed_files"), dict):
            loaded["completed_files"] = {}
        self.payload = loaded

    def save(self) -> None:
        self.payload["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.payload, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def reset(self) -> None:
        self.payload = self._default_payload()
        if self.path.exists():
            self.path.unlink()

    def completed(self) -> set[str]:
        completed = self.payload.get("completed_files", {})
        if not isinstance(completed, dict):
            return set()
        return set(completed.keys())

    def mark_completed(self, file_path: Path, rows_inserted: int) -> None:
        normalized = _normalized_file_path(file_path)
        token = _file_token(normalized)
        completed = self.payload.setdefault("completed_files", {})
        completed[normalized] = {
            "file_token": token,
            "rows_inserted": int(rows_inserted),
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self.save()


def _normalized_file_path(path: Path) -> str:
    return str(path.resolve())


def _file_token(normalized_path: str) -> str:
    return hashlib.sha1(normalized_path.encode("utf-8")).hexdigest()[:16]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fast MVP ingest into DuckDB (SQL-native normalization).",
    )
    parser.add_argument(
        "--input-path",
        action="append",
        dest="input_paths",
        required=True,
        help="Input file, directory, or glob. Repeat for multiple paths.",
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="DuckDB database path.",
    )
    parser.add_argument(
        "--table-name",
        default="mvp_association_points",
        help="Target DuckDB table name.",
    )
    parser.add_argument(
        "--dataset-id",
        default="hbp_mvp_association",
        help="Dataset identifier written into each row.",
    )
    parser.add_argument(
        "--source",
        default="million_veteran_program",
        help="Source label written into each row.",
    )
    parser.add_argument(
        "--phenotype-map-json",
        default=None,
        help="Optional JSON map for phenotype->dataset/category overrides.",
    )
    parser.add_argument(
        "--fallback-dataset-type",
        default="CVD",
        help="Dataset type used when phenotype is not mapped.",
    )
    parser.add_argument(
        "--dataset-types",
        default=None,
        help="Optional comma-separated dataset filter (for example: CVD,TRAIT).",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=None,
        help="Optional checkpoint path. Defaults to <db-path>.mvp_fast_checkpoint.json.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint resume and process all files.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset checkpoint before ingest starts.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=0,
        help="DuckDB thread count (0 keeps DuckDB default).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )
    return parser.parse_args()


def _load_mapper(args: argparse.Namespace) -> PhenotypeMapper:
    if args.phenotype_map_json:
        return PhenotypeMapper.from_json(
            args.phenotype_map_json,
            fallback_dataset_type=args.fallback_dataset_type,
        )
    try:
        return PhenotypeMapper.from_hbp_backend(
            fallback_dataset_type=args.fallback_dataset_type
        )
    except Exception:
        return PhenotypeMapper(mapping={}, fallback_dataset_type=args.fallback_dataset_type)


def _safe_table_name(table_name: str) -> str:
    if not table_name:
        raise ValueError("table name cannot be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if table_name[0].isdigit() or any(char not in allowed for char in table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    return table_name


def _prepare_mapping_rows(mapper: PhenotypeMapper) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for phenotype_slug, details in mapper.mapping.items():
        dataset_type, category = details
        rows.append(
            (
                phenotype_slug,
                str(dataset_type).upper(),
                PhenotypeMapper.normalize(category),
            )
        )
    return rows


def _create_mapping_table(connection: Any, mapper: PhenotypeMapper) -> None:
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE phenotype_map("
        "phenotype_slug VARCHAR, dataset_type VARCHAR, category VARCHAR)"
    )
    rows = _prepare_mapping_rows(mapper)
    if rows:
        connection.executemany(
            "INSERT INTO phenotype_map VALUES (?, ?, ?)",
            rows,
        )


def _build_sql(
    *,
    table_name: str,
    file_path: Path,
    dataset_id: str,
    source: str,
    fallback_dataset_type: str,
    include_dataset_types: set[str] | None,
) -> tuple[str, list[str]]:
    fallback = fallback_dataset_type.upper()
    include_filter = ""
    include_params: list[str] = []
    if include_dataset_types:
        placeholders = ",".join("?" for _ in sorted(include_dataset_types))
        include_filter = f"AND dataset_type IN ({placeholders})"
        include_params = sorted(include_dataset_types)

    sql = f"""
CREATE OR REPLACE TEMP MACRO _slug(v) AS
    lower(regexp_replace(replace(trim(coalesce(v, '')), '/', '_'), '\\\\s+', '_', 'g'));

CREATE OR REPLACE TEMP TABLE __mvp_file_rows AS
WITH raw AS (
    SELECT
        trim(gene_symbol) AS gene_id,
        trim(SNP_ID) AS variant_id,
        _slug(phenotype_description) AS phenotype,
        _slug(parent_phenotype) AS parent_slug,
        trim(ref) AS ref_allele,
        trim(alt) AS alt_allele,
        upper(trim(coalesce(ancestry, ''))) AS ancestry_code,
        TRY_CAST(af AS DOUBLE) AS af,
        TRY_CAST(pval AS DOUBLE) AS p_value,
        trim(phenotype_key) AS phenotype_key
    FROM read_csv_auto(?, header=true, compression='auto', all_varchar=true)
),
resolved AS (
    SELECT
        ? AS dataset_id,
        ? AS source,
        CASE
            WHEN upper(coalesce(pm.dataset_type, ?)) IN ('CVD', 'TRAIT')
                THEN upper(coalesce(pm.dataset_type, ?))
            ELSE ?
        END AS dataset_type,
        r.gene_id,
        r.variant_id,
        r.phenotype,
        coalesce(
            nullif(_slug(pm.category), ''),
            CASE
                WHEN r.parent_slug <> '' AND TRY_CAST(r.parent_slug AS BIGINT) IS NULL
                    THEN r.parent_slug
                ELSE ''
            END
        ) AS disease_category,
        CASE
            WHEN length(r.ref_allele) = 1 AND length(r.alt_allele) = 1 THEN 'SNP'
            WHEN r.ref_allele = '' OR r.alt_allele = '' THEN NULL
            ELSE 'INDEL'
        END AS variation_type,
        r.p_value,
        {ANCESTRY_SQL} AS ancestry,
        r.af,
        r.phenotype_key
    FROM raw AS r
    LEFT JOIN phenotype_map AS pm
        ON pm.phenotype_slug = r.phenotype
    WHERE r.gene_id <> '' AND r.variant_id <> '' AND r.phenotype <> ''
),
ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY dataset_type, gene_id, variant_id, phenotype, coalesce(ancestry, '')
            ORDER BY p_value ASC NULLS LAST
        ) AS rn
    FROM resolved
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
    NULL::VARCHAR AS clinical_significance,
    NULL::VARCHAR AS most_severe_consequence,
    p_value,
    ancestry,
    af AS ancestry_af,
    phenotype_key,
    ? AS source_file,
    now() AS ingested_at
FROM ranked
WHERE rn = 1
{include_filter};

INSERT INTO {table_name}
SELECT * FROM __mvp_file_rows;
"""
    params = [
        str(file_path),
        dataset_id,
        source,
        fallback,
        fallback,
        fallback,
        str(file_path),
        *include_params,
    ]
    return sql, params


def _ensure_target_table(connection: Any, table_name: str) -> None:
    connection.execute(
        f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    dataset_id VARCHAR,
    dataset_type VARCHAR,
    source VARCHAR,
    gene_id VARCHAR,
    variant_id VARCHAR,
    phenotype VARCHAR,
    disease_category VARCHAR,
    variation_type VARCHAR,
    clinical_significance VARCHAR,
    most_severe_consequence VARCHAR,
    p_value DOUBLE,
    ancestry VARCHAR,
    ancestry_af DOUBLE,
    phenotype_key VARCHAR,
    source_file VARCHAR,
    ingested_at TIMESTAMP
);
"""
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("datahub.mvp.fast")

    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "duckdb is required for ingest_mvp_duckdb_fast.py. Install DataHub requirements first."
        ) from exc

    started = time.perf_counter()
    table_name = _safe_table_name(args.table_name)

    input_files = sorted(expand_input_paths(args.input_paths), key=lambda item: str(item))
    if not input_files:
        raise ValueError("No MVP input files matched --input-path values.")

    include_dataset_types = (
        {item.strip().upper() for item in args.dataset_types.split(",") if item.strip()}
        if args.dataset_types
        else None
    )

    checkpoint_path = (
        Path(args.checkpoint_path)
        if args.checkpoint_path
        else Path(f"{args.db_path}.mvp_fast_checkpoint.json")
    )
    checkpoint = MVPFastCheckpoint(checkpoint_path)
    checkpoint.load()
    if args.reset_checkpoint:
        checkpoint.reset()
        logger.info("Checkpoint reset: %s", checkpoint_path)

    completed = checkpoint.completed() if not args.no_resume else set()
    pending_files = (
        [path for path in input_files if _normalized_file_path(path) not in completed]
        if not args.no_resume
        else input_files
    )
    skipped_files = len(input_files) - len(pending_files)

    logger.info(
        "Fast MVP ingest start: files=%d pending=%d skipped=%d db=%s table=%s",
        len(input_files),
        len(pending_files),
        skipped_files,
        args.db_path,
        table_name,
    )

    mapper = _load_mapper(args)

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(db_path))
    if args.threads > 0:
        connection.execute(f"PRAGMA threads={int(args.threads)}")

    _ensure_target_table(connection, table_name)
    _create_mapping_table(connection, mapper)

    total_inserted = 0
    processed_files = 0
    per_file_rows: dict[str, int] = {}

    try:
        for index, file_path in enumerate(pending_files, start=1):
            normalized = _normalized_file_path(file_path)
            token = _file_token(normalized)
            logger.info(
                "Fast ingest file %d/%d start: %s token=%s",
                index,
                len(pending_files),
                file_path,
                token,
            )

            sql, params = _build_sql(
                table_name=table_name,
                file_path=file_path,
                dataset_id=args.dataset_id,
                source=args.source,
                fallback_dataset_type=args.fallback_dataset_type,
                include_dataset_types=include_dataset_types,
            )
            connection.execute(sql, params)
            rows_inserted = int(
                connection.execute("SELECT COUNT(*) FROM __mvp_file_rows").fetchone()[0]
            )
            connection.execute("DROP TABLE IF EXISTS __mvp_file_rows")

            total_inserted += rows_inserted
            processed_files += 1
            per_file_rows[file_path.name] = rows_inserted

            if not args.no_resume:
                checkpoint.mark_completed(file_path, rows_inserted)

            logger.info(
                "Fast ingest file %d/%d complete: %s rows_inserted=%d",
                index,
                len(pending_files),
                file_path,
                rows_inserted,
            )
    finally:
        connection.close()

    elapsed = time.perf_counter() - started
    logger.info(
        (
            "Fast MVP ingest complete in %.2fs | files=%d processed=%d skipped=%d "
            "rows_inserted=%d"
        ),
        elapsed,
        len(input_files),
        processed_files,
        skipped_files,
        total_inserted,
    )

    print(
        json.dumps(
            {
                "engine": "duckdb_fast_ingest",
                "db_path": str(db_path),
                "table_name": table_name,
                "file_count": len(input_files),
                "processed_files": processed_files,
                "skipped_files": skipped_files,
                "rows_inserted": total_inserted,
                "resume_enabled": not args.no_resume,
                "checkpoint_path": str(checkpoint_path),
                "per_file_rows": per_file_rows,
                "elapsed_seconds": round(elapsed, 2),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
