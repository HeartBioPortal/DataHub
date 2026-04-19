#!/usr/bin/env python3
"""Add API-summary tables to an existing association serving DuckDB in place."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover - runtime guard
    duckdb = None

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from datahub.serving_summary import shape_summary_for_table


SUMMARY_TABLES = {
    "association_gene_payloads": "association_summary_payloads",
    "overall_gene_payloads": "overall_summary_payloads",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create lightweight summary tables inside an existing association "
            "serving DuckDB without rebuilding the full DataHub pipeline."
        )
    )
    parser.add_argument("--db-path", required=True, help="Existing association_serving.duckdb path.")
    parser.add_argument(
        "--tables",
        default="association,overall",
        help="Comma-separated source families to upgrade: association, overall.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Pending key batch size. Payload JSON is still loaded one row at a time.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional maximum rows to create per source table in this run. 0 means no limit.",
    )
    parser.add_argument(
        "--unit-partitions",
        type=int,
        default=1,
        help="Deterministically split genes into this many partitions for resumable Slurm runs.",
    )
    parser.add_argument(
        "--unit-partition-index",
        type=int,
        default=0,
        help="Zero-based partition index to process when --unit-partitions is greater than 1.",
    )
    parser.add_argument(
        "--replace-summary-tables",
        action="store_true",
        help="Drop existing summary tables before rebuilding them.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Log progress after this many inserted summary rows.",
    )
    parser.add_argument(
        "--slow-payload-seconds",
        type=float,
        default=30.0,
        help="Log any single payload that takes at least this many seconds to summarize.",
    )
    parser.add_argument(
        "--payload-source",
        default="auto",
        choices=["auto", "source-path", "duckdb"],
        help=(
            "Where to read full payloads from. auto prefers source_path JSON/JSON.GZ "
            "files when present and falls back to DuckDB payload_json."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=0,
        help="Optional DuckDB PRAGMA threads value. 0 keeps DuckDB default.",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        default="",
        help="Optional DuckDB PRAGMA memory_limit value, for example 64GB.",
    )
    parser.add_argument(
        "--duckdb-temp-directory",
        default="",
        help="Optional DuckDB PRAGMA temp_directory path for spill files.",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("datahub.serving.upgrade")


def _safe_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {value}")
    return value


def _create_summary_table(connection: Any, table_name: str) -> None:
    table_name = _safe_identifier(table_name)
    connection.execute(
        f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
    )


def _create_metadata_table(connection: Any) -> None:
    connection.execute(
        """
CREATE TABLE IF NOT EXISTS serving_summary_metadata (
    built_at TIMESTAMP,
    association_summary_row_count BIGINT,
    overall_summary_row_count BIGINT,
    source VARCHAR
)
"""
    )


def _drop_summary_table(connection: Any, table_name: str) -> None:
    table_name = _safe_identifier(table_name)
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")


def _partition_predicate(
    *,
    unit_partitions: int,
    unit_partition_index: int,
    alias: str = "p",
) -> tuple[str, list[Any]]:
    if unit_partitions <= 1:
        return "", []
    return (
        f"AND (hash({alias}.gene_id_normalized) % ?) = ?",
        [int(unit_partitions), int(unit_partition_index)],
    )


def _count_pending(
    connection: Any,
    *,
    source_table: str,
    summary_table: str,
    unit_partitions: int = 1,
    unit_partition_index: int = 0,
) -> int:
    source_table = _safe_identifier(source_table)
    summary_table = _safe_identifier(summary_table)
    partition_sql, partition_params = _partition_predicate(
        unit_partitions=unit_partitions,
        unit_partition_index=unit_partition_index,
    )
    row = connection.execute(
        f"""
SELECT count(*)
FROM {source_table} p
WHERE NOT EXISTS (
    SELECT 1
    FROM {summary_table} s
    WHERE s.dataset_type = p.dataset_type
      AND s.gene_id_normalized = p.gene_id_normalized
)
{partition_sql}
""",
        partition_params,
    ).fetchone()
    return int(row[0] if row else 0)


def _create_index(connection: Any, *, table_name: str, index_name: str) -> None:
    table_name = _safe_identifier(table_name)
    index_name = _safe_identifier(index_name)
    try:
        connection.execute(
            f"CREATE INDEX {index_name} ON {table_name} (dataset_type, gene_id_normalized)"
        )
    except Exception as exc:  # DuckDB lacks IF NOT EXISTS for some index versions.
        message = str(exc).lower()
        if "already exists" not in message:
            raise


def _mark_summary_complete(connection: Any) -> None:
    _create_metadata_table(connection)
    connection.execute("DELETE FROM serving_summary_metadata")
    connection.execute(
        """
INSERT INTO serving_summary_metadata
SELECT
    current_timestamp,
    (SELECT count(*) FROM association_summary_payloads),
    (SELECT count(*) FROM overall_summary_payloads),
    'upgrade_association_serving_duckdb.py'
"""
    )


def _all_summary_tables_complete(connection: Any) -> bool:
    for source_table, summary_table in SUMMARY_TABLES.items():
        if _count_pending(
            connection,
            source_table=source_table,
            summary_table=summary_table,
        ):
            return False
    return True


def _fetch_pending_keys(
    connection: Any,
    *,
    source_table: str,
    summary_table: str,
    limit: int,
    unit_partitions: int,
    unit_partition_index: int,
) -> list[tuple[str, str, str, str]]:
    source_table = _safe_identifier(source_table)
    summary_table = _safe_identifier(summary_table)
    partition_sql, partition_params = _partition_predicate(
        unit_partitions=unit_partitions,
        unit_partition_index=unit_partition_index,
    )
    return connection.execute(
        f"""
SELECT
    p.dataset_type,
    p.gene_id,
    p.gene_id_normalized,
    p.source_path
FROM {source_table} p
WHERE NOT EXISTS (
    SELECT 1
    FROM {summary_table} s
    WHERE s.dataset_type = p.dataset_type
      AND s.gene_id_normalized = p.gene_id_normalized
)
{partition_sql}
ORDER BY p.dataset_type, p.gene_id_normalized
LIMIT ?
""",
        [*partition_params, int(limit)],
    ).fetchall()


def _fetch_payload_json(
    connection: Any,
    *,
    source_table: str,
    dataset_type: str,
    gene_id_normalized: str,
) -> str | None:
    source_table = _safe_identifier(source_table)
    row = connection.execute(
        f"""
SELECT payload_json
FROM {source_table}
WHERE dataset_type = ? AND gene_id_normalized = ?
LIMIT 1
""",
        [dataset_type, gene_id_normalized],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _read_payload_text_from_path(source_path: str | None) -> str | None:
    if not source_path:
        return None
    path_text = str(source_path).strip()
    if not path_text or ";" in path_text:
        return None
    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return None
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return handle.read()
    return path.read_text(encoding="utf-8")


def _load_payload_json(
    connection: Any,
    *,
    source_table: str,
    dataset_type: str,
    gene_id_normalized: str,
    source_path: str | None,
    payload_source: str,
) -> tuple[str, str]:
    if payload_source in {"auto", "source-path"}:
        payload_json = _read_payload_text_from_path(source_path)
        if payload_json is not None:
            return payload_json, "source_path"
        if payload_source == "source-path":
            raise FileNotFoundError(
                f"Payload source_path is not readable for {source_table} "
                f"{dataset_type}/{gene_id_normalized}: {source_path}"
            )

    payload_json = _fetch_payload_json(
        connection,
        source_table=source_table,
        dataset_type=dataset_type,
        gene_id_normalized=gene_id_normalized,
    )
    if payload_json is None:
        raise RuntimeError(
            f"Missing payload_json for {source_table} {dataset_type}/{gene_id_normalized}"
        )
    return payload_json, "duckdb"


def _upgrade_table(
    connection: Any,
    *,
    source_table: str,
    summary_table: str,
    batch_size: int,
    max_rows: int,
    unit_partitions: int,
    unit_partition_index: int,
    progress_interval: int,
    slow_payload_seconds: float,
    payload_source: str,
    logger: logging.Logger,
) -> int:
    source_table = _safe_identifier(source_table)
    summary_table = _safe_identifier(summary_table)
    pending = _count_pending(
        connection,
        source_table=source_table,
        summary_table=summary_table,
        unit_partitions=unit_partitions,
        unit_partition_index=unit_partition_index,
    )
    logger.info(
        "Summary upgrade start: source=%s summary=%s pending_rows=%d partition=%d/%d max_rows=%s",
        source_table,
        summary_table,
        pending,
        unit_partition_index,
        unit_partitions,
        max_rows if max_rows > 0 else "ALL",
    )
    if pending == 0:
        return 0

    pending_limit = min(pending, max_rows) if max_rows > 0 else pending
    keys = _fetch_pending_keys(
        connection,
        source_table=source_table,
        summary_table=summary_table,
        limit=pending_limit,
        unit_partitions=unit_partitions,
        unit_partition_index=unit_partition_index,
    )
    write_cursor = connection.cursor()
    inserted = 0
    insert_sql = f"INSERT INTO {summary_table} VALUES (?, ?, ?, ?, ?)"
    next_log = progress_interval if progress_interval > 0 else 0
    payload_source_counts = {"source_path": 0, "duckdb": 0}
    summary_rows = []

    for dataset_type, gene_id, gene_id_normalized, source_path in keys:
        payload_started = time.perf_counter()
        payload_json, loaded_from = _load_payload_json(
            connection,
            source_table=source_table,
            dataset_type=str(dataset_type),
            gene_id_normalized=str(gene_id_normalized),
            source_path=str(source_path or ""),
            payload_source=payload_source,
        )
        payload_source_counts[loaded_from] += 1
        payload = json.loads(str(payload_json))
        summary_payload = shape_summary_for_table(
            payload,
            source_table=source_table,
            dataset_type=str(dataset_type),
        )
        summary_rows.append(
            (
                str(dataset_type),
                str(gene_id),
                str(gene_id_normalized),
                json.dumps(summary_payload, separators=(",", ":")),
                str(source_path or ""),
            )
        )
        del payload
        del payload_json
        del summary_payload
        payload_elapsed = time.perf_counter() - payload_started
        if slow_payload_seconds > 0 and payload_elapsed >= slow_payload_seconds:
            logger.info(
                "Slow payload summarized: source=%s dataset_type=%s gene=%s elapsed=%.2fs loaded_from=%s source_path=%s",
                source_table,
                dataset_type,
                gene_id_normalized,
                payload_elapsed,
                loaded_from,
                source_path,
            )

        if len(summary_rows) >= max(batch_size, 1):
            write_cursor.executemany(insert_sql, summary_rows)
            inserted += len(summary_rows)
            summary_rows.clear()
            if next_log and inserted >= next_log:
                logger.info(
                    "Summary upgrade progress: source=%s inserted=%d/%d source_path_reads=%d duckdb_reads=%d",
                    source_table,
                    inserted,
                    pending,
                    payload_source_counts["source_path"],
                    payload_source_counts["duckdb"],
                )
                while inserted >= next_log:
                    next_log += progress_interval

    if summary_rows:
        write_cursor.executemany(insert_sql, summary_rows)
        inserted += len(summary_rows)
        if next_log and inserted >= next_log:
            logger.info(
                "Summary upgrade progress: source=%s inserted=%d/%d source_path_reads=%d duckdb_reads=%d",
                source_table,
                inserted,
                pending,
                payload_source_counts["source_path"],
                payload_source_counts["duckdb"],
            )

    _create_index(
        connection,
        table_name=summary_table,
        index_name=f"idx_{summary_table}_gene",
    )
    logger.info(
        "Summary upgrade complete: source=%s summary=%s inserted=%d source_path_reads=%d duckdb_reads=%d",
        source_table,
        summary_table,
        inserted,
        payload_source_counts["source_path"],
        payload_source_counts["duckdb"],
    )
    return inserted


def _selected_source_tables(value: str) -> list[str]:
    selected = {item.strip().lower() for item in value.split(",") if item.strip()}
    if not selected:
        raise ValueError("At least one table family must be selected.")

    source_tables = []
    for label, source_table in (
        ("association", "association_gene_payloads"),
        ("overall", "overall_gene_payloads"),
    ):
        if label in selected:
            source_tables.append(source_table)

    unknown = selected - {"association", "overall"}
    if unknown:
        raise ValueError(f"Unknown table family: {', '.join(sorted(unknown))}")
    return source_tables


def main() -> int:
    args = parse_args()
    logger = _configure_logging(args.log_level)

    if duckdb is None:
        raise RuntimeError("duckdb is required for upgrade_association_serving_duckdb.py")

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Serving DuckDB not found: {db_path}")

    if args.unit_partitions < 1:
        raise ValueError("--unit-partitions must be >= 1")
    if not 0 <= args.unit_partition_index < args.unit_partitions:
        raise ValueError("--unit-partition-index must be in [0, unit_partitions)")
    if args.batch_size < 1:
        raise ValueError("--batch-size must be >= 1")
    if args.max_rows < 0:
        raise ValueError("--max-rows must be >= 0")

    source_tables = _selected_source_tables(args.tables)
    connection = duckdb.connect(str(db_path))
    try:
        if args.duckdb_threads > 0:
            connection.execute(f"PRAGMA threads={int(args.duckdb_threads)}")
        if args.duckdb_memory_limit:
            safe_memory = str(args.duckdb_memory_limit).replace("'", "").strip()
            connection.execute(f"PRAGMA memory_limit='{safe_memory}'")
        if args.duckdb_temp_directory:
            temp_dir = Path(args.duckdb_temp_directory)
            temp_dir.mkdir(parents=True, exist_ok=True)
            connection.execute(f"PRAGMA temp_directory='{str(temp_dir)}'")

        inserted_total = 0
        _create_metadata_table(connection)
        connection.execute("DELETE FROM serving_summary_metadata")
        if args.replace_summary_tables:
            for summary_table in SUMMARY_TABLES.values():
                _drop_summary_table(connection, summary_table)
        for summary_table in SUMMARY_TABLES.values():
            _create_summary_table(connection, summary_table)
        for source_table in source_tables:
            summary_table = SUMMARY_TABLES[source_table]
            inserted_total += _upgrade_table(
                connection,
                source_table=source_table,
                summary_table=summary_table,
                batch_size=args.batch_size,
                max_rows=args.max_rows,
                unit_partitions=args.unit_partitions,
                unit_partition_index=args.unit_partition_index,
                progress_interval=args.progress_interval,
                slow_payload_seconds=args.slow_payload_seconds,
                payload_source=args.payload_source,
                logger=logger,
            )

        if _all_summary_tables_complete(connection):
            _mark_summary_complete(connection)
        else:
            logger.info(
                "Summary metadata not marked complete yet; pending summary rows remain."
            )
        logger.info(
            "Association serving summary upgrade finished: db=%s inserted_total=%d",
            db_path,
            inserted_total,
        )
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
