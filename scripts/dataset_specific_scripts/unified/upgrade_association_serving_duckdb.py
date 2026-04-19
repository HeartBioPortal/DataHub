#!/usr/bin/env python3
"""Add API-summary tables to an existing association serving DuckDB in place."""

from __future__ import annotations

import argparse
import json
import logging
import sys
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
        help="Rows to fetch/insert per batch. Keep modest because payload_json can be large.",
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
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
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


def _count_pending(connection: Any, *, source_table: str, summary_table: str) -> int:
    source_table = _safe_identifier(source_table)
    summary_table = _safe_identifier(summary_table)
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
"""
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


def _upgrade_table(
    connection: Any,
    *,
    source_table: str,
    summary_table: str,
    batch_size: int,
    progress_interval: int,
    logger: logging.Logger,
) -> int:
    source_table = _safe_identifier(source_table)
    summary_table = _safe_identifier(summary_table)
    pending = _count_pending(
        connection,
        source_table=source_table,
        summary_table=summary_table,
    )
    logger.info(
        "Summary upgrade start: source=%s summary=%s pending_rows=%d",
        source_table,
        summary_table,
        pending,
    )
    if pending == 0:
        return 0

    read_cursor = connection.cursor()
    write_cursor = connection.cursor()
    read_cursor.execute(
        f"""
SELECT
    p.dataset_type,
    p.gene_id,
    p.gene_id_normalized,
    p.payload_json,
    p.source_path
FROM {source_table} p
WHERE NOT EXISTS (
    SELECT 1
    FROM {summary_table} s
    WHERE s.dataset_type = p.dataset_type
      AND s.gene_id_normalized = p.gene_id_normalized
)
"""
    )

    inserted = 0
    insert_sql = f"INSERT INTO {summary_table} VALUES (?, ?, ?, ?, ?)"
    while True:
        rows = read_cursor.fetchmany(max(batch_size, 1))
        if not rows:
            break

        summary_rows = []
        for dataset_type, gene_id, gene_id_normalized, payload_json, source_path in rows:
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

        write_cursor.executemany(insert_sql, summary_rows)
        inserted += len(summary_rows)
        if progress_interval > 0 and inserted % progress_interval == 0:
            logger.info(
                "Summary upgrade progress: source=%s inserted=%d/%d",
                source_table,
                inserted,
                pending,
            )

    _create_index(
        connection,
        table_name=summary_table,
        index_name=f"idx_{summary_table}_gene",
    )
    logger.info(
        "Summary upgrade complete: source=%s summary=%s inserted=%d",
        source_table,
        summary_table,
        inserted,
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

    source_tables = _selected_source_tables(args.tables)
    connection = duckdb.connect(str(db_path))
    try:
        inserted_total = 0
        _create_metadata_table(connection)
        connection.execute("DELETE FROM serving_summary_metadata")
        for source_table in source_tables:
            summary_table = SUMMARY_TABLES[source_table]
            if args.replace_summary_tables:
                _drop_summary_table(connection, summary_table)
            _create_summary_table(connection, summary_table)
            inserted_total += _upgrade_table(
                connection,
                source_table=source_table,
                summary_table=summary_table,
                batch_size=args.batch_size,
                progress_interval=args.progress_interval,
                logger=logger,
            )

        _mark_summary_complete(connection)
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
