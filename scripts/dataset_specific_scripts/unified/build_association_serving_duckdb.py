#!/usr/bin/env python3
"""Build a compact DuckDB serving artifact from published association outputs."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover - runtime guard
    duckdb = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a compact read-only DuckDB serving database from published "
            "association JSON/JSON.GZ outputs."
        )
    )
    parser.add_argument(
        "--input-root",
        required=True,
        help=(
            "Published output root. Accepts either the DataHub output root "
            "(containing association/final) or the final directory itself."
        ),
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Destination DuckDB path for compact serving tables.",
    )
    parser.add_argument(
        "--association-subdir",
        default="association",
        help="Subdirectory under association/final containing per-gene association payloads.",
    )
    parser.add_argument(
        "--overall-subdir",
        default="overall",
        help="Subdirectory under association/final containing per-gene overall payloads.",
    )
    parser.add_argument(
        "--dataset-types",
        default="CVD,TRAIT",
        help="Comma-separated dataset types to include.",
    )
    parser.add_argument(
        "--include-genes",
        default="",
        help="Optional comma-separated gene list filter.",
    )
    parser.add_argument(
        "--include-genes-file",
        default=None,
        help="Optional newline-delimited file containing genes to include.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete an existing destination DB before rebuilding.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Insert batch size.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )
    return parser.parse_args()


def _setup_logger(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("datahub.serving.builder")


def _parse_dataset_types(value: str) -> list[str]:
    parsed = []
    for item in str(value).split(","):
        normalized = item.strip().upper()
        if normalized:
            parsed.append(normalized)
    return parsed


def _parse_gene_filter(args: argparse.Namespace) -> set[str] | None:
    genes: set[str] = set()
    if args.include_genes:
        genes.update(
            item.strip().upper()
            for item in str(args.include_genes).split(",")
            if item.strip()
        )
    if args.include_genes_file:
        path = Path(args.include_genes_file)
        genes.update(
            line.strip().upper()
            for line in path.read_text().splitlines()
            if line.strip()
        )
    return genes or None


def _resolve_final_root(input_root: str | Path) -> Path:
    root = Path(input_root)
    if (root / "association").is_dir() and (root / "overall").is_dir():
        return root

    candidate = root / "association" / "final"
    if (candidate / "association").is_dir() and (candidate / "overall").is_dir():
        return candidate

    raise FileNotFoundError(
        "Could not resolve published final output root from input-root: "
        f"{root}"
    )


def _strip_payload_suffix(path: Path) -> str:
    if path.name.endswith(".json.gz"):
        return path.name[:-8]
    if path.name.endswith(".json"):
        return path.stem
    raise ValueError(f"Unsupported payload file: {path}")


def _collect_gene_payload_files(
    root: Path,
    *,
    include_genes: set[str] | None,
) -> list[tuple[str, Path]]:
    preferred: dict[str, tuple[int, str, Path]] = {}
    for path in root.iterdir():
        if not path.is_file():
            continue

        priority = 0
        if path.name.endswith(".json.gz"):
            priority = 2
        elif path.name.endswith(".json"):
            priority = 1
        else:
            continue

        gene_id = _strip_payload_suffix(path)
        normalized = gene_id.upper()
        if include_genes is not None and normalized not in include_genes:
            continue

        existing = preferred.get(normalized)
        if existing is None or priority > existing[0]:
            preferred[normalized] = (priority, gene_id, path)

    return sorted(
        [(gene_id, payload_path) for _normalized, (_priority, gene_id, payload_path) in preferred.items()],
        key=lambda item: item[0].upper(),
    )


def _load_payload(path: Path) -> Any:
    if path.name.endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return json.load(stream)
    return json.loads(path.read_text())


def _create_tables(connection: Any) -> None:
    connection.execute(
        """
CREATE TABLE association_gene_payloads (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
    )
    connection.execute(
        """
CREATE TABLE overall_gene_payloads (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
    )
    connection.execute(
        """
CREATE TABLE gene_catalog (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    has_cvd BOOLEAN,
    has_trait BOOLEAN,
    has_cvd_association BOOLEAN,
    has_trait_association BOOLEAN,
    has_cvd_overall BOOLEAN,
    has_trait_overall BOOLEAN
)
"""
    )
    connection.execute(
        """
CREATE TABLE build_metadata (
    built_at TIMESTAMP,
    source_root VARCHAR,
    association_subdir VARCHAR,
    overall_subdir VARCHAR,
    dataset_types VARCHAR,
    filtered_gene_count BIGINT,
    association_row_count BIGINT,
    overall_row_count BIGINT
)
"""
    )


def _insert_rows(
    connection: Any,
    *,
    table_name: str,
    rows: list[tuple[str, str, str, str, str]],
    batch_size: int,
) -> None:
    sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)"
    for start in range(0, len(rows), max(batch_size, 1)):
        connection.executemany(sql, rows[start:start + max(batch_size, 1)])


def main() -> int:
    args = parse_args()
    logger = _setup_logger(args.log_level)

    if duckdb is None:
        raise RuntimeError(
            "duckdb is required for build_association_serving_duckdb.py. "
            "Install DataHub requirements first."
        )

    final_root = _resolve_final_root(args.input_root)
    dataset_types = _parse_dataset_types(args.dataset_types)
    include_genes = _parse_gene_filter(args)
    db_path = Path(args.db_path)

    if args.replace and db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Association serving build start: final_root=%s db=%s dataset_types=%s include_genes=%s",
        final_root,
        db_path,
        dataset_types,
        len(include_genes) if include_genes is not None else "ALL",
    )

    connection = duckdb.connect(str(db_path))
    try:
        _create_tables(connection)

        association_rows: list[tuple[str, str, str, str, str]] = []
        overall_rows: list[tuple[str, str, str, str, str]] = []

        for dataset_type in dataset_types:
            association_root = final_root / args.association_subdir / dataset_type
            overall_root = final_root / args.overall_subdir / dataset_type

            if association_root.exists():
                files = _collect_gene_payload_files(
                    association_root,
                    include_genes=include_genes,
                )
                logger.info(
                    "Association payload scan: dataset_type=%s files=%d root=%s",
                    dataset_type,
                    len(files),
                    association_root,
                )
                for gene_id, payload_path in files:
                    association_rows.append(
                        (
                            dataset_type,
                            gene_id,
                            gene_id.upper(),
                            json.dumps(_load_payload(payload_path), separators=(",", ":")),
                            str(payload_path),
                        )
                    )

            if overall_root.exists():
                files = _collect_gene_payload_files(
                    overall_root,
                    include_genes=include_genes,
                )
                logger.info(
                    "Overall payload scan: dataset_type=%s files=%d root=%s",
                    dataset_type,
                    len(files),
                    overall_root,
                )
                for gene_id, payload_path in files:
                    overall_rows.append(
                        (
                            dataset_type,
                            gene_id,
                            gene_id.upper(),
                            json.dumps(_load_payload(payload_path), separators=(",", ":")),
                            str(payload_path),
                        )
                    )

        _insert_rows(
            connection,
            table_name="association_gene_payloads",
            rows=association_rows,
            batch_size=args.batch_size,
        )
        _insert_rows(
            connection,
            table_name="overall_gene_payloads",
            rows=overall_rows,
            batch_size=args.batch_size,
        )

        connection.execute(
            """
INSERT INTO gene_catalog
SELECT
    coalesce(a.gene_id, o.gene_id) AS gene_id,
    coalesce(a.gene_id_normalized, o.gene_id_normalized) AS gene_id_normalized,
    coalesce(a.has_cvd_association, false) OR coalesce(o.has_cvd_overall, false) AS has_cvd,
    coalesce(a.has_trait_association, false) OR coalesce(o.has_trait_overall, false) AS has_trait,
    coalesce(a.has_cvd_association, false) AS has_cvd_association,
    coalesce(a.has_trait_association, false) AS has_trait_association,
    coalesce(o.has_cvd_overall, false) AS has_cvd_overall,
    coalesce(o.has_trait_overall, false) AS has_trait_overall
FROM (
    SELECT
        gene_id,
        gene_id_normalized,
        bool_or(dataset_type = 'CVD') AS has_cvd_association,
        bool_or(dataset_type = 'TRAIT') AS has_trait_association
    FROM association_gene_payloads
    GROUP BY gene_id, gene_id_normalized
) a
FULL OUTER JOIN (
    SELECT
        gene_id,
        gene_id_normalized,
        bool_or(dataset_type = 'CVD') AS has_cvd_overall,
        bool_or(dataset_type = 'TRAIT') AS has_trait_overall
    FROM overall_gene_payloads
    GROUP BY gene_id, gene_id_normalized
) o
  ON a.gene_id_normalized = o.gene_id_normalized
"""
        )

        connection.execute(
            "INSERT INTO build_metadata VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)",
            [
                str(final_root),
                args.association_subdir,
                args.overall_subdir,
                ",".join(dataset_types),
                0 if include_genes is None else len(include_genes),
                len(association_rows),
                len(overall_rows),
            ],
        )

        connection.execute(
            "CREATE INDEX idx_association_gene ON association_gene_payloads (dataset_type, gene_id_normalized)"
        )
        connection.execute(
            "CREATE INDEX idx_overall_gene ON overall_gene_payloads (dataset_type, gene_id_normalized)"
        )
        connection.execute(
            "CREATE INDEX idx_gene_catalog ON gene_catalog (gene_id_normalized)"
        )

        summary = {
            "db_path": str(db_path),
            "source_root": str(final_root),
            "dataset_types": dataset_types,
            "filtered_gene_count": 0 if include_genes is None else len(include_genes),
            "association_rows": len(association_rows),
            "overall_rows": len(overall_rows),
            "catalog_rows": int(connection.execute("SELECT COUNT(*) FROM gene_catalog").fetchone()[0]),
        }
        logger.info("Association serving build complete: %s", summary)
        print(json.dumps(summary, indent=2))
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
