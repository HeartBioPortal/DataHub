#!/usr/bin/env python3
"""Build a compact DuckDB serving artifact from published association outputs."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import math
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

from datahub.axis_normalization import normalize_counter_items, normalize_counter_mapping
from datahub.phenotype_paths import PhenotypePathResolver


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
        "--phenotype-tree-json",
        default=None,
        help="Optional phenotype hierarchy JSON used to canonicalize disease/trait paths.",
    )
    parser.add_argument(
        "--expression-json-path",
        default=None,
        help=(
            "Optional explicit expression.json path. If omitted, expression export "
            "is skipped."
        ),
    )
    parser.add_argument(
        "--sga-root",
        default=None,
        help=(
            "Optional explicit SGA root containing cvd/ and trait/ phenotype JSON "
            "files. If omitted, SGA export is skipped."
        ),
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


def _resolve_optional_path(
    *,
    value: str | None,
    label: str,
    logger: logging.Logger,
) -> Path | None:
    if value is None or not str(value).strip():
        logger.warning(
            "%s export disabled: no explicit path was provided.",
            label,
        )
        return None

    path = Path(value)
    if not path.exists():
        raise FileNotFoundError(f"{label} path does not exist: {path}")

    logger.info("%s export enabled: path=%s", label, path)
    return path


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


def _normalize_label_path(
    entry: dict[str, Any],
    *,
    dataset_type: str,
    path_resolver: PhenotypePathResolver | None,
) -> dict[str, Any]:
    if path_resolver is None:
        return entry

    cloned = dict(entry)
    for key in ("disease", "trait"):
        raw_path = cloned.get(key)
        if not isinstance(raw_path, list) or not raw_path:
            continue
        phenotype = raw_path[-1]
        category = raw_path[-2] if len(raw_path) > 1 else None
        cloned[key] = list(
            path_resolver.resolve_leaf_path(
                dataset_type=dataset_type,
                phenotype=phenotype,
                fallback_category=category,
                fallback_path=raw_path,
            )
        )
    return cloned


def _load_payload(
    path: Path,
    *,
    dataset_type: str,
    path_resolver: PhenotypePathResolver | None,
) -> Any:
    if path.name.endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return _normalize_payload(
                json.load(stream),
                dataset_type=dataset_type,
                path_resolver=path_resolver,
            )
    return _normalize_payload(
        json.loads(path.read_text()),
        dataset_type=dataset_type,
        path_resolver=path_resolver,
    )

def _normalize_payload(
    payload: Any,
    *,
    dataset_type: str,
    path_resolver: PhenotypePathResolver | None,
) -> Any:
    if isinstance(payload, list):
        normalized_entries = []
        for entry in payload:
            if not isinstance(entry, dict):
                normalized_entries.append(entry)
                continue
            cloned = _normalize_label_path(
                entry,
                dataset_type=dataset_type,
                path_resolver=path_resolver,
            )
            if isinstance(cloned.get("cs"), list):
                cloned["cs"] = normalize_counter_items(
                    cloned["cs"],
                    axis="clinical_significance",
                    skip_unknown=True,
                )
            normalized_entries.append(cloned)
        return normalized_entries

    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        cloned = dict(payload)
        data = dict(cloned["data"])
        if isinstance(data.get("cs"), dict):
            data["cs"] = normalize_counter_mapping(
                data["cs"],
                axis="clinical_significance",
                skip_unknown=True,
            )
        cloned["data"] = data
        return cloned

    return payload


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
    has_trait_overall BOOLEAN,
    has_expression BOOLEAN,
    has_sga BOOLEAN
)
"""
    )
    connection.execute(
        """
CREATE TABLE expression_gene_payloads (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
    )
    connection.execute(
        """
CREATE TABLE sga_gene_payloads (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
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
    overall_row_count BIGINT,
    expression_row_count BIGINT,
    sga_row_count BIGINT,
    expression_source_path VARCHAR,
    sga_source_root VARCHAR
)
"""
    )


def _insert_rows(
    connection: Any,
    *,
    table_name: str,
    rows: list[tuple[Any, ...]],
    batch_size: int,
) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in range(len(rows[0])))
    sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
    for start in range(0, len(rows), max(batch_size, 1)):
        connection.executemany(sql, rows[start:start + max(batch_size, 1)])


def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _normalize_expression_entry(value: Any) -> Any:
    if not isinstance(value, dict):
        return value

    if "regulation" in value and isinstance(value.get("regulation"), dict):
        regulation = value["regulation"]
        return {
            "up": 0 if _is_nan(regulation.get("upregulated", 0)) else regulation.get("upregulated", 0),
            "down": 0 if _is_nan(regulation.get("downregulated", 0)) else regulation.get("downregulated", 0),
        }

    normalized: dict[str, Any] = {}
    for disease, regulation in value.items():
        if not isinstance(regulation, dict):
            normalized[disease] = regulation
            continue
        up = regulation.get("upregulated", 0)
        down = regulation.get("downregulated", 0)
        normalized[disease] = {
            "up": 0 if _is_nan(up) else up,
            "down": 0 if _is_nan(down) else down,
        }
    return normalized


def _load_expression_rows(
    expression_json_path: Path | None,
    *,
    include_genes: set[str] | None,
) -> list[tuple[str, str, str, str]]:
    if expression_json_path is None or not expression_json_path.exists():
        return []

    payload = json.loads(expression_json_path.read_text())
    rows: list[tuple[str, str, str, str]] = []
    for gene_id, value in payload.items():
        normalized_gene = str(gene_id).upper()
        if "," in str(gene_id):
            continue
        if include_genes is not None and normalized_gene not in include_genes:
            continue
        rows.append(
            (
                str(gene_id),
                normalized_gene,
                json.dumps(_normalize_expression_entry(value), separators=(",", ":")),
                str(expression_json_path),
            )
        )

    return rows


def _load_sga_rows(
    sga_root: Path | None,
    *,
    include_genes: set[str] | None,
    logger: logging.Logger,
) -> list[tuple[str, str, str, str]]:
    if sga_root is None or not sga_root.exists():
        return []

    by_gene: dict[str, list[dict[str, Any]]] = {}
    source_paths: dict[str, set[str]] = {}

    for dtype in ("cvd", "trait"):
        dtype_root = sga_root / dtype
        if not dtype_root.exists():
            continue
        for payload_path in sorted(dtype_root.glob("*.json")):
            phenotype_name = payload_path.stem
            try:
                payload = json.loads(payload_path.read_text())
            except Exception as exc:
                logger.warning("Skipping unreadable SGA payload %s: %s", payload_path, exc)
                continue
            if not isinstance(payload, dict):
                continue
            for gene_id, data in payload.items():
                normalized_gene = str(gene_id).upper()
                if "," in str(gene_id):
                    continue
                if include_genes is not None and normalized_gene not in include_genes:
                    continue
                by_gene.setdefault(normalized_gene, []).append(
                    {
                        "gene": str(gene_id),
                        "data": data,
                        "type": dtype,
                        "name": phenotype_name,
                    }
                )
                source_paths.setdefault(normalized_gene, set()).add(str(payload_path))

    rows: list[tuple[str, str, str, str]] = []
    for normalized_gene in sorted(by_gene.keys()):
        payload_items = sorted(
            by_gene[normalized_gene],
            key=lambda item: (str(item.get("type", "")), str(item.get("name", ""))),
        )
        gene_id = str(payload_items[0].get("gene", normalized_gene))
        rows.append(
            (
                gene_id,
                normalized_gene,
                json.dumps(payload_items, separators=(",", ":")),
                ";".join(sorted(source_paths.get(normalized_gene, set()))),
            )
        )
    return rows


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
    path_resolver = (
        PhenotypePathResolver.from_tree_json(args.phenotype_tree_json)
        if args.phenotype_tree_json
        else None
    )
    db_path = Path(args.db_path)
    expression_json_path = _resolve_optional_path(
        value=args.expression_json_path,
        label="Expression",
        logger=logger,
    )
    sga_root = _resolve_optional_path(
        value=args.sga_root,
        label="SGA",
        logger=logger,
    )

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
    logger.info(
        "Serving builder configuration: phenotype_tree=%s expression=%s sga=%s replace=%s",
        args.phenotype_tree_json or "disabled",
        str(expression_json_path) if expression_json_path else "disabled",
        str(sga_root) if sga_root else "disabled",
        bool(args.replace),
    )

    connection = duckdb.connect(str(db_path))
    try:
        _create_tables(connection)

        association_rows: list[tuple[str, str, str, str, str]] = []
        overall_rows: list[tuple[str, str, str, str, str]] = []
        expression_rows = _load_expression_rows(
            expression_json_path,
            include_genes=include_genes,
        )
        sga_rows = _load_sga_rows(
            sga_root,
            include_genes=include_genes,
            logger=logger,
        )

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
                            json.dumps(
                                _load_payload(
                                    payload_path,
                                    dataset_type=dataset_type,
                                    path_resolver=path_resolver,
                                ),
                                separators=(",", ":"),
                            ),
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
                            json.dumps(
                                _load_payload(
                                    payload_path,
                                    dataset_type=dataset_type,
                                    path_resolver=path_resolver,
                                ),
                                separators=(",", ":"),
                            ),
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
        _insert_rows(
            connection,
            table_name="expression_gene_payloads",
            rows=expression_rows,
            batch_size=args.batch_size,
        )
        _insert_rows(
            connection,
            table_name="sga_gene_payloads",
            rows=sga_rows,
            batch_size=args.batch_size,
        )

        connection.execute(
            """
INSERT INTO gene_catalog
SELECT
    coalesce(a.gene_id, o.gene_id, e.gene_id, s.gene_id) AS gene_id,
    coalesce(
        a.gene_id_normalized,
        o.gene_id_normalized,
        e.gene_id_normalized,
        s.gene_id_normalized
    ) AS gene_id_normalized,
    coalesce(a.has_cvd_association, false) OR coalesce(o.has_cvd_overall, false) AS has_cvd,
    coalesce(a.has_trait_association, false) OR coalesce(o.has_trait_overall, false) AS has_trait,
    coalesce(a.has_cvd_association, false) AS has_cvd_association,
    coalesce(a.has_trait_association, false) AS has_trait_association,
    coalesce(o.has_cvd_overall, false) AS has_cvd_overall,
    coalesce(o.has_trait_overall, false) AS has_trait_overall,
    coalesce(e.has_expression, false) AS has_expression,
    coalesce(s.has_sga, false) AS has_sga
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
FULL OUTER JOIN (
    SELECT
        gene_id,
        gene_id_normalized,
        true AS has_expression
    FROM expression_gene_payloads
) e
  ON coalesce(a.gene_id_normalized, o.gene_id_normalized) = e.gene_id_normalized
FULL OUTER JOIN (
    SELECT
        gene_id,
        gene_id_normalized,
        true AS has_sga
    FROM sga_gene_payloads
) s
  ON coalesce(a.gene_id_normalized, o.gene_id_normalized, e.gene_id_normalized) = s.gene_id_normalized
"""
        )

        connection.execute(
            "INSERT INTO build_metadata VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                str(final_root),
                args.association_subdir,
                args.overall_subdir,
                ",".join(dataset_types),
                0 if include_genes is None else len(include_genes),
                len(association_rows),
                len(overall_rows),
                len(expression_rows),
                len(sga_rows),
                str(expression_json_path) if expression_json_path else "",
                str(sga_root) if sga_root else "",
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
        connection.execute(
            "CREATE INDEX idx_expression_gene ON expression_gene_payloads (gene_id_normalized)"
        )
        connection.execute(
            "CREATE INDEX idx_sga_gene ON sga_gene_payloads (gene_id_normalized)"
        )

        summary = {
            "db_path": str(db_path),
            "source_root": str(final_root),
            "dataset_types": dataset_types,
            "filtered_gene_count": 0 if include_genes is None else len(include_genes),
            "association_rows": len(association_rows),
            "overall_rows": len(overall_rows),
            "expression_rows": len(expression_rows),
            "sga_rows": len(sga_rows),
            "catalog_rows": int(connection.execute("SELECT COUNT(*) FROM gene_catalog").fetchone()[0]),
        }
        logger.info("Association serving build complete: %s", summary)
        print(json.dumps(summary, indent=2))
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
