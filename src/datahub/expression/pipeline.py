"""Build expression v2 artifacts and DuckDB datamarts."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import ExpressionBuildConfig
from .records import (
    DifferentialExpressionRow,
    GenePhenotypeSummary,
    summarize_gene_phenotypes,
)


def _json_default(value: Any) -> Any:
    if isinstance(value, set):
        return sorted(value)
    return str(value)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _legacy_summary(summaries: list[GenePhenotypeSummary]) -> dict[str, dict[str, dict[str, int]]]:
    payload: dict[str, dict[str, dict[str, int]]] = defaultdict(dict)
    for row in summaries:
        payload[row.gene_id][row.phenotype_id] = {
            "up": row.number_of_studies_upregulated,
            "down": row.number_of_studies_downregulated,
            "not_significant": row.number_of_studies_not_significant,
            "total": row.number_of_total_studies,
        }
    return {gene: dict(value) for gene, value in sorted(payload.items())}


def _legacy_compatible_summary(
    summaries: list[GenePhenotypeSummary],
) -> dict[str, dict[str, dict[str, int]]]:
    payload: dict[str, dict[str, dict[str, int]]] = defaultdict(dict)
    for row in summaries:
        payload[row.gene_id][row.phenotype_id] = {
            "upregulated": row.number_of_studies_upregulated,
            "downregulated": row.number_of_studies_downregulated,
        }
    return {gene: dict(value) for gene, value in sorted(payload.items())}


def _write_duckdb(
    *,
    db_path: Path,
    rows: list[DifferentialExpressionRow],
    summaries: list[GenePhenotypeSummary],
    manifest: dict[str, Any],
) -> None:
    try:
        import duckdb  # type: ignore
        import pandas as pd  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("duckdb and pandas are required for expression datamart export") from exc

    db_path.parent.mkdir(parents=True, exist_ok=True)
    row_frame = pd.DataFrame([row.to_dict() for row in rows])
    summary_frame = pd.DataFrame([row.to_dict() for row in summaries])
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("DROP TABLE IF EXISTS expression_differential_results")
        connection.execute("DROP TABLE IF EXISTS expression_gene_phenotype_summary")
        connection.execute("DROP TABLE IF EXISTS expression_pipeline_metadata")
        connection.execute(
            """
CREATE TABLE expression_differential_results AS
SELECT * FROM row_frame
"""
        )
        connection.execute(
            """
CREATE TABLE expression_gene_phenotype_summary AS
SELECT * FROM summary_frame
"""
        )
        connection.execute(
            """
CREATE TABLE expression_pipeline_metadata (
    key VARCHAR,
    value VARCHAR
)
"""
        )
        connection.executemany(
            "INSERT INTO expression_pipeline_metadata VALUES (?, ?)",
            [(key, json.dumps(value, default=_json_default)) for key, value in manifest.items()],
        )
    finally:
        connection.close()


def build_expression_outputs(
    *,
    rows: list[DifferentialExpressionRow],
    output_root: str | Path,
    config: ExpressionBuildConfig | None = None,
    duckdb_path: str | Path | None = None,
) -> dict[str, Any]:
    """Write row-level, summary, legacy-compatible, and optional DuckDB outputs."""

    cfg = config or ExpressionBuildConfig()
    root = Path(output_root)
    final_root = root / "final" / cfg.artifact_subdir
    final_root.mkdir(parents=True, exist_ok=True)

    summaries = summarize_gene_phenotypes(rows)
    rows_csv = final_root / "differential_expression_rows.csv"
    summary_csv = final_root / "gene_phenotype_summary.csv"
    summary_json = final_root / "gene_phenotype_summary.json"
    legacy_json = final_root / "expression_legacy_compatible.json"
    serving_json = final_root / "expression_serving_summary.json"
    manifest_path = final_root / "manifest.json"

    row_dicts = [row.to_dict() for row in rows]
    summary_dicts = [row.to_dict() for row in summaries]
    _write_csv(rows_csv, row_dicts)
    _write_csv(summary_csv, summary_dicts)
    summary_json.write_text(json.dumps(summary_dicts, indent=2, default=_json_default) + "\n")
    legacy_json.write_text(json.dumps(_legacy_compatible_summary(summaries), indent=2) + "\n")
    serving_json.write_text(json.dumps(_legacy_summary(summaries), indent=2) + "\n")

    studies = sorted({row.study_accession for row in rows if row.study_accession})
    genes = sorted({row.gene_id for row in rows if row.gene_id})
    phenotypes = sorted({row.phenotype_label_normalized for row in rows if row.phenotype_label_normalized})
    if cfg.artifact_subdir == "expression_v3":
        limitations = [
            "Only curation-approved source studies should be included in expression v3.",
            "Sample-level metadata, tissue, platform, and exact contrast definitions depend on the curation manifest and source rows.",
            "Expression Atlas, ArrayExpress, GTEx baseline, and single-cell layers require separate importers or source-role labels before aggregation.",
        ]
    else:
        limitations = [
            "Legacy CardioQuilt CSV contains significant rows only; not-significant counts cannot be reconstructed from it.",
            "Sample-level metadata, tissue, platform, and exact contrast definitions are incomplete unless supplied in source rows.",
        ]
    manifest = {
        "analysis_id": cfg.artifact_subdir,
        "pipeline_name": cfg.pipeline_name,
        "pipeline_version": cfg.pipeline_version,
        "built_at": datetime.now(UTC).isoformat(),
        "row_count": len(rows),
        "summary_row_count": len(summaries),
        "gene_count": len(genes),
        "phenotype_count": len(phenotypes),
        "study_count": len(studies),
        "thresholds": {
            "adjusted_p_value_lt": cfg.adjusted_p_value_threshold,
            "minimum_abs_log2_fold_change": cfg.minimum_abs_log2_fold_change,
            "fdr_method": cfg.fdr_method,
        },
        "outputs": {
            "differential_expression_rows": str(rows_csv),
            "gene_phenotype_summary": str(summary_csv),
            "gene_phenotype_summary_json": str(summary_json),
            "legacy_compatible_expression_json": str(legacy_json),
            "serving_summary_json": str(serving_json),
        },
        "limitations": limitations,
    }
    if duckdb_path is not None:
        manifest["outputs"]["duckdb"] = str(duckdb_path)
        _write_duckdb(db_path=Path(duckdb_path), rows=rows, summaries=summaries, manifest=manifest)
    manifest_path.write_text(json.dumps(manifest, indent=2, default=_json_default) + "\n")
    return manifest
