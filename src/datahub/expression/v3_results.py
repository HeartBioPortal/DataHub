"""Import expression v3 differential-expression result tables."""

from __future__ import annotations

import csv
from pathlib import Path

from .config import ExpressionBuildConfig
from .records import DifferentialExpressionRow


def read_expression_v3_results(
    path: str | Path,
    *,
    config: ExpressionBuildConfig | None = None,
) -> list[DifferentialExpressionRow]:
    cfg = config or ExpressionBuildConfig(
        analysis_method="expression_v3_public_geo",
        pipeline_version="3",
    )
    rows: list[DifferentialExpressionRow] = []
    with Path(path).open(newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            rows.append(
                DifferentialExpressionRow.from_mapping(
                    raw,
                    source_database=raw.get("source_database") or cfg.source_database,
                    adjusted_p_value_threshold=cfg.adjusted_p_value_threshold,
                    fdr_method=raw.get("fdr_method") or cfg.fdr_method,
                    analysis_method=raw.get("analysis_method") or cfg.analysis_method,
                    analysis_package_version=raw.get("analysis_package_version")
                    or cfg.analysis_package_version,
                    preprocessing_method=raw.get("preprocessing_method"),
                    notes=raw.get("notes"),
                )
            )
    return rows

