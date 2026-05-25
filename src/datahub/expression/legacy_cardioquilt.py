"""Import legacy CardioQuilt GEO/CREEDS differential-expression outputs."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from .config import ExpressionBuildConfig
from .records import DifferentialExpressionRow


def read_cardioquilt_csv(
    path: str | Path,
    *,
    config: ExpressionBuildConfig | None = None,
) -> list[DifferentialExpressionRow]:
    """Read CardioQuilt `cardioquilt_CREEDS_GEO.csv` rows.

    The legacy file contains significant rows only. It can populate up/down
    counts, but cannot prove not-significant counts because tested genes absent
    from the file are not represented.
    """

    cfg = config or ExpressionBuildConfig()
    rows: list[DifferentialExpressionRow] = []
    with Path(path).open(newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            adjusted = raw.get("adj.P.Val")
            try:
                if adjusted not in (None, "") and float(adjusted) >= cfg.adjusted_p_value_threshold:
                    continue
            except ValueError:
                continue
            rows.append(
                DifferentialExpressionRow.from_mapping(
                    raw,
                    source_database=cfg.source_database,
                    adjusted_p_value_threshold=cfg.adjusted_p_value_threshold,
                    fdr_method=cfg.fdr_method,
                    analysis_method=cfg.analysis_method,
                    analysis_package_version=cfg.analysis_package_version,
                    preprocessing_method=(
                        "GEOquery/GEOmetadb discovery plus limma disease-vs-control "
                        "contrast in legacy CardioQuilt where recoverable"
                    ),
                    notes=(
                        "Imported from legacy CardioQuilt significant-row CSV. "
                        "Sample metadata and tested-but-not-significant genes are not present."
                    ),
                )
            )
    return rows


def iter_cardioquilt_csv(
    path: str | Path,
    *,
    config: ExpressionBuildConfig | None = None,
) -> Iterable[DifferentialExpressionRow]:
    yield from read_cardioquilt_csv(path, config=config)

