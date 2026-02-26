"""Shared utilities for tabular source adapters."""

from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from datahub.adapters.phenotypes import PhenotypeMapper


POPULATION_COLUMNS: tuple[str, ...] = (
    "Total",
    "European",
    "African",
    "African Others",
    "African American",
    "Asian",
    "East Asian",
    "Other Asian",
    "Latin American 1",
    "Latin American 2",
    "South Asian",
    "Other",
)


def _is_csv_like(path: Path) -> bool:
    """Return True if the file looks like a CSV or compressed CSV."""

    name = path.name.lower()
    return name.endswith(".csv") or name.endswith(".csv.gz")


def expand_input_paths(input_paths: str | Path | Iterable[str | Path]) -> list[Path]:
    """Expand file, directory, or glob inputs into concrete CSV paths."""

    if isinstance(input_paths, (str, Path)):
        items: list[str | Path] = [input_paths]
    else:
        items = list(input_paths)

    resolved: list[Path] = []
    for item in items:
        expanded_item = os.path.expandvars(os.path.expanduser(str(item)))
        item_path = Path(expanded_item)

        if item_path.is_dir():
            resolved.extend(
                sorted(
                    path
                    for path in item_path.iterdir()
                    if path.is_file() and _is_csv_like(path)
                )
            )
            continue

        if item_path.exists():
            resolved.append(item_path)
            continue

        matches = [Path(path) for path in glob.glob(expanded_item)]
        resolved.extend(sorted(match for match in matches if _is_csv_like(match)))

    return resolved


class TabularAdapterMixin:
    """Common conversions for CSV-based source adapters."""

    @staticmethod
    def _to_string(value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None

        cleaned = str(value).strip()
        if not cleaned or cleaned.lower() in {"nan", "none", "null", "na"}:
            return None

        return cleaned

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or pd.isna(value):
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_phenotype(value: Any) -> str | None:
        as_string = TabularAdapterMixin._to_string(value)
        return PhenotypeMapper.normalize(as_string) if as_string else None

    def _should_include_dataset_type(
        self,
        dataset_type: str,
        include_dataset_types: set[str] | None,
    ) -> bool:
        return include_dataset_types is None or dataset_type.upper() in include_dataset_types

    def _parse_population_columns(self, row: Mapping[str, Any]) -> dict[str, Any]:
        ancestry: dict[str, Any] = {}
        for population in POPULATION_COLUMNS:
            value = self._to_string(row.get(population))
            if value is None:
                continue
            ancestry[population] = value
        return ancestry
