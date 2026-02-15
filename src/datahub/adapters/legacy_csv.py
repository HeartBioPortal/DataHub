"""Adapter that ingests legacy DataManager aggregated association CSV files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from datahub.adapters.base import DataAdapter
from datahub.adapters.phenotypes import PhenotypeMapper
from datahub.models import CanonicalRecord


class LegacyAssociationCsvAdapter(DataAdapter):
    """Read legacy ``final_aggregated_results.csv`` into canonical records."""

    name = "legacy_association_csv"

    def __init__(
        self,
        *,
        csv_path: str | Path,
        dataset_id: str = "hbp_legacy_association",
        source: str = "legacy_aggregated_csv",
        phenotype_mapper: PhenotypeMapper | None = None,
        include_dataset_types: set[str] | None = None,
        chunksize: int = 100_000,
    ) -> None:
        self.csv_path = Path(csv_path)
        self.dataset_id = dataset_id
        self.source = source
        if phenotype_mapper is not None:
            self.phenotype_mapper = phenotype_mapper
        else:
            try:
                self.phenotype_mapper = PhenotypeMapper.from_hbp_backend()
            except Exception:
                self.phenotype_mapper = PhenotypeMapper(mapping={})
        self.include_dataset_types = {item.upper() for item in include_dataset_types} if include_dataset_types else None
        self.chunksize = chunksize

    def read(self) -> Iterable[CanonicalRecord]:
        columns = [
            "rsid",
            "gene",
            "phenotype",
            "var_class",
            "clinical_significance",
            "most_severe_consequence",
            "pval",
            "pmid",
            "ancestry_data",
            "functional_class",
        ]

        frame_iter = pd.read_csv(self.csv_path, usecols=lambda c: c in columns, chunksize=self.chunksize)
        for frame in frame_iter:
            for row in frame.itertuples(index=False):
                gene_id = self._to_string(getattr(row, "gene", None))
                variant_id = self._to_string(getattr(row, "rsid", None))
                phenotype = PhenotypeMapper.normalize(self._to_string(getattr(row, "phenotype", None)))

                if not gene_id or not variant_id or not phenotype:
                    continue

                dataset_type, category = self.phenotype_mapper.resolve(phenotype)
                dataset_type = dataset_type.upper()

                if self.include_dataset_types and dataset_type not in self.include_dataset_types:
                    continue

                yield CanonicalRecord(
                    dataset_id=self.dataset_id,
                    dataset_type=dataset_type,
                    source=self.source,
                    gene_id=gene_id,
                    variant_id=variant_id,
                    phenotype=phenotype,
                    disease_category=category,
                    variation_type=self._to_string(getattr(row, "var_class", None)),
                    clinical_significance=self._to_string(
                        getattr(row, "clinical_significance", None)
                    ),
                    most_severe_consequence=self._to_string(
                        getattr(row, "most_severe_consequence", None)
                    ),
                    p_value=self._to_float(getattr(row, "pval", None)),
                    pmid=self._to_string(getattr(row, "pmid", None)),
                    ancestry=self._parse_ancestry(getattr(row, "ancestry_data", None)),
                    metadata={
                        "functional_class": self._to_string(
                            getattr(row, "functional_class", None)
                        )
                    },
                )

    @staticmethod
    def _to_string(value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None

        cleaned = str(value).strip()
        if not cleaned or cleaned.lower() in {"nan", "none", "null"}:
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

    def _parse_ancestry(self, value: Any) -> dict[str, Any]:
        if value is None or pd.isna(value):
            return {}

        if isinstance(value, dict):
            parsed = value
        else:
            text_value = str(value).strip()
            if not text_value or text_value.lower() in {"nan", "none", "null"}:
                return {}
            try:
                parsed = json.loads(text_value)
            except json.JSONDecodeError:
                return {}

        ancestry: dict[str, Any] = {}
        for population, payload in parsed.items():
            maf = payload
            if isinstance(payload, dict):
                maf = payload.get("MAF", payload.get("maf", payload.get("value")))

            if maf is None:
                continue

            maf_text = str(maf).strip()
            if not maf_text or maf_text.lower() in {"nan", "none", "null"}:
                continue

            ancestry[str(population)] = maf

        return ancestry
