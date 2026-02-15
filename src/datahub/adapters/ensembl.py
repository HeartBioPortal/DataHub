"""Dedicated adapter for DataManager Ensembl scraper outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from datahub.adapters.base import DataAdapter
from datahub.adapters.common import TabularAdapterMixin, expand_input_paths
from datahub.adapters.phenotypes import PhenotypeMapper
from datahub.models import CanonicalRecord


class EnsemblAssociationAdapter(DataAdapter, TabularAdapterMixin):
    """Ingest Ensembl-derived CSV files into canonical association records."""

    name = "ensembl_association"

    def __init__(
        self,
        *,
        input_paths: str | Path | Iterable[str | Path],
        dataset_id: str = "hbp_ensembl_association",
        source: str = "ensembl",
        phenotype_mapper: PhenotypeMapper | None = None,
        include_dataset_types: set[str] | None = None,
        chunksize: int = 100_000,
    ) -> None:
        self.input_paths = expand_input_paths(input_paths)
        self.dataset_id = dataset_id
        self.source = source
        self.phenotype_mapper = phenotype_mapper or PhenotypeMapper(mapping={})
        self.include_dataset_types = (
            {item.upper() for item in include_dataset_types}
            if include_dataset_types
            else None
        )
        self.chunksize = chunksize

    def read(self) -> Iterable[CanonicalRecord]:
        usecols = {
            "diseases_associated",
            "phenotype",
            "pmid",
            "Gene.symbol",
            "Gene.id",
            "var_class",
            "adj.P.Val",
            "clinical_significance",
            "most_severe_consequence",
            "rsID",
            "variation_id",
            "source",
            "description",
            "location",
            "MAF",
            "allele_string",
            "protein_start",
            "protein_end",
        }

        from datahub.adapters.common import POPULATION_COLUMNS

        all_usecols = usecols | set(POPULATION_COLUMNS)

        for input_path in self.input_paths:
            frame_iter = pd.read_csv(
                input_path,
                usecols=lambda col: col in all_usecols,
                chunksize=self.chunksize,
            )

            for frame in frame_iter:
                for row in frame.to_dict(orient="records"):
                    record = self._to_record(row, input_path)
                    if record is not None:
                        yield record

    def _to_record(self, row: dict[str, Any], source_path: Path) -> CanonicalRecord | None:
        gene_id = self._to_string(row.get("Gene.symbol")) or self._to_string(row.get("Gene.id"))
        variant_id = self._to_string(row.get("rsID")) or self._to_string(row.get("variation_id"))
        phenotype = self._normalize_phenotype(row.get("diseases_associated") or row.get("phenotype"))

        if not gene_id or not variant_id or not phenotype:
            return None

        dataset_type, category = self.phenotype_mapper.resolve(phenotype)
        dataset_type = self._dataset_type_from_path(source_path, dataset_type)

        if not self._should_include_dataset_type(dataset_type, self.include_dataset_types):
            return None

        ancestry = self._parse_population_columns(row)
        if not ancestry:
            maf = self._to_string(row.get("MAF"))
            if maf is not None:
                ancestry = {"Total": maf}

        return CanonicalRecord(
            dataset_id=self.dataset_id,
            dataset_type=dataset_type,
            source=self.source,
            gene_id=gene_id,
            variant_id=variant_id,
            phenotype=phenotype,
            disease_category=category,
            variation_type=self._to_string(row.get("var_class")),
            clinical_significance=self._to_string(row.get("clinical_significance")),
            most_severe_consequence=self._to_string(row.get("most_severe_consequence")),
            p_value=self._to_float(row.get("adj.P.Val")),
            pmid=self._to_string(row.get("pmid")),
            ancestry=ancestry,
            metadata={
                "allele_string": self._to_string(row.get("allele_string")),
                "protein_start": self._to_string(row.get("protein_start")),
                "protein_end": self._to_string(row.get("protein_end")),
                "location": self._to_string(row.get("location")),
                "source_note": self._to_string(row.get("source")),
                "description": self._to_string(row.get("description")),
                "source_file": str(source_path),
            },
        )

    def _dataset_type_from_path(self, source_path: Path, mapped_type: str) -> str:
        upper = str(source_path).upper()
        if "TRAITS" in upper or "TRAIT" in upper:
            return "TRAIT"
        if "CVD" in upper or "DISEASE" in upper:
            return "CVD"

        mapped = mapped_type.upper() if mapped_type else "CVD"
        return mapped if mapped in {"CVD", "TRAIT"} else "CVD"
