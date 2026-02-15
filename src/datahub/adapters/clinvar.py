"""Dedicated adapter for DataManager ClinVar outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from datahub.adapters.base import DataAdapter
from datahub.adapters.common import TabularAdapterMixin, expand_input_paths
from datahub.adapters.phenotypes import PhenotypeMapper
from datahub.models import CanonicalRecord


class ClinVarAssociationAdapter(DataAdapter, TabularAdapterMixin):
    """Ingest ClinVar-derived tables into canonical records.

    Supports both ClinVar variant-viewer CSVs and structural variant CSVs.
    """

    name = "clinvar_association"

    def __init__(
        self,
        *,
        input_paths: str | Path | Iterable[str | Path],
        dataset_id: str = "hbp_clinvar_association",
        source: str = "clinvar",
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
            "rsID",
            "Variant ID",
            "primary_gene",
            "Gene Name",
            "phenotype",
            "Subject Phenotype",
            "pmid",
            "Clinical Interpretation",
            "Variant Call type",
            "mutation",
            "amino_acid",
            "protein_id",
            "protein_length",
            "value",
            "GMAF",
            "af_value",
            "af_source",
        }

        for input_path in self.input_paths:
            frame_iter = pd.read_csv(
                input_path,
                usecols=lambda col: col in usecols,
                chunksize=self.chunksize,
            )

            for frame in frame_iter:
                for row in frame.to_dict(orient="records"):
                    record = self._to_record(row, input_path)
                    if record is not None:
                        yield record

    def _to_record(self, row: dict[str, Any], source_path: Path) -> CanonicalRecord | None:
        gene_id = self._to_string(row.get("primary_gene")) or self._to_string(row.get("Gene Name"))
        variant_id = self._to_string(row.get("rsID")) or self._to_string(row.get("Variant ID"))
        phenotype = self._normalize_phenotype(row.get("phenotype") or row.get("Subject Phenotype"))

        if not gene_id or not variant_id or not phenotype:
            return None

        dataset_type, category = self.phenotype_mapper.resolve(phenotype)
        dataset_type = self._dataset_type_from_path(source_path, dataset_type)

        if not self._should_include_dataset_type(dataset_type, self.include_dataset_types):
            return None

        return CanonicalRecord(
            dataset_id=self.dataset_id,
            dataset_type=dataset_type,
            source=self.source,
            gene_id=gene_id,
            variant_id=variant_id,
            phenotype=phenotype,
            disease_category=category,
            variation_type=self._to_string(row.get("Variant Call type")),
            clinical_significance=self._to_string(row.get("Clinical Interpretation")),
            most_severe_consequence=None,
            p_value=None,
            pmid=self._to_string(row.get("pmid")),
            ancestry={},
            metadata={
                "mutation": self._to_string(row.get("mutation")),
                "amino_acid": self._to_string(row.get("amino_acid")),
                "protein_id": self._to_string(row.get("protein_id")),
                "protein_length": self._to_string(row.get("protein_length")),
                "value": self._to_string(row.get("value")),
                "gmaf": self._to_string(row.get("GMAF")),
                "af_value": self._to_string(row.get("af_value")),
                "af_source": self._to_string(row.get("af_source")),
                "source_file": str(source_path),
            },
        )

    def _dataset_type_from_path(self, source_path: Path, mapped_type: str) -> str:
        upper = str(source_path).upper()
        if "TRAIT" in upper:
            return "TRAIT"
        if "DISEASE" in upper or "CVD" in upper:
            return "CVD"

        mapped = mapped_type.upper() if mapped_type else "CVD"
        return mapped if mapped in {"CVD", "TRAIT"} else "CVD"
