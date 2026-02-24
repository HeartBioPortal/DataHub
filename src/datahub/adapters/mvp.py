"""Adapter for Million Veteran Program association datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from datahub.adapters.base import DataAdapter
from datahub.adapters.common import TabularAdapterMixin, expand_input_paths
from datahub.adapters.phenotypes import PhenotypeMapper
from datahub.models import CanonicalRecord


@dataclass
class _MVPAccumulator:
    """Mutable accumulator for one MVP (gene, variant, phenotype) record."""

    record: CanonicalRecord
    best_pvalue: float | None


class MVPAssociationAdapter(DataAdapter, TabularAdapterMixin):
    """Ingest MVP long-form ancestry rows into canonical association records.

    Input rows are expected to contain per-ancestry frequencies, for example:
    ``SNP_ID, af, ancestry, gene_symbol, phenotype_description, ...``.
    This adapter merges ancestry slices into one canonical record per
    ``(gene_symbol, SNP_ID, phenotype_description)``.
    """

    name = "mvp_association"

    DEFAULT_ANCESTRY_CODE_MAP: dict[str, str] = {
        "ALL": "Total",
        "AFR": "African",
        "AMR": "Admixed American",
        "ASJ": "Ashkenazi Jewish",
        "EAS": "East Asian",
        "EUR": "European",
        "FIN": "Finnish",
        "SAS": "South Asian",
        "OTH": "Other",
    }

    def __init__(
        self,
        *,
        input_paths: str | Path | Iterable[str | Path],
        dataset_id: str = "hbp_mvp_association",
        source: str = "million_veteran_program",
        phenotype_mapper: PhenotypeMapper | None = None,
        include_dataset_types: set[str] | None = None,
        ancestry_code_map: dict[str, str] | None = None,
        chunksize: int = 200_000,
        prefer_parent_phenotype_as_category: bool = True,
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
        self.ancestry_code_map = {
            key.upper(): value
            for key, value in (ancestry_code_map or self.DEFAULT_ANCESTRY_CODE_MAP).items()
        }
        self.chunksize = chunksize
        self.prefer_parent_phenotype_as_category = prefer_parent_phenotype_as_category

    def read(self) -> Iterable[CanonicalRecord]:
        usecols = {
            "SNP_ID",
            "chrom",
            "pos",
            "ref",
            "alt",
            "ea",
            "af",
            "num_samples",
            "case_af",
            "num_cases",
            "control_af",
            "num_controls",
            "or",
            "ci",
            "pval",
            "r2",
            "q_pval",
            "i2",
            "direction",
            "ancestry",
            "gene_symbol",
            "phenotype_key",
            "phenotype_description",
            "parent_phenotype",
            "is_sub_phenotype",
        }

        for input_path in self.input_paths:
            accumulators: dict[tuple[str, str, str, str], _MVPAccumulator] = {}
            frame_iter = pd.read_csv(
                input_path,
                usecols=lambda col: col in usecols,
                chunksize=self.chunksize,
                compression="infer",
            )

            for frame in frame_iter:
                for row in frame.to_dict(orient="records"):
                    self._consume_row(row, source_path=input_path, accumulators=accumulators)

            for key in sorted(accumulators.keys()):
                yield accumulators[key].record

    def _consume_row(
        self,
        row: dict[str, Any],
        *,
        source_path: Path,
        accumulators: dict[tuple[str, str, str, str], _MVPAccumulator],
    ) -> None:
        gene_id = self._to_string(row.get("gene_symbol"))
        variant_id = self._to_string(row.get("SNP_ID"))
        phenotype = self._normalize_phenotype(row.get("phenotype_description"))
        phenotype_key = self._to_string(row.get("phenotype_key"))

        if not gene_id or not variant_id or not phenotype:
            return

        dataset_type, category = self.phenotype_mapper.resolve(phenotype)
        dataset_type = dataset_type.upper() if dataset_type else "CVD"
        if dataset_type not in {"CVD", "TRAIT"}:
            dataset_type = "CVD"

        if not self._should_include_dataset_type(dataset_type, self.include_dataset_types):
            return

        parent_phenotype = self._to_string(row.get("parent_phenotype"))
        if self.prefer_parent_phenotype_as_category and not category and parent_phenotype:
            parent_slug = PhenotypeMapper.normalize(parent_phenotype)
            if parent_slug and not parent_slug.isdigit():
                category = parent_slug

        key = (
            dataset_type,
            gene_id,
            variant_id,
            phenotype_key or phenotype,
        )
        accumulator = accumulators.get(key)
        if accumulator is None:
            accumulator = _MVPAccumulator(
                record=CanonicalRecord(
                    dataset_id=self.dataset_id,
                    dataset_type=dataset_type,
                    source=self.source,
                    gene_id=gene_id,
                    variant_id=variant_id,
                    phenotype=phenotype,
                    disease_category=category,
                    variation_type=self._infer_variation_type(
                        self._to_string(row.get("ref")),
                        self._to_string(row.get("alt")),
                    ),
                    clinical_significance=None,
                    most_severe_consequence=None,
                    p_value=self._to_float(row.get("pval")),
                    pmid=None,
                    ancestry={},
                    metadata={
                        "chrom": self._to_string(row.get("chrom")),
                        "pos": self._to_int(row.get("pos")),
                        "ref": self._to_string(row.get("ref")),
                        "alt": self._to_string(row.get("alt")),
                        "effect_allele": self._to_string(row.get("ea")),
                        "allele_string": self._to_allele_string(row),
                        "phenotype_key": phenotype_key,
                        "phenotype_description": self._to_string(
                            row.get("phenotype_description")
                        ),
                        "parent_phenotype": parent_phenotype,
                        "is_sub_phenotype": self._to_bool(row.get("is_sub_phenotype")),
                        "source_file": str(source_path),
                        "mvp_by_ancestry": {},
                    },
                ),
                best_pvalue=self._to_float(row.get("pval")),
            )
            accumulators[key] = accumulator

        self._update_ancestry(accumulator.record, row)
        self._update_best_statistics(accumulator, row)

    def _update_ancestry(self, record: CanonicalRecord, row: dict[str, Any]) -> None:
        ancestry_code = self._to_string(row.get("ancestry"))
        if ancestry_code is None:
            return

        population = self.ancestry_code_map.get(ancestry_code.upper(), ancestry_code)
        af = self._to_float(row.get("af"))
        if af is not None:
            record.ancestry[population] = af

        per_population = record.metadata.setdefault("mvp_by_ancestry", {})
        per_population[population] = {
            "af": af,
            "case_af": self._to_float(row.get("case_af")),
            "control_af": self._to_float(row.get("control_af")),
            "num_samples": self._to_int(row.get("num_samples")),
            "num_cases": self._to_int(row.get("num_cases")),
            "num_controls": self._to_int(row.get("num_controls")),
            "or": self._to_float(row.get("or")),
            "ci": self._parse_ci(row.get("ci")),
            "r2": self._to_float(row.get("r2")),
            "q_pval": self._to_float(row.get("q_pval")),
            "i2": self._to_float(row.get("i2")),
            "direction": self._to_string(row.get("direction")),
            "raw_ancestry_code": ancestry_code,
        }

    def _update_best_statistics(self, accumulator: _MVPAccumulator, row: dict[str, Any]) -> None:
        candidate = self._to_float(row.get("pval"))
        current = accumulator.best_pvalue
        if not self._is_better_pvalue(candidate, current):
            return

        accumulator.best_pvalue = candidate
        record = accumulator.record
        record.p_value = candidate
        record.metadata.update(
            {
                "best_or": self._to_float(row.get("or")),
                "best_ci": self._parse_ci(row.get("ci")),
                "best_q_pval": self._to_float(row.get("q_pval")),
                "best_r2": self._to_float(row.get("r2")),
                "best_i2": self._to_float(row.get("i2")),
                "best_direction": self._to_string(row.get("direction")),
                "best_num_samples": self._to_int(row.get("num_samples")),
                "best_num_cases": self._to_int(row.get("num_cases")),
                "best_num_controls": self._to_int(row.get("num_controls")),
                "best_case_af": self._to_float(row.get("case_af")),
                "best_control_af": self._to_float(row.get("control_af")),
            }
        )

    @staticmethod
    def _is_better_pvalue(candidate: float | None, current: float | None) -> bool:
        if current is None:
            return True
        if candidate is None:
            return False
        return candidate < current

    @staticmethod
    def _infer_variation_type(ref: str | None, alt: str | None) -> str | None:
        if ref is None or alt is None:
            return None
        return "SNP" if len(ref) == 1 and len(alt) == 1 else "INDEL"

    @staticmethod
    def _to_allele_string(row: dict[str, Any]) -> str | None:
        ref = TabularAdapterMixin._to_string(row.get("ref"))
        alt = TabularAdapterMixin._to_string(row.get("alt"))
        if ref is None or alt is None:
            return None
        return f"{ref}/{alt}"

    @staticmethod
    def _to_int(value: Any) -> int | None:
        as_float = TabularAdapterMixin._to_float(value)
        if as_float is None:
            return None
        try:
            return int(as_float)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_bool(value: Any) -> bool | None:
        text = TabularAdapterMixin._to_string(value)
        if text is None:
            return None
        lowered = text.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        return None

    @staticmethod
    def _parse_ci(value: Any) -> dict[str, Any] | None:
        text = TabularAdapterMixin._to_string(value)
        if text is None:
            return None
        parts = [item.strip() for item in text.split(",")]
        if len(parts) != 2:
            return {"raw": text}

        lower = TabularAdapterMixin._to_float(parts[0])
        upper = TabularAdapterMixin._to_float(parts[1])
        if lower is None or upper is None:
            return {"raw": text}
        return {"lower": lower, "upper": upper}
