"""Phenotype hierarchy rollup publisher for association outputs."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from datahub.adapters.phenotypes import PhenotypeMapper
from datahub.models import CanonicalRecord
from datahub.publishers.base import Publisher


class PhenotypeRollupPublisher(Publisher):
    """Publish rollup association payloads grouped by parent phenotype labels.

    The output shape intentionally matches legacy association payload contracts:
    each record contains ``ancestry``, ``vc``, ``msc``, ``cs`` and either
    ``disease`` or ``trait``. The difference is grouping semantics: records are
    rolled up by parent category labels and de-duplicated by ``rsid``.
    """

    def __init__(
        self,
        *,
        output_root: str | Path,
        tree_json_path: str | Path | None = None,
        skip_unknown_axis_values: bool = True,
        deduplicate_variants: bool = True,
        ancestry_value_precision: int | None = None,
        association_subdir: str = "association_rollup",
        overall_subdir: str = "overall_rollup",
    ) -> None:
        self.output_root = Path(output_root)
        self.skip_unknown_axis_values = skip_unknown_axis_values
        self.deduplicate_variants = deduplicate_variants
        self.ancestry_value_precision = ancestry_value_precision
        self.association_subdir = association_subdir
        self.overall_subdir = overall_subdir
        self.rollup_map = self._load_rollup_map(tree_json_path)

    def publish(self, records: list[CanonicalRecord]) -> None:
        grouped: dict[str, dict[str, dict[tuple[str, str], dict[str, CanonicalRecord]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )

        for index, record in enumerate(records):
            dataset_type = (record.dataset_type or "CVD").upper()
            category, label = self._resolve_rollup_target(record, dataset_type)
            rollup_key = (category, label)

            variant_key = record.variant_id
            if not self.deduplicate_variants:
                variant_key = f"{record.variant_id}::{index}"

            bucket = grouped[dataset_type][record.gene_id][rollup_key]
            if variant_key in bucket:
                bucket[variant_key] = self._pick_best_record(bucket[variant_key], record)
            else:
                bucket[variant_key] = record

        for dataset_type, genes in grouped.items():
            association_dir = (
                self.output_root
                / "association"
                / "final"
                / self.association_subdir
                / dataset_type
            )
            overall_dir = (
                self.output_root
                / "association"
                / "final"
                / self.overall_subdir
                / dataset_type
            )
            association_dir.mkdir(parents=True, exist_ok=True)
            overall_dir.mkdir(parents=True, exist_ok=True)

            for gene_id, rollup_groups in genes.items():
                payload: list[dict[str, Any]] = []
                overall_data = {
                    "vc": Counter(),
                    "msc": Counter(),
                    "cs": Counter(),
                    "ancestry": defaultdict(dict),
                }

                for label, variant_map in sorted(rollup_groups.items(), key=lambda item: item[0][1]):
                    selected = list(variant_map.values())
                    payload.append(
                        self._build_entry(
                            dataset_type=dataset_type,
                            label=label,
                            records=selected,
                        )
                    )
                    self._update_overall(overall_data, selected)

                association_path = association_dir / f"{self._safe_gene(gene_id)}.json"
                with association_path.open("w") as stream:
                    json.dump(payload, stream, indent=4)

                overall_path = overall_dir / f"{self._safe_gene(gene_id)}.json"
                with overall_path.open("w") as stream:
                    json.dump(
                        {
                            "data": {
                                "vc": dict(overall_data["vc"]),
                                "msc": dict(overall_data["msc"]),
                                "cs": dict(overall_data["cs"]),
                                "ancestry": dict(overall_data["ancestry"]),
                            },
                            "pvals": {},
                        },
                        stream,
                        indent=4,
                    )

    def _build_entry(
        self,
        *,
        dataset_type: str,
        label: tuple[str, str],
        records: list[CanonicalRecord],
    ) -> dict[str, Any]:
        ancestry_points: dict[str, dict[str, Any]] = defaultdict(dict)
        vc_counter: Counter[str] = Counter()
        msc_counter: Counter[str] = Counter()
        cs_counter: Counter[str] = Counter()

        for record in records:
            self._update_axis_counter(vc_counter, record.variation_type, normalize_case="variation")
            self._update_axis_counter(msc_counter, record.most_severe_consequence)
            self._update_axis_counter(
                cs_counter,
                record.clinical_significance,
                normalize_case="clinical_significance",
            )
            for population, value in record.ancestry.items():
                if self._is_unknown(value):
                    continue
                ancestry_points[population][record.variant_id] = self._normalize_ancestry_value(value)

        entry = {
            "ancestry": [
                {
                    "name": population,
                    "data": [
                        {"rsid": rsid, "value": value}
                        for rsid, value in sorted(point_map.items(), key=lambda item: item[0])
                    ],
                }
                for population, point_map in sorted(ancestry_points.items(), key=lambda item: item[0])
                if point_map
            ],
            "vc": self._counter_to_items(vc_counter),
            "msc": self._counter_to_items(msc_counter),
            "cs": self._counter_to_items(cs_counter),
        }

        category, phenotype = label
        if dataset_type == "TRAIT":
            entry["trait"] = [category, phenotype]
        else:
            entry["disease"] = [category, phenotype]
        return entry

    def _update_overall(self, overall_data: dict[str, Any], records: list[CanonicalRecord]) -> None:
        for record in records:
            self._update_axis_counter(
                overall_data["vc"],
                record.variation_type,
                normalize_case="variation",
            )
            self._update_axis_counter(overall_data["msc"], record.most_severe_consequence)
            self._update_axis_counter(
                overall_data["cs"],
                record.clinical_significance,
                normalize_case="clinical_significance",
            )
            for population, value in record.ancestry.items():
                if self._is_unknown(value):
                    continue
                overall_data["ancestry"][population][record.variant_id] = self._normalize_ancestry_value(value)

    def _resolve_rollup_target(
        self,
        record: CanonicalRecord,
        dataset_type: str,
    ) -> tuple[str, str]:
        phenotype = PhenotypeMapper.normalize(record.phenotype)
        mapped = self.rollup_map.get((dataset_type, phenotype)) or self.rollup_map.get(("", phenotype))

        mapped_category = mapped or ""
        fallback_category = PhenotypeMapper.normalize(record.disease_category) if record.disease_category else ""
        category = mapped_category or fallback_category

        parent_meta = ""
        if isinstance(record.metadata, dict):
            raw_parent = record.metadata.get("parent_phenotype")
            parent_meta = PhenotypeMapper.normalize(str(raw_parent)) if raw_parent is not None else ""

        label = parent_meta if parent_meta and not parent_meta.isdigit() else category
        if not label:
            label = phenotype

        return category, label

    @staticmethod
    def _pick_best_record(existing: CanonicalRecord, candidate: CanonicalRecord) -> CanonicalRecord:
        existing_p = existing.p_value
        candidate_p = candidate.p_value
        if existing_p is None:
            return candidate
        if candidate_p is None:
            return existing
        return candidate if candidate_p < existing_p else existing

    def _update_axis_counter(
        self,
        counter: Counter[str],
        value: str | None,
        *,
        normalize_case: str | None = None,
    ) -> None:
        if self._is_unknown(value):
            if not self.skip_unknown_axis_values:
                counter["Unknown"] += 1
            return

        normalized = str(value).strip()
        if normalize_case == "variation" and normalized.lower() == "snp":
            normalized = "SNP"
        elif normalize_case == "clinical_significance":
            normalized = normalized.lower()

        counter[normalized] += 1

    @staticmethod
    def _counter_to_items(counter: Counter[str]) -> list[dict[str, Any]]:
        return [
            {"name": name, "value": value}
            for name, value in sorted(counter.items(), key=lambda item: item[0])
        ]

    @staticmethod
    def _safe_gene(gene_id: str) -> str:
        return gene_id.replace("/", "-")

    @staticmethod
    def _is_unknown(value: Any) -> bool:
        if value is None:
            return True
        text = str(value).strip()
        return not text or text.lower() in {"nan", "none", "null", "unknown"}

    def _normalize_ancestry_value(self, value: Any) -> Any:
        if self.ancestry_value_precision is None:
            return value
        try:
            return round(float(value), self.ancestry_value_precision)
        except (TypeError, ValueError):
            return value

    @staticmethod
    def _load_rollup_map(tree_json_path: str | Path | None) -> dict[tuple[str, str], str]:
        if tree_json_path is None:
            return {}

        payload = json.loads(Path(tree_json_path).read_text())
        mapping: dict[tuple[str, str], str] = {}

        for dataset_type, categories in payload.items():
            if not isinstance(categories, dict):
                continue
            dtype = str(dataset_type).upper()
            for category, phenotypes in categories.items():
                category_slug = PhenotypeMapper.normalize(str(category))
                if not isinstance(phenotypes, list):
                    continue
                for phenotype in phenotypes:
                    phenotype_slug = PhenotypeMapper.normalize(str(phenotype))
                    if phenotype_slug:
                        mapping[(dtype, phenotype_slug)] = category_slug

        return mapping
