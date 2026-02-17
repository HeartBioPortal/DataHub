"""Legacy-compatible association JSON publisher for HeartBioPortal frontend."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from datahub.models import CanonicalRecord
from datahub.publishers.base import Publisher


class LegacyAssociationPublisher(Publisher):
    """Publish canonical records into existing HBP association JSON contracts."""

    def __init__(
        self,
        *,
        output_root: str | Path,
        skip_unknown_axis_values: bool = True,
        ancestry_value_precision: int | None = None,
        deduplicate_ancestry_points: bool = True,
    ) -> None:
        self.output_root = Path(output_root)
        self.skip_unknown_axis_values = skip_unknown_axis_values
        self.ancestry_value_precision = ancestry_value_precision
        self.deduplicate_ancestry_points = deduplicate_ancestry_points

    def publish(self, records: list[CanonicalRecord]) -> None:
        grouped_by_dtype: dict[str, dict[str, dict[tuple[str, str], list[CanonicalRecord]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for record in records:
            dataset_type = (record.dataset_type or "CVD").upper()
            label = (record.disease_category or "", record.phenotype)
            grouped_by_dtype[dataset_type][record.gene_id][label].append(record)

        for dataset_type, genes in grouped_by_dtype.items():
            association_dir = (
                self.output_root / "association" / "final" / "association" / dataset_type
            )
            overall_dir = self.output_root / "association" / "final" / "overall" / dataset_type
            association_dir.mkdir(parents=True, exist_ok=True)
            overall_dir.mkdir(parents=True, exist_ok=True)

            for gene_id, phenotype_groups in genes.items():
                association_payload = []
                overall_data = {
                    "vc": Counter(),
                    "msc": Counter(),
                    "cs": Counter(),
                    "ancestry": defaultdict(dict),
                }

                for label, phenotype_records in sorted(phenotype_groups.items(), key=lambda item: item[0][1]):
                    association_payload.append(
                        self._build_association_entry(
                            dataset_type=dataset_type,
                            label=label,
                            records=phenotype_records,
                        )
                    )

                    self._update_overall(overall_data, phenotype_records)

                association_path = association_dir / f"{self._safe_gene(gene_id)}.json"
                with association_path.open("w") as stream:
                    json.dump(association_payload, stream, indent=4)

                overall_path = overall_dir / f"{self._safe_gene(gene_id)}.json"
                with overall_path.open("w") as stream:
                    json.dump(
                        {
                            "data": {
                                "vc": dict(overall_data["vc"]),
                                "msc": dict(overall_data["msc"]),
                                "cs": dict(overall_data["cs"]),
                                "ancestry": {
                                    key: value
                                    for key, value in overall_data["ancestry"].items()
                                },
                            },
                            "pvals": {},
                        },
                        stream,
                        indent=4,
                    )

    def _build_association_entry(
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
                normalized_value = self._normalize_ancestry_value(value)
                if self.deduplicate_ancestry_points:
                    ancestry_points[population][record.variant_id] = normalized_value
                else:
                    ancestry_points[population][f"{record.variant_id}::{len(ancestry_points[population])}"] = {
                        "rsid": record.variant_id,
                        "value": normalized_value,
                    }

        payload = {
            "ancestry": [
                {
                    "name": population,
                    "data": self._ancestry_points_to_payload(
                        population_points,
                    ),
                }
                for population, population_points in sorted(
                    ancestry_points.items(),
                    key=lambda item: item[0],
                )
                if population_points
            ],
            "vc": self._counter_to_items(vc_counter),
            "msc": self._counter_to_items(msc_counter),
            "cs": self._counter_to_items(cs_counter),
        }

        category, phenotype = label
        if dataset_type == "TRAIT":
            payload["trait"] = [category, phenotype]
        else:
            payload["disease"] = [category, phenotype]

        return payload

    def _update_overall(
        self,
        overall_data: dict[str, Any],
        records: list[CanonicalRecord],
    ) -> None:
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

    def _is_unknown(self, value: Any) -> bool:
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

    def _ancestry_points_to_payload(self, population_points: dict[str, Any]) -> list[dict[str, Any]]:
        if not self.deduplicate_ancestry_points:
            return list(population_points.values())

        return [
            {"rsid": rsid, "value": value}
            for rsid, value in sorted(population_points.items(), key=lambda item: item[0])
        ]
