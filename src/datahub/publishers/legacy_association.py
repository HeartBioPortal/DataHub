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
        incremental_merge: bool = False,
    ) -> None:
        self.output_root = Path(output_root)
        self.skip_unknown_axis_values = skip_unknown_axis_values
        self.ancestry_value_precision = ancestry_value_precision
        self.deduplicate_ancestry_points = deduplicate_ancestry_points
        self.incremental_merge = incremental_merge

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
                if self.incremental_merge and association_path.exists():
                    existing_payload = json.loads(association_path.read_text())
                    association_payload = self._merge_association_payload(
                        existing_payload,
                        association_payload,
                    )
                with association_path.open("w") as stream:
                    json.dump(association_payload, stream, indent=4)

                overall_path = overall_dir / f"{self._safe_gene(gene_id)}.json"
                overall_payload = {
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
                }
                if self.incremental_merge and overall_path.exists():
                    overall_existing = json.loads(overall_path.read_text())
                    overall_payload = self._merge_overall_payload(overall_existing, overall_payload)
                with overall_path.open("w") as stream:
                    json.dump(overall_payload, stream, indent=4)

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

    def _merge_association_payload(
        self,
        existing_payload: list[dict[str, Any]],
        new_payload: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[tuple[str, str, str], dict[str, Any]] = {}

        for entry in existing_payload:
            key = self._entry_key(entry)
            if key is not None:
                merged[key] = entry

        for entry in new_payload:
            key = self._entry_key(entry)
            if key is None:
                continue
            if key not in merged:
                merged[key] = entry
                continue
            merged[key] = self._merge_association_entry(merged[key], entry)

        return [
            merged[key]
            for key in sorted(merged.keys(), key=lambda item: item[2])
        ]

    def _merge_association_entry(
        self,
        existing: dict[str, Any],
        new: dict[str, Any],
    ) -> dict[str, Any]:
        result = dict(existing)
        result["vc"] = self._merge_counter_items(existing.get("vc", []), new.get("vc", []))
        result["msc"] = self._merge_counter_items(existing.get("msc", []), new.get("msc", []))
        result["cs"] = self._merge_counter_items(existing.get("cs", []), new.get("cs", []))
        result["ancestry"] = self._merge_ancestry_items(
            existing.get("ancestry", []),
            new.get("ancestry", []),
        )
        return result

    def _merge_overall_payload(
        self,
        existing: dict[str, Any],
        new: dict[str, Any],
    ) -> dict[str, Any]:
        existing_data = existing.get("data", {})
        new_data = new.get("data", {})
        return {
            "data": {
                "vc": self._merge_counter_dict(
                    existing_data.get("vc", {}),
                    new_data.get("vc", {}),
                ),
                "msc": self._merge_counter_dict(
                    existing_data.get("msc", {}),
                    new_data.get("msc", {}),
                ),
                "cs": self._merge_counter_dict(
                    existing_data.get("cs", {}),
                    new_data.get("cs", {}),
                ),
                "ancestry": self._merge_ancestry_map(
                    existing_data.get("ancestry", {}),
                    new_data.get("ancestry", {}),
                ),
            },
            "pvals": {},
        }

    @staticmethod
    def _entry_key(entry: dict[str, Any]) -> tuple[str, str, str] | None:
        if "disease" in entry and isinstance(entry["disease"], list) and len(entry["disease"]) == 2:
            return ("disease", str(entry["disease"][0]), str(entry["disease"][1]))
        if "trait" in entry and isinstance(entry["trait"], list) and len(entry["trait"]) == 2:
            return ("trait", str(entry["trait"][0]), str(entry["trait"][1]))
        return None

    @staticmethod
    def _merge_counter_items(
        existing_items: list[dict[str, Any]],
        new_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        counter = Counter(
            {
                str(item.get("name")): int(item.get("value", 0))
                for item in existing_items
            }
        )
        counter.update(
            {
                str(item.get("name")): int(item.get("value", 0))
                for item in new_items
            }
        )
        return [
            {"name": name, "value": value}
            for name, value in sorted(counter.items(), key=lambda item: item[0])
        ]

    @staticmethod
    def _merge_counter_dict(existing: dict[str, Any], new: dict[str, Any]) -> dict[str, int]:
        counter = Counter({str(key): int(value) for key, value in existing.items()})
        counter.update({str(key): int(value) for key, value in new.items()})
        return dict(counter)

    @staticmethod
    def _merge_ancestry_items(
        existing_items: list[dict[str, Any]],
        new_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = defaultdict(dict)
        for item in existing_items:
            population = str(item.get("name", ""))
            data = item.get("data", [])
            if not population or not isinstance(data, list):
                continue
            for point in data:
                rsid = str(point.get("rsid", ""))
                if rsid:
                    merged[population][rsid] = point.get("value")
        for item in new_items:
            population = str(item.get("name", ""))
            data = item.get("data", [])
            if not population or not isinstance(data, list):
                continue
            for point in data:
                rsid = str(point.get("rsid", ""))
                if rsid:
                    merged[population][rsid] = point.get("value")

        return [
            {
                "name": population,
                "data": [
                    {"rsid": rsid, "value": value}
                    for rsid, value in sorted(points.items(), key=lambda item: item[0])
                ],
            }
            for population, points in sorted(merged.items(), key=lambda item: item[0])
            if points
        ]

    @staticmethod
    def _merge_ancestry_map(
        existing: dict[str, Any],
        new: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = defaultdict(dict)
        for population, mapping in existing.items():
            if isinstance(mapping, dict):
                merged[str(population)].update(mapping)
        for population, mapping in new.items():
            if isinstance(mapping, dict):
                merged[str(population)].update(mapping)
        return dict(merged)
