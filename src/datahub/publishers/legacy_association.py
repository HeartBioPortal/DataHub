"""Legacy-compatible association JSON publisher for HeartBioPortal frontend."""

from __future__ import annotations

import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from datahub.axis_normalization import is_unknown_axis_value, normalize_axis_value
from datahub.export_manifest import AssociationExportRuntime
from datahub.models import CanonicalRecord
from datahub.phenotype_paths import PhenotypePathResolver
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
        json_indent: int | None = 4,
        json_compression: str = "none",
        json_gzip_level: int = 6,
        tree_json_path: str | Path | None = None,
        export_runtime: AssociationExportRuntime | None = None,
    ) -> None:
        self.output_root = Path(output_root)
        self.skip_unknown_axis_values = skip_unknown_axis_values
        self.ancestry_value_precision = ancestry_value_precision
        self.deduplicate_ancestry_points = deduplicate_ancestry_points
        self.incremental_merge = incremental_merge
        self.json_indent = json_indent
        self.json_compression = str(json_compression).strip().lower()
        self.json_gzip_level = int(json_gzip_level)
        self.path_resolver = (
            PhenotypePathResolver.from_tree_json(tree_json_path)
            if tree_json_path
            else None
        )
        self.export_runtime = export_runtime
        if self.json_compression not in {"none", "gzip"}:
            raise ValueError("json_compression must be one of: none, gzip")

    def publish(self, records: list[CanonicalRecord]) -> None:
        grouped_by_dtype: dict[str, dict[str, dict[tuple[str, ...], list[CanonicalRecord]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for record in records:
            dataset_type = (record.dataset_type or "CVD").upper()
            label = self._resolve_label_path(record, dataset_type)
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

                for label, phenotype_records in sorted(
                    phenotype_groups.items(),
                    key=lambda item: item[0][-1] if item[0] else "",
                ):
                    association_payload.append(
                        self._build_association_entry(
                            dataset_type=dataset_type,
                            label=label,
                            records=phenotype_records,
                        )
                    )

                association_path = association_dir / f"{self._safe_gene(gene_id)}.json"
                if self.incremental_merge and self._payload_exists(association_path):
                    existing_payload = self._read_payload(association_path)
                    association_payload = self._merge_association_payload(
                        existing_payload,
                        association_payload,
                    )
                self._write_payload(association_path, association_payload)

                gene_records = [
                    record
                    for phenotype_records in phenotype_groups.values()
                    for record in phenotype_records
                ]
                overall_data = self._build_overall_data(gene_records)
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
                if self.export_runtime is not None:
                    meta = self.export_runtime.build_publish_meta(
                        scope="overall",
                        records=gene_records,
                        dataset_type=dataset_type,
                    )
                    if meta:
                        overall_payload["_datahub"] = meta
                if self.incremental_merge and self._payload_exists(overall_path):
                    overall_existing = self._read_payload(overall_path)
                    overall_payload = self._merge_overall_payload(overall_existing, overall_payload)
                self._write_payload(overall_path, overall_payload)

    def _payload_exists(self, base_path: Path) -> bool:
        return base_path.exists() or base_path.with_suffix(base_path.suffix + ".gz").exists()

    def _read_payload(self, base_path: Path) -> Any:
        if base_path.exists():
            return json.loads(base_path.read_text())

        gz_path = base_path.with_suffix(base_path.suffix + ".gz")
        if gz_path.exists():
            with gzip.open(gz_path, "rt", encoding="utf-8") as stream:
                return json.load(stream)

        raise FileNotFoundError(f"Payload not found: {base_path}")

    def _write_payload(self, base_path: Path, payload: Any) -> None:
        base_path.parent.mkdir(parents=True, exist_ok=True)
        gz_path = base_path.with_suffix(base_path.suffix + ".gz")

        if self.json_compression == "gzip":
            with gzip.open(gz_path, "wt", encoding="utf-8", compresslevel=self.json_gzip_level) as stream:
                json.dump(
                    payload,
                    stream,
                    indent=self.json_indent,
                    separators=(",", ":") if self.json_indent is None else None,
                )
            if base_path.exists():
                base_path.unlink()
            return

        with base_path.open("w") as stream:
            json.dump(
                payload,
                stream,
                indent=self.json_indent,
                separators=(",", ":") if self.json_indent is None else None,
            )
        if gz_path.exists():
            gz_path.unlink()

    def _build_association_entry(
        self,
        *,
        dataset_type: str,
        label: tuple[str, ...],
        records: list[CanonicalRecord],
    ) -> dict[str, Any]:
        axis_records = self._select_best_records_by_variant(records)
        ancestry_records = axis_records if self.deduplicate_ancestry_points else records
        ancestry_points: dict[str, dict[str, Any]] = defaultdict(dict)
        vc_counter: Counter[str] = Counter()
        msc_counter: Counter[str] = Counter()
        cs_counter: Counter[str] = Counter()

        for record in axis_records:
            self._update_axis_counter(vc_counter, record.variation_type, normalize_case="variation")
            self._update_axis_counter(
                msc_counter,
                record.most_severe_consequence,
                normalize_case="most_severe_consequence",
            )
            self._update_axis_counter(
                cs_counter,
                record.clinical_significance,
                normalize_case="clinical_significance",
            )

        for record in ancestry_records:
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

        if dataset_type == "TRAIT":
            payload["trait"] = list(label)
        else:
            payload["disease"] = list(label)

        if self.export_runtime is not None:
            meta = self.export_runtime.build_publish_meta(
                scope="entry",
                records=records,
                dataset_type=dataset_type,
            )
            if meta:
                payload["_datahub"] = meta

        return payload

    def _resolve_label_path(
        self,
        record: CanonicalRecord,
        dataset_type: str,
    ) -> tuple[str, ...]:
        fallback_path = [record.disease_category, record.phenotype]
        if self.path_resolver is None:
            if self.export_runtime is None:
                return tuple(
                    segment
                    for segment in fallback_path
                    if segment is not None and str(segment).strip()
                )
            return self.export_runtime.resolve_label_path(
                record=record,
                dataset_type=dataset_type,
            )

        if self.export_runtime is not None:
            return self.export_runtime.resolve_label_path(
                record=record,
                dataset_type=dataset_type,
            )

        return self.path_resolver.resolve_leaf_path(
            dataset_type=dataset_type,
            phenotype=record.phenotype,
            fallback_category=record.disease_category,
            fallback_path=fallback_path,
        )

    def _build_overall_data(self, records: list[CanonicalRecord]) -> dict[str, Any]:
        overall_data = {
            "vc": Counter(),
            "msc": Counter(),
            "cs": Counter(),
            "ancestry": defaultdict(dict),
        }
        for record in self._select_best_records_by_variant(records):
            self._update_axis_counter(
                overall_data["vc"],
                record.variation_type,
                normalize_case="variation",
            )
            self._update_axis_counter(
                overall_data["msc"],
                record.most_severe_consequence,
                normalize_case="most_severe_consequence",
            )
            self._update_axis_counter(
                overall_data["cs"],
                record.clinical_significance,
                normalize_case="clinical_significance",
            )

            for population, value in record.ancestry.items():
                if self._is_unknown(value):
                    continue
                overall_data["ancestry"][population][record.variant_id] = self._normalize_ancestry_value(value)
        return overall_data

    def _select_best_records_by_variant(
        self,
        records: list[CanonicalRecord],
    ) -> list[CanonicalRecord]:
        by_variant: dict[str, CanonicalRecord] = {}
        for record in records:
            existing = by_variant.get(record.variant_id)
            if existing is None:
                by_variant[record.variant_id] = record
                continue
            by_variant[record.variant_id] = self._pick_best_record(existing, record)
        return [
            by_variant[variant_id]
            for variant_id in sorted(by_variant.keys())
        ]

    def _update_axis_counter(
        self,
        counter: Counter[str],
        value: str | None,
        *,
        normalize_case: str | None = None,
    ) -> None:
        axis = "variation" if normalize_case == "variation" else normalize_case
        if self.export_runtime is not None and axis is not None:
            normalized = self.export_runtime.normalize_axis(axis=axis, value=value)
        else:
            normalized = normalize_axis_value(value, axis=axis or "generic")
        if normalized is None:
            if not self.skip_unknown_axis_values:
                counter["Unknown"] += 1
            return

        counter[normalized] += 1

    @staticmethod
    def _counter_to_items(counter: Counter[str]) -> list[dict[str, Any]]:
        return [
            {"name": name, "value": value}
            for name, value in sorted(counter.items(), key=lambda item: item[0])
        ]

    @staticmethod
    def _pick_best_record(existing: CanonicalRecord, candidate: CanonicalRecord) -> CanonicalRecord:
        existing_p = existing.p_value
        candidate_p = candidate.p_value
        if existing_p is None:
            return candidate
        if candidate_p is None:
            return existing
        return candidate if candidate_p < existing_p else existing

    @staticmethod
    def _safe_gene(gene_id: str) -> str:
        return gene_id.replace("/", "-")

    def _is_unknown(self, value: Any) -> bool:
        return is_unknown_axis_value(value)

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
        merged: dict[tuple[str, ...], dict[str, Any]] = {}

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
            for key in sorted(merged.keys(), key=lambda item: item[1:])
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
        if self.export_runtime is not None:
            merged_meta = self.export_runtime.merge_publish_meta(
                existing.get("_datahub"),
                new.get("_datahub"),
            )
            if merged_meta:
                result["_datahub"] = merged_meta
        return result

    def _merge_overall_payload(
        self,
        existing: dict[str, Any],
        new: dict[str, Any],
    ) -> dict[str, Any]:
        existing_data = existing.get("data", {})
        new_data = new.get("data", {})
        merged_payload = {
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
        if self.export_runtime is not None:
            merged_meta = self.export_runtime.merge_publish_meta(
                existing.get("_datahub"),
                new.get("_datahub"),
            )
            if merged_meta:
                merged_payload["_datahub"] = merged_meta
        return merged_payload

    @staticmethod
    def _entry_key(entry: dict[str, Any]) -> tuple[str, ...] | None:
        if "disease" in entry and isinstance(entry["disease"], list) and entry["disease"]:
            return ("disease", *[str(item) for item in entry["disease"]])
        if "trait" in entry and isinstance(entry["trait"], list) and entry["trait"]:
            return ("trait", *[str(item) for item in entry["trait"]])
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
