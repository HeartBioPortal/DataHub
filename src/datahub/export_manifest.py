"""Manifest-driven preservation and derivation rules for analyzed exports."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from datahub.export_helpers import (
    ExportHelperContext,
    ExportHelperRegistry,
    build_default_export_helper_registry,
)
from datahub.models import CanonicalRecord
from datahub.phenotype_paths import PhenotypePathResolver


@dataclass(frozen=True)
class MetadataFieldSpec:
    name: str
    source: str
    target: str


@dataclass(frozen=True)
class DerivedFieldSpec:
    name: str
    helper: str
    scope: str
    target: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PublishFieldSpec:
    entry_meta: tuple[str, ...] = ()
    overall_meta: tuple[str, ...] = ()


@dataclass(frozen=True)
class ServingFieldSpec:
    top_level_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class AssociationExportManifest:
    manifest_id: str
    version: int
    dataset_types: tuple[str, ...]
    promoted_fields: tuple[str, ...]
    metadata_fields: tuple[MetadataFieldSpec, ...]
    derived_fields: tuple[DerivedFieldSpec, ...]
    publish_fields: PublishFieldSpec
    serving_fields: ServingFieldSpec
    source_overrides: dict[str, str] = field(default_factory=dict)


class AssociationExportManifestLoader:
    """Load and merge association export manifests from config/export_manifests."""

    def __init__(
        self,
        manifests_dir: str | Path | None = None,
        helper_registry: ExportHelperRegistry | None = None,
    ) -> None:
        if manifests_dir is None:
            manifests_dir = Path(__file__).resolve().parents[2] / "config" / "export_manifests"
        self.manifests_dir = Path(manifests_dir)
        self.helper_registry = helper_registry or build_default_export_helper_registry()

    def load(
        self,
        name_or_path: str | Path,
        *,
        source_id: str | None = None,
    ) -> AssociationExportManifest:
        base_path = self._resolve_path(name_or_path)
        payload = json.loads(base_path.read_text())
        override_payload: dict[str, Any] | None = None
        normalized_source = str(source_id).strip().lower() if source_id else ""
        if normalized_source:
            override_ref = payload.get("source_overrides", {}).get(normalized_source)
            if override_ref:
                override_path = (base_path.parent / override_ref).resolve()
                if not override_path.exists():
                    raise FileNotFoundError(
                        f"Export manifest override for source '{normalized_source}' not found: {override_path}"
                    )
                override_payload = json.loads(override_path.read_text())
        merged = self._merge_payload(payload, override_payload)
        return self._parse(merged)

    def _resolve_path(self, name_or_path: str | Path) -> Path:
        requested = Path(name_or_path)
        if requested.exists():
            return requested

        candidate = self.manifests_dir / requested
        if candidate.is_dir():
            base_path = candidate / "base.json"
            if base_path.exists():
                return base_path
        if candidate.exists():
            return candidate

        candidate_json = self.manifests_dir / f"{requested}.json"
        if candidate_json.exists():
            return candidate_json

        raise FileNotFoundError(f"Export manifest not found: {name_or_path}")

    def _merge_payload(
        self,
        base_payload: dict[str, Any],
        override_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if override_payload is None:
            return dict(base_payload)

        merged = dict(base_payload)
        merged["promoted_fields"] = self._merge_string_lists(
            base_payload.get("promoted_fields", []),
            override_payload.get("promoted_fields", []),
        )
        merged["metadata_fields"] = self._merge_named_objects(
            base_payload.get("metadata_fields", []),
            override_payload.get("metadata_fields", []),
            key="target",
        )
        merged["derived_fields"] = self._merge_named_objects(
            base_payload.get("derived_fields", []),
            override_payload.get("derived_fields", []),
            key="name",
        )
        base_publish = dict(base_payload.get("publish_fields", {}))
        override_publish = dict(override_payload.get("publish_fields", {}))
        merged["publish_fields"] = {
            "entry_meta": self._merge_string_lists(
                base_publish.get("entry_meta", []),
                override_publish.get("entry_meta", []),
            ),
            "overall_meta": self._merge_string_lists(
                base_publish.get("overall_meta", []),
                override_publish.get("overall_meta", []),
            ),
        }
        base_serving = dict(base_payload.get("serving_fields", {}))
        override_serving = dict(override_payload.get("serving_fields", {}))
        merged["serving_fields"] = {
            "top_level_keys": self._merge_string_lists(
                base_serving.get("top_level_keys", []),
                override_serving.get("top_level_keys", []),
            )
        }
        return merged

    @staticmethod
    def _merge_string_lists(base_values: Iterable[Any], override_values: Iterable[Any]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for value in [*list(base_values), *list(override_values)]:
            normalized = str(value).strip()
            if not normalized or normalized in seen:
                continue
            merged.append(normalized)
            seen.add(normalized)
        return merged

    @staticmethod
    def _merge_named_objects(
        base_values: Iterable[dict[str, Any]],
        override_values: Iterable[dict[str, Any]],
        *,
        key: str,
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for payload in [*list(base_values), *list(override_values)]:
            if not isinstance(payload, dict):
                continue
            identity = str(payload.get(key, "")).strip()
            if not identity:
                continue
            if identity not in order:
                order.append(identity)
            merged[identity] = dict(payload)
        return [merged[identity] for identity in order]

    def _parse(self, payload: dict[str, Any]) -> AssociationExportManifest:
        helper_names = []
        metadata_fields: list[MetadataFieldSpec] = []
        for raw_field in payload.get("metadata_fields", []):
            name = str(raw_field.get("name", "")).strip()
            source = str(raw_field.get("source", "")).strip()
            target = str(raw_field.get("target", "")).strip()
            if not name or not source or not target:
                raise ValueError("metadata_fields entries require name, source, and target")
            if not (source.startswith("row.") or source.startswith("literal:")):
                raise ValueError(
                    f"Unsupported metadata field source '{source}'. Use row.<field> or literal:<value>."
                )
            metadata_fields.append(MetadataFieldSpec(name=name, source=source, target=target))

        derived_fields: list[DerivedFieldSpec] = []
        derived_names: set[str] = set()
        for raw_field in payload.get("derived_fields", []):
            name = str(raw_field.get("name", "")).strip()
            helper = str(raw_field.get("helper", "")).strip()
            scope = str(raw_field.get("scope", "")).strip().lower()
            target = str(raw_field.get("target", "")).strip()
            if not name or not helper or not scope or not target:
                raise ValueError("derived_fields entries require name, helper, scope, and target")
            if name in derived_names:
                raise ValueError(f"Duplicate derived field name: {name}")
            if scope not in {"axis", "entry", "overall"}:
                raise ValueError(f"Unsupported derived field scope: {scope}")
            derived_names.add(name)
            helper_names.append(helper)
            derived_fields.append(
                DerivedFieldSpec(
                    name=name,
                    helper=helper,
                    scope=scope,
                    target=target,
                    params=dict(raw_field.get("params", {})),
                )
            )

        self.helper_registry.validate(helper_names)

        publish_fields = PublishFieldSpec(
            entry_meta=tuple(
                self._validate_publish_reference(ref, derived_fields, scope="entry")
                for ref in payload.get("publish_fields", {}).get("entry_meta", [])
            ),
            overall_meta=tuple(
                self._validate_publish_reference(ref, derived_fields, scope="overall")
                for ref in payload.get("publish_fields", {}).get("overall_meta", [])
            ),
        )

        return AssociationExportManifest(
            manifest_id=str(payload["manifest_id"]).strip(),
            version=int(payload["version"]),
            dataset_types=tuple(
                str(value).strip().upper()
                for value in payload.get("dataset_types", [])
                if str(value).strip()
            ),
            promoted_fields=tuple(
                str(value).strip()
                for value in payload.get("promoted_fields", [])
                if str(value).strip()
            ),
            metadata_fields=tuple(metadata_fields),
            derived_fields=tuple(derived_fields),
            publish_fields=publish_fields,
            serving_fields=ServingFieldSpec(
                top_level_keys=tuple(
                    str(value).strip()
                    for value in payload.get("serving_fields", {}).get("top_level_keys", [])
                    if str(value).strip()
                )
            ),
            source_overrides={
                str(key).strip().lower(): str(value).strip()
                for key, value in dict(payload.get("source_overrides", {})).items()
                if str(key).strip() and str(value).strip()
            },
        )

    @staticmethod
    def _validate_publish_reference(
        reference: Any,
        derived_fields: list[DerivedFieldSpec],
        *,
        scope: str,
    ) -> str:
        normalized = str(reference).strip()
        if normalized in {"manifest_id", "manifest_version"}:
            return normalized
        if any(field.name == normalized and field.scope == scope for field in derived_fields):
            return normalized
        raise ValueError(
            f"Unknown publish field reference '{normalized}' for scope '{scope}'."
        )


class AssociationExportRuntime:
    """Runtime view of an association export manifest plus helpers."""

    def __init__(
        self,
        manifest: AssociationExportManifest,
        *,
        helper_registry: ExportHelperRegistry | None = None,
        path_resolver: PhenotypePathResolver | None = None,
    ) -> None:
        self.manifest = manifest
        self.helper_registry = helper_registry or build_default_export_helper_registry()
        self.path_resolver = path_resolver
        self._derived_by_name = {field.name: field for field in manifest.derived_fields}
        self._axis_by_target = {
            field.target: field
            for field in manifest.derived_fields
            if field.scope == "axis"
        }

    def extract_metadata(self, row_map: dict[str, Any]) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for field in self.manifest.metadata_fields:
            value = self._resolve_source_value(field.source, row_map)
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            self._set_nested(metadata, field.target, value)
        return metadata

    def resolve_label_path(
        self,
        *,
        record: CanonicalRecord,
        dataset_type: str,
    ) -> tuple[str, ...]:
        helper_spec = self._find_derived(scope="entry", target="label_path")
        if helper_spec is None:
            fallback = [record.disease_category, record.phenotype]
            return tuple(
                segment
                for segment in fallback
                if segment is not None and str(segment).strip()
            )
        helper = self.helper_registry.get(helper_spec.helper)
        return tuple(
            helper(
                record=record,
                context=ExportHelperContext(
                    dataset_type=dataset_type,
                    path_resolver=self.path_resolver,
                ),
                params=helper_spec.params,
            )
        )

    def normalize_axis(self, *, axis: str, value: Any) -> Any:
        helper_spec = self._axis_by_target.get(axis)
        if helper_spec is None:
            return value
        helper = self.helper_registry.get(helper_spec.helper)
        return helper(
            value=value,
            context=ExportHelperContext(),
            params=helper_spec.params,
        )

    def build_publish_meta(
        self,
        *,
        scope: str,
        records: list[CanonicalRecord],
        dataset_type: str,
    ) -> dict[str, Any]:
        if not records:
            return {}

        requested = (
            self.manifest.publish_fields.entry_meta
            if scope == "entry"
            else self.manifest.publish_fields.overall_meta
        )
        if not requested:
            return {}

        payload: dict[str, Any] = {}
        for reference in requested:
            if reference == "manifest_id":
                payload["manifest_id"] = self.manifest.manifest_id
                continue
            if reference == "manifest_version":
                payload["manifest_version"] = self.manifest.version
                continue
            spec = self._derived_by_name[reference]
            helper = self.helper_registry.get(spec.helper)
            value = helper(
                records=records,
                context=ExportHelperContext(
                    dataset_type=dataset_type,
                    path_resolver=self.path_resolver,
                ),
                params=spec.params,
            )
            if value is None:
                continue
            target = spec.target.removeprefix("meta.")
            payload[target] = value
        return payload

    def merge_publish_meta(
        self,
        existing: dict[str, Any] | None,
        new: dict[str, Any] | None,
    ) -> dict[str, Any]:
        existing = dict(existing or {})
        new = dict(new or {})
        if not existing:
            return new
        if not new:
            return existing

        merged = dict(existing)
        if "manifest_id" in new:
            merged["manifest_id"] = existing.get("manifest_id") or new.get("manifest_id")
        if "manifest_version" in new:
            merged["manifest_version"] = existing.get("manifest_version") or new.get("manifest_version")

        if "provenance" in existing or "provenance" in new:
            merged["provenance"] = self._merge_provenance(
                existing.get("provenance", {}),
                new.get("provenance", {}),
            )
        if "ancestry_provenance" in existing or "ancestry_provenance" in new:
            merged["ancestry_provenance"] = self._merge_ancestry_provenance(
                existing.get("ancestry_provenance", {}),
                new.get("ancestry_provenance", {}),
            )
        if "coverage" in existing or "coverage" in new:
            merged["coverage"] = self._merge_coverage(
                existing.get("coverage", {}),
                new.get("coverage", {}),
            )

        for key, value in new.items():
            if key in {"manifest_id", "manifest_version", "provenance", "ancestry_provenance", "coverage"}:
                continue
            if key not in merged:
                merged[key] = value
        return merged

    def normalize_payload_entry(
        self,
        entry: dict[str, Any],
        *,
        dataset_type: str,
    ) -> dict[str, Any]:
        cloned = dict(entry)
        helper_spec = self._find_derived(scope="entry", target="label_path")
        if helper_spec is not None:
            key = "trait" if dataset_type == "TRAIT" else "disease"
            raw_path = cloned.get(key)
            if isinstance(raw_path, list) and raw_path:
                fallback_category = raw_path[-2] if len(raw_path) > 1 else None
                record = CanonicalRecord(
                    dataset_id="",
                    dataset_type=dataset_type,
                    source="",
                    gene_id="",
                    variant_id="",
                    phenotype=str(raw_path[-1]),
                    disease_category=str(fallback_category) if fallback_category is not None else None,
                )
                cloned[key] = list(self.resolve_label_path(record=record, dataset_type=dataset_type))

        axis_helper = self._axis_by_target.get("clinical_significance")
        if axis_helper is not None and isinstance(cloned.get("cs"), list):
            counter: dict[str, int] = {}
            for item in cloned.get("cs", []):
                if not isinstance(item, dict):
                    continue
                normalized = self.normalize_axis(axis="clinical_significance", value=item.get("name"))
                if normalized is None:
                    continue
                counter[normalized] = counter.get(normalized, 0) + int(item.get("value", 0))
            cloned["cs"] = [
                {"name": name, "value": value}
                for name, value in sorted(counter.items(), key=lambda pair: pair[0])
            ]

        if "_datahub" in cloned and "_datahub" not in self.manifest.serving_fields.top_level_keys:
            cloned.pop("_datahub", None)
        return cloned

    def normalize_overall_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        cloned = dict(payload)
        if isinstance(cloned.get("data"), dict):
            data = dict(cloned["data"])
            if isinstance(data.get("cs"), dict):
                normalized_counter: dict[str, int] = {}
                for key, value in data["cs"].items():
                    normalized = self.normalize_axis(axis="clinical_significance", value=key)
                    if normalized is None:
                        continue
                    normalized_counter[normalized] = normalized_counter.get(normalized, 0) + int(value)
                data["cs"] = dict(sorted(normalized_counter.items(), key=lambda item: item[0]))
            cloned["data"] = data
        if "_datahub" in cloned and "_datahub" not in self.manifest.serving_fields.top_level_keys:
            cloned.pop("_datahub", None)
        return cloned

    def _find_derived(self, *, scope: str, target: str) -> DerivedFieldSpec | None:
        for field in self.manifest.derived_fields:
            if field.scope == scope and field.target == target:
                return field
        return None

    @staticmethod
    def _resolve_source_value(source: str, row_map: dict[str, Any]) -> Any:
        if source.startswith("row."):
            return row_map.get(source.removeprefix("row."))
        if source.startswith("literal:"):
            return source.removeprefix("literal:")
        return None

    @staticmethod
    def _set_nested(target: dict[str, Any], dotted_path: str, value: Any) -> None:
        parts = [part for part in str(dotted_path).split(".") if part]
        if not parts:
            return
        cursor = target
        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child
        cursor[parts[-1]] = value

    @staticmethod
    def _merge_provenance(existing: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        source_counts = Counter()
        for payload in (existing.get("source_counts", {}), new.get("source_counts", {})):
            if isinstance(payload, dict):
                source_counts.update({str(key): int(value) for key, value in payload.items()})
        return {
            "sources": sorted(
                {
                    *list(existing.get("sources", []) or []),
                    *list(new.get("sources", []) or []),
                }
            ),
            "source_counts": dict(sorted(source_counts.items(), key=lambda item: item[0])),
            "source_families": sorted(
                {
                    *list(existing.get("source_families", []) or []),
                    *list(new.get("source_families", []) or []),
                }
            ),
            "source_file_count": int(existing.get("source_file_count", 0) or 0)
            + int(new.get("source_file_count", 0) or 0),
        }

    @staticmethod
    def _merge_ancestry_provenance(existing: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        return {
            "canonical_groups": sorted(
                {
                    *list(existing.get("canonical_groups", []) or []),
                    *list(new.get("canonical_groups", []) or []),
                }
            ),
            "source_codes": sorted(
                {
                    *list(existing.get("source_codes", []) or []),
                    *list(new.get("source_codes", []) or []),
                }
            ),
            "source_labels": sorted(
                {
                    *list(existing.get("source_labels", []) or []),
                    *list(new.get("source_labels", []) or []),
                }
            ),
        }

    @staticmethod
    def _merge_coverage(existing: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": str(existing.get("status") or new.get("status") or ""),
            "scope": str(existing.get("scope") or new.get("scope") or ""),
            "record_count": int(existing.get("record_count", 0) or 0)
            + int(new.get("record_count", 0) or 0),
            "dataset_type": str(existing.get("dataset_type") or new.get("dataset_type") or ""),
        }


class AssociationExportManifestCatalog:
    """Cache source-specific resolved manifests for one association export family."""

    def __init__(
        self,
        *,
        base_manifest_ref: str | Path,
        manifests_dir: str | Path | None = None,
        helper_registry: ExportHelperRegistry | None = None,
        path_resolver: PhenotypePathResolver | None = None,
    ) -> None:
        self.loader = AssociationExportManifestLoader(
            manifests_dir=manifests_dir,
            helper_registry=helper_registry,
        )
        self.base_manifest_ref = base_manifest_ref
        self.path_resolver = path_resolver
        self.helper_registry = helper_registry or build_default_export_helper_registry()
        self._cache: dict[str, AssociationExportRuntime] = {}
        self.base_runtime = self.runtime_for_source(None)

    def runtime_for_source(self, source_id: str | None) -> AssociationExportRuntime:
        normalized = str(source_id).strip().lower() if source_id else "__base__"
        if normalized not in self._cache:
            manifest = self.loader.load(
                self.base_manifest_ref,
                source_id=None if normalized == "__base__" else normalized,
            )
            self._cache[normalized] = AssociationExportRuntime(
                manifest,
                helper_registry=self.helper_registry,
                path_resolver=self.path_resolver,
            )
        return self._cache[normalized]
