"""Named helper registry for manifest-driven analyzed export behavior."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from datahub.axis_normalization import normalize_axis_value
from datahub.models import CanonicalRecord
from datahub.phenotype_paths import PhenotypePathResolver


ExportHelper = Callable[..., Any]


@dataclass(frozen=True)
class ExportHelperContext:
    """Runtime context passed to export helpers."""

    dataset_type: str | None = None
    path_resolver: PhenotypePathResolver | None = None


class ExportHelperRegistry:
    """Registry of stable helper IDs referenced by export manifests."""

    def __init__(self) -> None:
        self._helpers: dict[str, ExportHelper] = {}

    def register(self, name: str, helper: ExportHelper) -> None:
        normalized = str(name).strip()
        if not normalized:
            raise ValueError("helper name cannot be empty")
        self._helpers[normalized] = helper

    def get(self, name: str) -> ExportHelper:
        if name not in self._helpers:
            raise KeyError(
                f"Unknown export helper '{name}'. Available: {', '.join(sorted(self._helpers))}"
            )
        return self._helpers[name]

    def validate(self, names: Iterable[str]) -> None:
        for name in names:
            self.get(name)

    def available(self) -> list[str]:
        return sorted(self._helpers)


def _helper_canonical_phenotype_path(
    *,
    record: CanonicalRecord,
    context: ExportHelperContext,
    params: dict[str, Any] | None = None,
) -> tuple[str, ...]:
    del params
    fallback_path = [record.disease_category, record.phenotype]
    if context.path_resolver is None:
        return tuple(
            segment
            for segment in fallback_path
            if segment is not None and str(segment).strip()
        )

    dataset_type = (context.dataset_type or record.dataset_type or "CVD").upper()
    return context.path_resolver.resolve_leaf_path(
        dataset_type=dataset_type,
        phenotype=record.phenotype,
        fallback_category=record.disease_category,
        fallback_path=fallback_path,
    )


def _helper_normalized_clinical_significance(
    *,
    value: Any,
    context: ExportHelperContext,
    params: dict[str, Any] | None = None,
) -> str | None:
    del context, params
    return normalize_axis_value(value, axis="clinical_significance")


def _helper_source_provenance_bundle(
    *,
    records: list[CanonicalRecord],
    context: ExportHelperContext,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del context, params
    source_counts = Counter(str(record.source).strip() for record in records if str(record.source).strip())
    source_families = sorted(
        {
            str(source_family).strip()
            for record in records
            for source_family in [((record.metadata or {}).get("provenance", {}) or {}).get("source_family")]
            if str(source_family).strip()
        }
    )
    source_files = sorted(
        {
            str(source_file).strip()
            for record in records
            for source_file in [(record.metadata or {}).get("source_file")]
            if str(source_file).strip()
        }
    )

    return {
        "sources": sorted(source_counts),
        "source_counts": dict(sorted(source_counts.items(), key=lambda item: item[0])),
        "source_families": source_families,
        "source_file_count": len(source_files),
    }


def _helper_ancestry_provenance_bundle(
    *,
    records: list[CanonicalRecord],
    context: ExportHelperContext,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del context, params
    canonical_groups = sorted(
        {
            str(group).strip()
            for record in records
            for group in record.ancestry.keys()
            if str(group).strip()
        }
    )
    source_codes = sorted(
        {
            str(payload.get("source_ancestry_code")).strip()
            for record in records
            for payload in ((record.metadata or {}).get("source_ancestry", {}) or {}).values()
            if str(payload.get("source_ancestry_code", "")).strip()
        }
    )
    source_labels = sorted(
        {
            str(payload.get("source_ancestry_label")).strip()
            for record in records
            for payload in ((record.metadata or {}).get("source_ancestry", {}) or {}).values()
            if str(payload.get("source_ancestry_label", "")).strip()
        }
    )
    return {
        "canonical_groups": canonical_groups,
        "source_codes": source_codes,
        "source_labels": source_labels,
    }


def _helper_coverage_stub(
    *,
    records: list[CanonicalRecord],
    context: ExportHelperContext,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scope = str((params or {}).get("scope", "association")).strip() or "association"
    return {
        "status": "not_computed",
        "scope": scope,
        "record_count": len(records),
        "dataset_type": (context.dataset_type or "").upper(),
    }


def build_default_export_helper_registry() -> ExportHelperRegistry:
    """Create the built-in helper registry used by export manifests."""

    registry = ExportHelperRegistry()
    registry.register("canonical_phenotype_path", _helper_canonical_phenotype_path)
    registry.register(
        "normalized_clinical_significance",
        _helper_normalized_clinical_significance,
    )
    registry.register("source_provenance_bundle", _helper_source_provenance_bundle)
    registry.register("ancestry_provenance_bundle", _helper_ancestry_provenance_bundle)
    registry.register("coverage_stub", _helper_coverage_stub)
    return registry
