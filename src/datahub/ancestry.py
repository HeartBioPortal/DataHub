"""Shared ancestry normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class MVPAncestryDefinition:
    """Resolved ancestry metadata for one MVP ancestry code."""

    source_code: str
    source_label: str
    canonical_group: str


MVP_ANCESTRY_DEFINITIONS: dict[str, MVPAncestryDefinition] = {
    "ALL": MVPAncestryDefinition(
        source_code="ALL",
        source_label="Total",
        canonical_group="Total",
    ),
    "AFR": MVPAncestryDefinition(
        source_code="AFR",
        source_label="African",
        canonical_group="African",
    ),
    "AMR": MVPAncestryDefinition(
        source_code="AMR",
        source_label="Admixed American",
        canonical_group="Admixed American",
    ),
    "ASJ": MVPAncestryDefinition(
        source_code="ASJ",
        source_label="Ashkenazi Jewish",
        canonical_group="Ashkenazi Jewish",
    ),
    "EAS": MVPAncestryDefinition(
        source_code="EAS",
        source_label="East Asian",
        canonical_group="East Asian",
    ),
    "EUR": MVPAncestryDefinition(
        source_code="EUR",
        source_label="European",
        canonical_group="European",
    ),
    "FIN": MVPAncestryDefinition(
        source_code="FIN",
        source_label="Finnish",
        canonical_group="Finnish",
    ),
    "SAS": MVPAncestryDefinition(
        source_code="SAS",
        source_label="South Asian",
        canonical_group="South Asian",
    ),
    "OTH": MVPAncestryDefinition(
        source_code="OTH",
        source_label="Other",
        canonical_group="Other",
    ),
}


def normalize_mvp_ancestry_code(value: str | None) -> str | None:
    """Return a stable uppercase ancestry code or ``None`` for empty input."""

    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized or None


def resolve_mvp_ancestry(value: str | None) -> MVPAncestryDefinition | None:
    """Resolve an MVP ancestry code into source and canonical labels."""

    return resolve_mvp_ancestry_with_overrides(value)


def resolve_mvp_ancestry_with_overrides(
    value: str | None,
    ancestry_definitions: Mapping[str, str | tuple[str, str] | MVPAncestryDefinition] | None = None,
) -> MVPAncestryDefinition | None:
    """Resolve an MVP ancestry code with optional caller-provided overrides."""

    normalized = normalize_mvp_ancestry_code(value)
    if normalized is None:
        return None

    if ancestry_definitions:
        override = ancestry_definitions.get(normalized)
        if isinstance(override, MVPAncestryDefinition):
            return override
        if isinstance(override, tuple) and len(override) == 2:
            return MVPAncestryDefinition(
                source_code=normalized,
                source_label=str(override[0]),
                canonical_group=str(override[1]),
            )
        if isinstance(override, str):
            return MVPAncestryDefinition(
                source_code=normalized,
                source_label=override,
                canonical_group=override,
            )

    resolved = MVP_ANCESTRY_DEFINITIONS.get(normalized)
    if resolved is not None:
        return resolved

    return MVPAncestryDefinition(
        source_code=normalized,
        source_label=normalized,
        canonical_group=normalized,
    )


def mvp_sql_case_for_source_label(column_name: str) -> str:
    """SQL CASE expression returning preserved source ancestry labels."""

    return _mvp_sql_case(column_name=column_name, field_name="source_label")


def mvp_sql_case_for_canonical_group(column_name: str) -> str:
    """SQL CASE expression returning canonical broad ancestry groups."""

    return _mvp_sql_case(column_name=column_name, field_name="canonical_group")


def _mvp_sql_case(*, column_name: str, field_name: str) -> str:
    lines = [f"CASE {column_name}"]
    for code, definition in sorted(MVP_ANCESTRY_DEFINITIONS.items()):
        value = getattr(definition, field_name).replace("'", "''")
        lines.append(f"    WHEN '{code}' THEN '{value}'")
    lines.append("    WHEN '' THEN NULL")
    lines.append(f"    ELSE {column_name}")
    lines.append("END")
    return "\n".join(lines)
