"""Normalization helpers for association axis values."""

from __future__ import annotations

from ast import literal_eval
from collections import Counter
from typing import Any

UNKNOWN_AXIS_VALUES = {
    "",
    "na",
    "n/a",
    "nan",
    "none",
    "null",
    "unknown",
    "missing",
    "not available",
    "not provided",
    "not specified",
    "unspecified",
}

_VARIATION_TYPE_CANONICAL = {
    "snp": "SNP",
    "snv": "SNV",
    "indel": "INDEL",
    "insertion": "Insertion",
    "deletion": "Deletion",
    "duplication": "Duplication",
    "inversion": "Inversion",
    "cnv": "CNV",
    "copy number variant": "CNV",
    "structural variant": "Structural variant",
    "sv": "SV",
    "mnv": "MNV",
}

_CLINICAL_SIGNIFICANCE_PRIORITY = {
    "pathogenic": 1,
    "pathogenic/likely pathogenic": 2,
    "likely pathogenic": 3,
    "risk factor": 4,
    "association": 5,
    "drug response": 6,
    "conflicting interpretations of pathogenicity": 7,
    "uncertain significance": 8,
    "likely benign": 9,
    "benign": 10,
    "benign/likely benign": 11,
    "other": 12,
    "protective": 13,
}


def normalize_text_label(value: Any) -> str:
    text = str(value).strip()
    text = text.strip("\"'")
    text = " ".join(text.replace("_", " ").split())
    return text


def is_unknown_axis_value(value: Any) -> bool:
    if value is None:
        return True
    return normalize_text_label(value).lower() in UNKNOWN_AXIS_VALUES


def normalize_variation_label(value: Any) -> str | None:
    if is_unknown_axis_value(value):
        return None
    normalized = normalize_text_label(value)
    folded = normalized.lower()
    if folded in _VARIATION_TYPE_CANONICAL:
        return _VARIATION_TYPE_CANONICAL[folded]
    return normalized


def normalize_most_severe_consequence_label(value: Any) -> str | None:
    if is_unknown_axis_value(value):
        return None
    return normalize_text_label(value).lower()


def _parse_list_like_label(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [normalize_text_label(item) for item in value]

    text = str(value).strip()
    if not (text.startswith("[") and text.endswith("]")):
        return [normalize_text_label(text)]

    try:
        parsed = literal_eval(text)
    except (ValueError, SyntaxError):
        parsed = None

    if isinstance(parsed, (list, tuple, set)):
        return [normalize_text_label(item) for item in parsed]

    inner = text[1:-1].strip()
    if not inner:
        return []
    return [normalize_text_label(part) for part in inner.split(",")]


def normalize_clinical_significance_label(value: Any) -> str | None:
    if is_unknown_axis_value(value):
        return None

    labels = []
    for item in _parse_list_like_label(value):
        normalized = normalize_text_label(item).lower()
        if normalized in UNKNOWN_AXIS_VALUES:
            continue
        if normalized:
            labels.append(normalized)

    if not labels:
        return None

    unique_labels = list(dict.fromkeys(labels))
    return min(unique_labels, key=lambda item: (_CLINICAL_SIGNIFICANCE_PRIORITY.get(item, 20), item))


def normalize_counter_items(
    items: list[dict[str, Any]],
    *,
    axis: str,
    skip_unknown: bool = True,
) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for item in items:
        name = item.get("name")
        value = int(item.get("value", 0))
        normalized = normalize_axis_value(name, axis=axis)
        if normalized is None:
            if not skip_unknown:
                counter["Unknown"] += value
            continue
        counter[normalized] += value
    return [
        {"name": name, "value": value}
        for name, value in sorted(counter.items(), key=lambda entry: entry[0])
    ]


def normalize_counter_mapping(
    data: dict[str, Any],
    *,
    axis: str,
    skip_unknown: bool = True,
) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for name, value in data.items():
        normalized = normalize_axis_value(name, axis=axis)
        if normalized is None:
            if not skip_unknown:
                counter["Unknown"] += int(value or 0)
            continue
        counter[normalized] += int(value or 0)
    return dict(sorted(counter.items(), key=lambda entry: entry[0]))


def normalize_axis_value(value: Any, *, axis: str) -> str | None:
    if axis == "variation":
        return normalize_variation_label(value)
    if axis == "clinical_significance":
        return normalize_clinical_significance_label(value)
    if axis == "most_severe_consequence":
        return normalize_most_severe_consequence_label(value)
    if is_unknown_axis_value(value):
        return None
    return normalize_text_label(value)
