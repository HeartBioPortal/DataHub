"""Audited normalization for legacy ClinVar-derived classification values."""

from __future__ import annotations

import ast
import json
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from typing import Any


PROVENANCE_LIMITATION = (
    "Legacy ClinVar-derived variant-level classification. The retained source "
    "does not include ClinVar condition, accession, review status, classification "
    "date, or release version and may not refer to the selected HBP phenotype."
)
EVIDENCE_GRANULARITY = "legacy_variant_level_annotation"
CONDITION_STATUS = "unavailable"

TERM_TO_DISPLAY_GROUP = {
    "Benign": "Benign spectrum",
    "Likely benign": "Benign spectrum",
    "Benign/Likely benign": "Benign spectrum",
    "Pathogenic": "Pathogenic spectrum",
    "Likely pathogenic": "Pathogenic spectrum",
    "Pathogenic/Likely pathogenic": "Pathogenic spectrum",
    "Uncertain significance": "Uncertain or conflicting",
    "Conflicting interpretations of pathogenicity": "Uncertain or conflicting",
    "association": "Association and risk context",
    "risk factor": "Association and risk context",
    "protective": "Association and risk context",
    "drug response": "Drug response",
    "Affects": "Other phenotype effect",
    "other": "Details only",
    "not provided": "Details only",
}
APPROVED_TERMS = tuple(TERM_TO_DISPLAY_GROUP)
MISSING_MARKERS = {"", "na", "n/a", "null", "none", "[]"}


def _normalized_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.strip().strip("\"'").strip()
    text = re.sub(r"\s*/\s*", "/", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _term_key(value: Any) -> str:
    text = _normalized_text(value).casefold()
    text = text.replace("_", " ")
    text = re.sub(r"(?<=\w)-(?=\w)", " ", text)
    text = re.sub(r"[.]$", "", text)
    return re.sub(r"\s+", " ", text).strip()


TERM_ALIASES = {_term_key(term): term for term in APPROVED_TERMS}


@dataclass(frozen=True)
class ParsedClinicalClassification:
    raw_value: str
    parse_form: str
    terms: tuple[str, ...]
    display_terms: tuple[str, ...]
    duplicate_terms: dict[str, int]
    unparsed_terms: tuple[str, ...]

    @property
    def meaningful(self) -> bool:
        return bool(self.terms)

    @property
    def confident(self) -> bool:
        return self.meaningful and not self.unparsed_terms


def _canonical_term(value: Any) -> str | None:
    return TERM_ALIASES.get(_term_key(value))


def _split_values(text: str) -> tuple[list[Any], str]:
    if text.startswith("[") and text.endswith("]"):
        try:
            value = json.loads(text)
            if isinstance(value, list):
                return value, "json_array"
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
        try:
            value = ast.literal_eval(text)
            if isinstance(value, (list, tuple)):
                return list(value), "python_list"
        except (TypeError, ValueError, SyntaxError):
            pass
        return re.split(r"\s*[,;]\s*", text[1:-1]), "unquoted_bracketed_list"

    if _canonical_term(text):
        return [text], "single"
    if ";" in text:
        return re.split(r"\s*;\s*", text), "semicolon_delimited"
    if "," in text:
        return re.split(r"\s*,\s*", text), "comma_delimited"
    return [text], "single"


def parse_clinical_classification(value: Any) -> ParsedClinicalClassification:
    """Parse one raw value without modifying it or choosing a representative term."""

    raw_value = "" if value is None else str(value)
    text = raw_value.strip()
    if text.casefold() in MISSING_MARKERS:
        return ParsedClinicalClassification(
            raw_value=raw_value,
            parse_form="missing_placeholder",
            terms=(),
            display_terms=(),
            duplicate_terms={},
            unparsed_terms=(),
        )

    values, parse_form = _split_values(text)
    terms: list[str] = []
    unparsed: list[str] = []
    pending = list(values)
    while pending:
        item = pending.pop(0)
        normalized = _normalized_text(item)
        if not normalized:
            continue
        canonical = _canonical_term(normalized)
        if canonical:
            terms.append(canonical)
            continue
        if ";" in normalized:
            pending[0:0] = re.split(r"\s*;\s*", normalized)
            continue
        if "," in normalized:
            pending[0:0] = re.split(r"\s*,\s*", normalized)
            continue
        unparsed.append(normalized)

    counts = Counter(terms)
    return ParsedClinicalClassification(
        raw_value=raw_value,
        parse_form=parse_form,
        terms=tuple(terms),
        display_terms=tuple(dict.fromkeys(terms)),
        duplicate_terms={term: count for term, count in counts.items() if count > 1},
        unparsed_terms=tuple(dict.fromkeys(unparsed)),
    )


def normalized_clinical_terms(value: Any, *, strict: bool = True) -> list[str]:
    parsed = parse_clinical_classification(value)
    if strict and parsed.unparsed_terms:
        raise ValueError(
            "Unparsed clinical-classification term(s): "
            + "; ".join(parsed.unparsed_terms)
            + f"; raw value={parsed.raw_value!r}"
        )
    return list(parsed.display_terms)


def display_group_for_term(term: str) -> str:
    try:
        return TERM_TO_DISPLAY_GROUP[term]
    except KeyError as exc:
        raise ValueError(f"Clinical-classification term is not approved: {term!r}") from exc
