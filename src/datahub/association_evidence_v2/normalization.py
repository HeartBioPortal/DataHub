"""Deterministic normalization helpers for association evidence v2."""

from __future__ import annotations

import ast
import hashlib
import json
import math
import re
from typing import Any


MISSING_TOKENS = {"", "na", "nan", "none", "null", "n/a", "."}
SEQUENCE_ALLELE = re.compile(r"^[ACGTN]+$", flags=re.IGNORECASE)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_id(prefix: str, value: Any, *, length: int = 24) -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest[:length]}"


def clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if text.lower() in MISSING_TOKENS else text


def finite_number(value: Any) -> float | None:
    text = clean(value)
    if text is None:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def phenotype_slug(value: Any) -> str:
    text = clean(value) or ""
    return re.sub(r"\s+", "_", text.replace("/", "_").strip().lower())


def normalized_alleles(value: Any) -> list[str]:
    text = clean(value)
    if text is None:
        return []
    try:
        parsed = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        # Some archived association exports use unquoted list notation such as
        # ``[A, G]``. Strip only a balanced outer bracket pair before applying
        # the delimiter parser; allele roles remain intentionally unordered.
        candidate = text[1:-1] if text.startswith("[") and text.endswith("]") else text
        parsed = [item.strip() for item in re.split(r"[,/|]", candidate)]
    if not isinstance(parsed, (list, tuple, set)):
        parsed = [parsed]
    result = []
    for item in parsed:
        allele = clean(item)
        if allele is not None:
            result.append(allele.upper())
    return sorted(set(result))


def variation_type_from_alleles(alleles: list[str]) -> dict[str, Any]:
    """Derive sequence class without assigning REF, ALT, or effect-allele roles."""

    if len(alleles) < 2:
        return _unresolved_variation("fewer than two normalized alleles are available")
    if any(not SEQUENCE_ALLELE.fullmatch(allele) for allele in alleles):
        return _unresolved_variation("symbolic or non-sequence allele is present")
    lengths = {len(allele) for allele in alleles}
    if lengths == {1}:
        value = "SNV"
    elif len(lengths) > 1:
        value = "INDEL"
    else:
        value = "MNV"
    return {
        "value": value,
        "status": "derived",
        "method": "normalized_unordered_source_allele_lengths",
        "reason": "REF, ALT, and effect-allele roles remain unresolved",
    }


def _unresolved_variation(reason: str) -> dict[str, Any]:
    return {
        "value": None,
        "status": "unresolved",
        "method": "normalized_unordered_source_allele_lengths",
        "reason": reason,
    }


def assertion_terms(value: Any) -> list[str]:
    text = clean(value)
    if text is None:
        return []
    try:
        parsed = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        parsed = text
    values = parsed if isinstance(parsed, (list, tuple, set)) else [parsed]
    return sorted({term for item in values if (term := clean(item)) is not None})


def clinical_conflict(values: list[str]) -> bool:
    lowered = {item.lower() for item in values}
    if any("conflicting" in item for item in lowered):
        return True
    pathogenic = any("pathogenic" in item and "benign" not in item for item in lowered)
    benign = any("benign" in item for item in lowered)
    uncertain = any("uncertain" in item for item in lowered)
    return pathogenic and (benign or uncertain)
