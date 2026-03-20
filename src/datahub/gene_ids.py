"""Gene identifier validation helpers."""

from __future__ import annotations

import re

SQL_GENE_ID_HAS_LETTER_PATTERN = r"[A-Za-z]"

_NULL_LIKE = {"", "nan", "none", "null"}


def is_valid_gene_id(value: object) -> bool:
    """Return ``True`` when a gene identifier is plausible for publication.

    The current HBP publish contract expects gene-like identifiers, not numeric
    payloads that leaked in from malformed rows or misaligned columns. The rule
    is intentionally permissive: any non-null token containing at least one
    ASCII letter is accepted.
    """

    if value is None:
        return False

    text = str(value).strip()
    if not text or text.lower() in _NULL_LIKE:
        return False

    return bool(re.search(SQL_GENE_ID_HAS_LETTER_PATTERN, text))


def sql_has_gene_like_identifier(expression: str) -> str:
    """Return a DuckDB SQL predicate for a plausible gene identifier."""

    return (
        f"regexp_matches(trim(coalesce({expression}, '')), "
        f"'{SQL_GENE_ID_HAS_LETTER_PATTERN}')"
    )
