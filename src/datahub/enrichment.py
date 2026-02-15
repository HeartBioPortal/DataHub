"""Configurable field enrichment for canonical records."""

from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict

from datahub.config import SourcePriority
from datahub.models import CanonicalRecord


@dataclass(frozen=True)
class EnrichmentCandidate:
    """Candidate value for a record field from a specific source."""

    record_key: tuple[str, str, str, str]
    field_name: str
    source: str
    value: str


class SourcePriorityEnricher:
    """Resolve missing fields using per-field source precedence rules."""

    def __init__(self, priority: SourcePriority):
        self.priority = priority

    def enrich(
        self,
        records: list[CanonicalRecord],
        candidates: list[EnrichmentCandidate] | None = None,
    ) -> list[CanonicalRecord]:
        if not candidates:
            return records

        by_record: dict[tuple[str, str, str, str], list[EnrichmentCandidate]] = defaultdict(list)
        for candidate in candidates:
            by_record[candidate.record_key].append(candidate)

        for record in records:
            grouped = by_record.get(record.key(), [])
            if not grouped:
                continue

            for field_name in {item.field_name for item in grouped}:
                current_value = getattr(record, field_name)
                if current_value not in (None, "", "nan", "NaN"):
                    continue

                resolved = self._resolve(field_name, grouped)
                if resolved is not None:
                    setattr(record, field_name, resolved)

        return records

    def _resolve(self, field_name: str, candidates: list[EnrichmentCandidate]) -> str | None:
        filtered = [c for c in candidates if c.field_name == field_name and c.value not in ("", "nan", "NaN")]
        if not filtered:
            return None

        priorities = self.priority.for_field(field_name)
        if priorities:
            for source in priorities:
                for candidate in filtered:
                    if candidate.source == source:
                        return candidate.value

        return filtered[0].value
