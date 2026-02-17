"""Profile loading for raw association preparation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RawAssociationPreparationProfile:
    """How to map raw source columns into standardized prepared association rows."""

    name: str
    dataset_type: str
    source_name: str
    field_candidates: dict[str, tuple[str, ...]]
    ancestry_fields: dict[str, str] = field(default_factory=dict)
    defaults: dict[str, Any] = field(default_factory=dict)

    def candidates_for(self, field_name: str) -> tuple[str, ...]:
        """Return ordered source-column candidates for a canonical field."""

        return self.field_candidates.get(field_name, ())


class RawAssociationPreparationProfileLoader:
    """Load preparation profiles from ``config/prep_profiles``."""

    def __init__(self, profiles_dir: str | Path | None = None) -> None:
        if profiles_dir is None:
            profiles_dir = Path(__file__).resolve().parents[3] / "config" / "prep_profiles"
        self.profiles_dir = Path(profiles_dir)

    def list_profiles(self) -> list[str]:
        """List available preparation profiles."""

        return sorted(path.stem for path in self.profiles_dir.glob("*.json"))

    def load(self, profile_name_or_path: str | Path) -> RawAssociationPreparationProfile:
        """Load profile by name or explicit JSON path."""

        path = self._resolve_path(profile_name_or_path)
        payload = json.loads(path.read_text())
        return self._parse(payload)

    def _resolve_path(self, profile_name_or_path: str | Path) -> Path:
        requested = Path(profile_name_or_path)
        if requested.exists():
            return requested

        candidate = self.profiles_dir / f"{requested}.json"
        if candidate.exists():
            return candidate

        raise FileNotFoundError(
            f"Preparation profile not found: {profile_name_or_path}. "
            f"Available: {', '.join(self.list_profiles())}"
        )

    def _parse(self, payload: dict[str, Any]) -> RawAssociationPreparationProfile:
        field_candidates: dict[str, tuple[str, ...]] = {}
        for field_name, candidates in payload.get("field_candidates", {}).items():
            field_candidates[field_name] = tuple(
                str(column).strip()
                for column in candidates
                if str(column).strip()
            )

        ancestry_fields = {
            str(source_col).strip(): str(target_label).strip()
            for source_col, target_label in payload.get("ancestry_fields", {}).items()
            if str(source_col).strip() and str(target_label).strip()
        }

        return RawAssociationPreparationProfile(
            name=str(payload["name"]),
            dataset_type=str(payload.get("dataset_type", "ASSOCIATION")).upper(),
            source_name=str(payload.get("source_name", payload.get("name", "unknown"))),
            field_candidates=field_candidates,
            ancestry_fields=ancestry_fields,
            defaults=dict(payload.get("defaults", {})),
        )
