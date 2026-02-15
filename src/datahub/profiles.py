"""First-class dataset profile loader for DataHub contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datahub.config import DatasetContract, FieldPolicy, MissingFieldStrategy


@dataclass(frozen=True)
class DatasetProfile:
    """Serializable profile that defines validation policy by dataset type."""

    name: str
    dataset_type: str
    description: str
    required_fields: tuple[str, ...]
    field_policies: dict[str, FieldPolicy]

    def to_contract(self) -> DatasetContract:
        """Convert profile into a runtime validation contract."""

        return DatasetContract(dataset_type=self.dataset_type, field_policies=self.field_policies)


class DatasetProfileLoader:
    """Load profile JSON from ``config/profiles`` or a custom path."""

    def __init__(self, profiles_dir: str | Path | None = None) -> None:
        if profiles_dir is None:
            profiles_dir = Path(__file__).resolve().parents[2] / "config" / "profiles"
        self.profiles_dir = Path(profiles_dir)

    def list_profiles(self) -> list[str]:
        """Return available profile names from the configured profile directory."""

        return sorted(path.stem for path in self.profiles_dir.glob("*.json"))

    def load(self, name_or_path: str | Path) -> DatasetProfile:
        """Load a profile by name (for example, ``association``) or explicit path."""

        path = self._resolve_path(name_or_path)
        payload = json.loads(path.read_text())
        return self._parse(payload)

    def _resolve_path(self, name_or_path: str | Path) -> Path:
        requested = Path(name_or_path)

        if requested.exists():
            return requested

        candidate = self.profiles_dir / f"{requested}.json"
        if candidate.exists():
            return candidate

        raise FileNotFoundError(
            f"Profile not found: {name_or_path}. Available: {', '.join(self.list_profiles())}"
        )

    def _parse(self, payload: dict[str, Any]) -> DatasetProfile:
        field_policies: dict[str, FieldPolicy] = {}
        for field_name, raw_policy in payload.get("field_policies", {}).items():
            missing_strategy = MissingFieldStrategy(raw_policy.get("missing_strategy", "exclude"))
            field_policies[field_name] = FieldPolicy(
                required=bool(raw_policy.get("required", False)),
                missing_strategy=missing_strategy,
                unknown_value=raw_policy.get("unknown_value", "Unknown"),
            )

        required_fields = tuple(payload.get("required_fields", ()))
        for field_name in required_fields:
            if field_name not in field_policies:
                field_policies[field_name] = FieldPolicy(required=True)

        return DatasetProfile(
            name=str(payload["name"]),
            dataset_type=str(payload["dataset_type"]).upper(),
            description=str(payload.get("description", "")),
            required_fields=required_fields,
            field_policies=field_policies,
        )
