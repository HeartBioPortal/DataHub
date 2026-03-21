"""Load declarative output-contract definitions for published artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OutputContract:
    """Serializable output contract definition loaded from JSON."""

    name: str
    description: str
    payload: dict[str, Any]


class OutputContractLoader:
    """Load output contracts from ``config/output_contracts`` or a custom path."""

    def __init__(self, contracts_dir: str | Path | None = None) -> None:
        if contracts_dir is None:
            contracts_dir = Path(__file__).resolve().parents[2] / "config" / "output_contracts"
        self.contracts_dir = Path(contracts_dir)

    def list_contracts(self) -> list[str]:
        return sorted(path.stem for path in self.contracts_dir.glob("*.json"))

    def load(self, name_or_path: str | Path) -> OutputContract:
        path = self._resolve_path(name_or_path)
        payload = json.loads(path.read_text())
        return OutputContract(
            name=str(payload["name"]),
            description=str(payload.get("description", "")),
            payload=dict(payload),
        )

    def _resolve_path(self, name_or_path: str | Path) -> Path:
        requested = Path(name_or_path)
        if requested.exists():
            return requested
        candidate = self.contracts_dir / f"{requested}.json"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(
            f"Output contract not found: {name_or_path}. Available: {', '.join(self.list_contracts())}"
        )
