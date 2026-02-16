"""Source manifest definitions for DataHub source onboarding."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


class SourceAccessMode(str, Enum):
    """Primary access pattern for a source dataset."""

    API = "api"
    DOWNLOAD = "download"
    SCRAPE = "scrape"
    HYBRID = "hybrid"


@dataclass(frozen=True)
class SourceManifest:
    """Immutable metadata and defaults for an ingestible source."""

    source_id: str
    display_name: str
    description: str
    adapter_name: str
    dataset_types: tuple[str, ...]
    modalities: tuple[str, ...]
    access_mode: SourceAccessMode
    homepage_url: str | None = None
    api_base_url: str | None = None
    bulk_download_url: str | None = None
    license_name: str | None = None
    update_frequency: str | None = None
    priority_tier: int = 3
    provenance_tags: tuple[str, ...] = ()
    default_params: Mapping[str, Any] = field(default_factory=dict)

    def merged_params(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Return adapter params merged with runtime overrides."""

        merged = dict(self.default_params)
        if overrides:
            merged.update(overrides)
        return merged


class SourceManifestLoader:
    """Load source manifests from ``config/sources`` JSON files."""

    def __init__(self, manifests_dir: str | Path | None = None) -> None:
        if manifests_dir is None:
            manifests_dir = Path(__file__).resolve().parents[3] / "config" / "sources"
        self.manifests_dir = Path(manifests_dir)

    def list_sources(self) -> list[str]:
        """List available source manifest IDs."""

        return sorted(path.stem for path in self.manifests_dir.glob("*.json"))

    def load(self, source_id_or_path: str | Path) -> SourceManifest:
        """Load one source manifest by ID or explicit path."""

        path = self._resolve_path(source_id_or_path)
        payload = json.loads(path.read_text())
        return self._parse(payload)

    def load_all(self) -> dict[str, SourceManifest]:
        """Load all source manifests in the manifest directory."""

        manifests: dict[str, SourceManifest] = {}
        for source_id in self.list_sources():
            manifests[source_id] = self.load(source_id)
        return manifests

    def _resolve_path(self, source_id_or_path: str | Path) -> Path:
        requested = Path(source_id_or_path)

        if requested.exists():
            return requested

        candidate = self.manifests_dir / f"{requested}.json"
        if candidate.exists():
            return candidate

        raise FileNotFoundError(
            f"Source manifest not found: {source_id_or_path}. "
            f"Available: {', '.join(self.list_sources())}"
        )

    def _parse(self, payload: dict[str, Any]) -> SourceManifest:
        source_id = str(payload["source_id"]).strip().lower()
        if not source_id:
            raise ValueError("Manifest source_id cannot be empty")

        adapter_name = str(payload["adapter_name"]).strip().lower()
        if not adapter_name:
            raise ValueError("Manifest adapter_name cannot be empty")

        dataset_types = tuple(
            value.strip().upper()
            for value in payload.get("dataset_types", [])
            if str(value).strip()
        )
        if not dataset_types:
            raise ValueError(f"Manifest {source_id} must define at least one dataset_type")

        modalities = tuple(
            value.strip().lower()
            for value in payload.get("modalities", [])
            if str(value).strip()
        )
        access_mode = SourceAccessMode(str(payload.get("access_mode", "download")).lower())
        priority_tier = int(payload.get("priority_tier", 3))
        if priority_tier < 1:
            raise ValueError(f"Manifest {source_id} priority_tier must be >= 1")

        default_params = payload.get("default_params", {})
        if not isinstance(default_params, dict):
            raise ValueError(f"Manifest {source_id} default_params must be an object")

        provenance_tags = tuple(
            value.strip().lower()
            for value in payload.get("provenance_tags", [])
            if str(value).strip()
        )

        return SourceManifest(
            source_id=source_id,
            display_name=str(payload.get("display_name", source_id)),
            description=str(payload.get("description", "")),
            adapter_name=adapter_name,
            dataset_types=dataset_types,
            modalities=modalities,
            access_mode=access_mode,
            homepage_url=self._clean_optional(payload.get("homepage_url")),
            api_base_url=self._clean_optional(payload.get("api_base_url")),
            bulk_download_url=self._clean_optional(payload.get("bulk_download_url")),
            license_name=self._clean_optional(payload.get("license_name")),
            update_frequency=self._clean_optional(payload.get("update_frequency")),
            priority_tier=priority_tier,
            provenance_tags=provenance_tags,
            default_params=default_params,
        )

    @staticmethod
    def _clean_optional(value: Any) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None
