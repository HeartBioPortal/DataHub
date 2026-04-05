"""Registry for secondary analysis manifests."""

from __future__ import annotations

import json
from pathlib import Path

from .base import SecondaryAnalysisManifest


class SecondaryAnalysisRegistry:
    """Load and validate secondary-analysis manifests from config."""

    def __init__(self, manifests_dir: str | Path | None = None) -> None:
        if manifests_dir is None:
            manifests_dir = (
                Path(__file__).resolve().parents[3] / "config" / "secondary_analyses"
            )
        self.manifests_dir = Path(manifests_dir)

    def list_ids(self) -> list[str]:
        return sorted(path.stem for path in self.manifests_dir.glob("*.json"))

    def load(self, analysis_id: str) -> SecondaryAnalysisManifest:
        path = self.manifests_dir / f"{analysis_id}.json"
        if not path.exists():
            raise FileNotFoundError(
                f"Secondary analysis manifest not found: {analysis_id}. "
                f"Available: {', '.join(self.list_ids())}"
            )

        payload = json.loads(path.read_text())
        manifest = SecondaryAnalysisManifest(
            analysis_id=str(payload["analysis_id"]).strip(),
            version=int(payload["version"]),
            mode=str(payload["mode"]).strip(),
            description=str(payload.get("description", "")).strip(),
            artifact_subdir=str(payload["artifact_subdir"]).strip(),
        )
        self._validate(manifest)
        return manifest

    @staticmethod
    def _validate(manifest: SecondaryAnalysisManifest) -> None:
        if not manifest.analysis_id:
            raise ValueError("Secondary analysis manifest must define analysis_id")
        if manifest.mode not in {"imported", "derived"}:
            raise ValueError(
                f"Secondary analysis {manifest.analysis_id} has unsupported mode: {manifest.mode}"
            )
        if not manifest.artifact_subdir:
            raise ValueError(
                f"Secondary analysis {manifest.analysis_id} must define artifact_subdir"
            )
