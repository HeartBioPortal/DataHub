"""Core models for secondary analyses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SecondaryAnalysisManifest:
    """Declarative definition for one secondary analysis."""

    analysis_id: str
    version: int
    mode: str
    description: str
    artifact_subdir: str


@dataclass(frozen=True)
class SecondaryArtifactRow:
    """One per-gene serving payload row emitted by a secondary analysis."""

    gene_id: str
    gene_id_normalized: str
    payload_json: str
    source_path: str

    @property
    def source_path_obj(self) -> Path:
        return Path(self.source_path)
