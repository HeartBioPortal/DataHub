"""Helpers for reading and writing secondary-analysis artifacts."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

from .base import SecondaryArtifactRow, SecondaryAnalysisManifest


def artifact_root(output_root: str | Path, manifest: SecondaryAnalysisManifest) -> Path:
    return Path(output_root) / "final" / manifest.artifact_subdir / "genes"


def metadata_path(output_root: str | Path, manifest: SecondaryAnalysisManifest) -> Path:
    return Path(output_root) / "final" / manifest.artifact_subdir / "metadata.json"


def write_gene_payload_artifact(
    *,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    gene_id: str,
    payload_json: str,
) -> Path:
    root = artifact_root(output_root, manifest)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{gene_id}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        stream.write(payload_json)
    return path


def write_metadata(
    *,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    payload: dict[str, object],
) -> Path:
    path = metadata_path(output_root, manifest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path


def read_gene_payload_artifacts(
    *,
    input_root: str | Path,
    manifest: SecondaryAnalysisManifest,
) -> list[SecondaryArtifactRow]:
    root = artifact_root(input_root, manifest)
    if not root.exists():
        return []

    rows: list[SecondaryArtifactRow] = []
    for payload_path in sorted(root.glob("*.json.gz")):
        gene_id = payload_path.name[:-8]
        with gzip.open(payload_path, "rt", encoding="utf-8") as stream:
            payload_json = stream.read()
        rows.append(
            SecondaryArtifactRow(
                gene_id=gene_id,
                gene_id_normalized=gene_id.upper(),
                payload_json=payload_json,
                source_path=str(payload_path),
            )
        )
    return rows
