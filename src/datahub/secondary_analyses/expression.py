"""Expression secondary-analysis generation."""

from __future__ import annotations

import json
import math
from pathlib import Path

from .artifacts import write_gene_payload_artifact, write_metadata
from .base import SecondaryAnalysisManifest, SecondaryArtifactRow


def _is_nan(value: object) -> bool:
    return isinstance(value, float) and math.isnan(value)


def normalize_expression_entry(value: object) -> object:
    if not isinstance(value, dict):
        return value

    if "regulation" in value and isinstance(value.get("regulation"), dict):
        regulation = value["regulation"]
        return {
            "up": 0 if _is_nan(regulation.get("upregulated", 0)) else regulation.get("upregulated", 0),
            "down": 0 if _is_nan(regulation.get("downregulated", 0)) else regulation.get("downregulated", 0),
        }

    normalized: dict[str, object] = {}
    for disease, regulation in value.items():
        if not isinstance(regulation, dict):
            normalized[str(disease)] = regulation
            continue
        up = regulation.get("upregulated", 0)
        down = regulation.get("downregulated", 0)
        normalized[str(disease)] = {
            "up": 0 if _is_nan(up) else up,
            "down": 0 if _is_nan(down) else down,
        }
    return normalized


def generate_expression_artifacts(
    *,
    expression_json_path: str | Path,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    include_genes: set[str] | None = None,
) -> list[SecondaryArtifactRow]:
    expression_path = Path(expression_json_path)
    payload = json.loads(expression_path.read_text())
    rows: list[SecondaryArtifactRow] = []

    for gene_id, value in payload.items():
        gene = str(gene_id)
        normalized_gene = gene.upper()
        if "," in gene:
            continue
        if include_genes is not None and normalized_gene not in include_genes:
            continue
        payload_json = json.dumps(normalize_expression_entry(value), separators=(",", ":"))
        artifact_path = write_gene_payload_artifact(
            output_root=output_root,
            manifest=manifest,
            gene_id=gene,
            payload_json=payload_json,
        )
        rows.append(
            SecondaryArtifactRow(
                gene_id=gene,
                gene_id_normalized=normalized_gene,
                payload_json=payload_json,
                source_path=str(artifact_path),
            )
        )

    write_metadata(
        output_root=output_root,
        manifest=manifest,
        payload={
            "analysis_id": manifest.analysis_id,
            "version": manifest.version,
            "mode": manifest.mode,
            "source_path": str(expression_path),
            "row_count": len(rows),
            "filtered_gene_count": 0 if include_genes is None else len(include_genes),
        },
    )
    return rows
