"""Expression secondary-analysis generation."""

from __future__ import annotations

import json
import math
import gzip
from pathlib import Path
from urllib.parse import quote

from .artifacts import write_gene_payload_artifact, write_metadata
from .base import SecondaryAnalysisManifest, SecondaryArtifactRow


MAX_EXPRESSION_GENE_FILENAME_LENGTH = 180


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

    if any(key in value for key in ("up", "down", "upregulated", "downregulated")):
        up = value.get("up", value.get("upregulated", 0))
        down = value.get("down", value.get("downregulated", 0))
        normalized_value = dict(value)
        normalized_value["up"] = 0 if _is_nan(up) else up
        normalized_value["down"] = 0 if _is_nan(down) else down
        normalized_value.setdefault("upregulated", normalized_value["up"])
        normalized_value.setdefault("downregulated", normalized_value["down"])
        return normalized_value

    normalized: dict[str, object] = {}
    for disease, regulation in value.items():
        if not isinstance(regulation, dict):
            normalized[str(disease)] = regulation
            continue
        normalized[str(disease)] = normalize_expression_entry(regulation)
    return normalized


def is_supported_expression_gene_id(gene_id: object) -> bool:
    gene = str(gene_id or "").strip()
    if not gene:
        return False
    if "," in gene or "///" in gene:
        return False
    encoded_filename = f"{quote(gene, safe='')}.json.gz"
    return len(encoded_filename) <= MAX_EXPRESSION_GENE_FILENAME_LENGTH


def generate_expression_artifacts(
    *,
    expression_json_path: str | Path,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    include_genes: set[str] | None = None,
) -> list[SecondaryArtifactRow]:
    expression_path = Path(expression_json_path)
    if expression_path.suffix == ".gz":
        with gzip.open(expression_path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    else:
        payload = json.loads(expression_path.read_text())
    rows: list[SecondaryArtifactRow] = []
    skipped_gene_count = 0

    for gene_id, value in payload.items():
        gene = str(gene_id)
        normalized_gene = gene.upper()
        if not is_supported_expression_gene_id(gene):
            skipped_gene_count += 1
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
            "skipped_gene_count": skipped_gene_count,
            "skip_reason": "Empty, comma-delimited, CardioQuilt multi-gene, or filename-unsafe legacy identifiers are not emitted as per-gene expression artifacts.",
        },
    )
    return rows
