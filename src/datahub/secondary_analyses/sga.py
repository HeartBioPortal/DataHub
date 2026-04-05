"""Shared genetic architecture secondary-analysis generation."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from datahub.gene_ids import sql_has_gene_like_identifier

from .artifacts import write_gene_payload_artifact, write_metadata
from .base import SecondaryAnalysisManifest, SecondaryArtifactRow


def _identity_interval(variant_id: str) -> list[float]:
    token = hashlib.sha1(str(variant_id).encode("utf-8")).hexdigest()[:12]
    value = float(int(token, 16))
    return [value, value]


def _normalize_variant_payload(variant_ids: set[str]) -> dict[str, list[float]]:
    return {
        variant_id: _identity_interval(variant_id)
        for variant_id in sorted(variant_ids)
    }


def _slug_sql(column_sql: str) -> str:
    return (
        f"lower(regexp_replace(replace(trim(coalesce({column_sql}, '')), '/', '_'), '\\\\s+', '_', 'g'))"
    )


def _load_gene_variant_sets(
    connection: Any,
    *,
    source_table: str,
    include_genes: set[str] | None,
) -> dict[str, dict[str, dict[str, set[str]]]]:
    gene_filter_sql = ""
    params: list[str] = []
    if include_genes:
        placeholders = ",".join("?" for _ in sorted(include_genes))
        gene_filter_sql = f" AND upper(trim(gene_id)) IN ({placeholders})"
        params.extend(sorted(include_genes))

    rows = connection.execute(
        f"""
WITH cleaned AS (
    SELECT
        upper(trim(dataset_type)) AS dataset_type,
        trim(gene_id) AS gene_id,
        trim(variant_id) AS variant_id,
        {_slug_sql("phenotype")} AS phenotype
    FROM {source_table}
    WHERE coalesce(trim(dataset_type), '') <> ''
      AND coalesce(trim(gene_id), '') <> ''
      AND {sql_has_gene_like_identifier("gene_id")}
      AND coalesce(trim(variant_id), '') <> ''
      AND coalesce(trim(phenotype), '') <> ''
      AND upper(trim(dataset_type)) IN ('CVD', 'TRAIT')
      {gene_filter_sql}
),
deduped AS (
    SELECT DISTINCT
        dataset_type,
        gene_id,
        variant_id,
        phenotype
    FROM cleaned
)
SELECT dataset_type, gene_id, phenotype, variant_id
FROM deduped
ORDER BY gene_id, dataset_type, phenotype, variant_id
""",
        params,
    ).fetchall()

    by_gene: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: {"CVD": defaultdict(set), "TRAIT": defaultdict(set)}
    )
    for dataset_type, gene_id, phenotype, variant_id in rows:
        by_gene[str(gene_id)][str(dataset_type)][str(phenotype)].add(str(variant_id))
    return by_gene


def _build_gene_payload(gene_id: str, grouped: dict[str, dict[str, set[str]]]) -> list[dict[str, Any]]:
    shared_by_key: dict[tuple[str, str], set[str]] = defaultdict(set)
    cvd_sets = grouped.get("CVD", {})
    trait_sets = grouped.get("TRAIT", {})

    for cvd_name, cvd_variants in cvd_sets.items():
        if not cvd_variants:
            continue
        for trait_name, trait_variants in trait_sets.items():
            if not trait_variants:
                continue
            shared = cvd_variants & trait_variants
            if not shared:
                continue
            shared_by_key[("cvd", cvd_name)].update(shared)
            shared_by_key[("trait", trait_name)].update(shared)

    payload: list[dict[str, Any]] = []
    for item_type, item_name in sorted(shared_by_key.keys(), key=lambda item: (item[0], item[1])):
        payload.append(
            {
                "gene": gene_id,
                "type": item_type,
                "name": item_name,
                "data": _normalize_variant_payload(shared_by_key[(item_type, item_name)]),
                "_datahub": {
                    "analysis_id": "sga",
                    "encoding": "rsid_identity_interval",
                    "shared_variant_count": len(shared_by_key[(item_type, item_name)]),
                },
            }
        )
    return payload


def generate_sga_artifacts(
    *,
    db_path: str | Path,
    source_table: str,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    include_genes: set[str] | None = None,
) -> list[SecondaryArtifactRow]:
    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("duckdb is required to derive SGA artifacts") from exc

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        by_gene = _load_gene_variant_sets(
            connection,
            source_table=source_table,
            include_genes=include_genes,
        )
    finally:
        connection.close()

    rows: list[SecondaryArtifactRow] = []
    for gene_id in sorted(by_gene.keys(), key=lambda item: item.upper()):
        payload = _build_gene_payload(gene_id, by_gene[gene_id])
        if not payload:
            continue
        payload_json = json.dumps(payload, separators=(",", ":"))
        artifact_path = write_gene_payload_artifact(
            output_root=output_root,
            manifest=manifest,
            gene_id=gene_id,
            payload_json=payload_json,
        )
        rows.append(
            SecondaryArtifactRow(
                gene_id=gene_id,
                gene_id_normalized=gene_id.upper(),
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
            "source_db_path": str(Path(db_path)),
            "source_table": source_table,
            "row_count": len(rows),
            "filtered_gene_count": 0 if include_genes is None else len(include_genes),
            "semantics": "shared rsid identity across CVD/TRAIT phenotype pairs",
            "encoding": "rsid_identity_interval",
        },
    )
    return rows
