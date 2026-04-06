"""Shared genetic architecture secondary-analysis generation."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from datahub.gene_ids import sql_has_gene_like_identifier

from .artifacts import write_gene_payload_artifact, write_metadata
from .base import SecondaryAnalysisManifest


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


def _stream_gene_variant_sets(
    connection: Any,
    *,
    source_table: str,
    include_genes: set[str] | None,
    unit_partitions: int = 1,
    unit_partition_index: int = 0,
    chunk_size: int = 100_000,
) -> Iterable[tuple[str, dict[str, dict[str, set[str]]]]]:
    gene_filter_sql = ""
    params: list[str] = []
    if include_genes:
        placeholders = ",".join("?" for _ in sorted(include_genes))
        gene_filter_sql = f" AND upper(trim(gene_id)) IN ({placeholders})"
        params.extend(sorted(include_genes))

    partitions = max(int(unit_partitions), 1)
    partition_index = int(unit_partition_index)
    if partition_index < 0 or partition_index >= partitions:
        raise ValueError(
            f"unit_partition_index must be in [0, {partitions - 1}], got {partition_index}"
        )
    partition_filter_sql = ""
    if partitions > 1:
        partition_filter_sql = (
            f" AND (hash(upper(trim(gene_id))) % {partitions}) = {partition_index}"
        )

    cursor = connection.execute(
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
      {partition_filter_sql}
),
eligible_genes AS (
    SELECT gene_id
    FROM cleaned
    GROUP BY gene_id
    HAVING count(distinct dataset_type) = 2
),
deduped AS (
    SELECT DISTINCT
        c.dataset_type,
        c.gene_id,
        c.variant_id,
        c.phenotype
    FROM cleaned c
    INNER JOIN eligible_genes g
      ON c.gene_id = g.gene_id
)
SELECT dataset_type, gene_id, phenotype, variant_id
FROM deduped
ORDER BY gene_id, dataset_type, phenotype, variant_id
""",
        params,
    )

    current_gene: str | None = None
    current_grouped: dict[str, dict[str, set[str]]] = {
        "CVD": defaultdict(set),
        "TRAIT": defaultdict(set),
    }

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        for dataset_type, gene_id, phenotype, variant_id in rows:
            normalized_gene = str(gene_id)
            if current_gene is None:
                current_gene = normalized_gene
            if normalized_gene != current_gene:
                yield current_gene, current_grouped
                current_gene = normalized_gene
                current_grouped = {
                    "CVD": defaultdict(set),
                    "TRAIT": defaultdict(set),
                }
            current_grouped[str(dataset_type)][str(phenotype)].add(str(variant_id))

    if current_gene is not None:
        yield current_gene, current_grouped


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
    unit_partitions: int = 1,
    unit_partition_index: int = 0,
) -> int:
    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("duckdb is required to derive SGA artifacts") from exc

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        row_count = 0
        for gene_id, grouped in _stream_gene_variant_sets(
            connection,
            source_table=source_table,
            include_genes=include_genes,
            unit_partitions=unit_partitions,
            unit_partition_index=unit_partition_index,
        ):
            payload = _build_gene_payload(gene_id, grouped)
            if not payload:
                continue
            payload_json = json.dumps(payload, separators=(",", ":"))
            write_gene_payload_artifact(
                output_root=output_root,
                manifest=manifest,
                gene_id=gene_id,
                payload_json=payload_json,
            )
            row_count += 1
    finally:
        connection.close()

    metadata_filename = "metadata.json"
    if int(unit_partitions) > 1:
        width = max(3, len(str(int(unit_partitions) - 1)))
        metadata_filename = (
            f"metadata.part{int(unit_partition_index):0{width}d}"
            f"of{int(unit_partitions):0{width}d}.json"
        )

    write_metadata(
        output_root=output_root,
        manifest=manifest,
        filename=metadata_filename,
        payload={
            "analysis_id": manifest.analysis_id,
            "version": manifest.version,
            "mode": manifest.mode,
            "source_db_path": str(Path(db_path)),
            "source_table": source_table,
            "row_count": row_count,
            "filtered_gene_count": 0 if include_genes is None else len(include_genes),
            "unit_partitions": int(unit_partitions),
            "unit_partition_index": int(unit_partition_index),
            "semantics": "shared rsid identity across CVD/TRAIT phenotype pairs",
            "encoding": "rsid_identity_interval",
        },
    )
    return row_count
