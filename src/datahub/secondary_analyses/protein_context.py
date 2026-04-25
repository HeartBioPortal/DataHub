"""Protein-context secondary-analysis generation for the splicing viewer."""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from datahub.apis import (
    EbiProteinsApiClient,
    EnsemblRestClient,
    InterProApiClient,
    JsonFileApiCache,
)
from datahub.protein_context import (
    EbiProteinsFeatureClient,
    EnsemblProteinContextClient,
    InterProFeatureClient,
    IsoformHint,
    build_protein_context_payload,
)

from .artifacts import write_gene_payload_artifact, write_metadata
from .base import SecondaryAnalysisManifest, SecondaryArtifactRow


logger = logging.getLogger(__name__)


def _safe_gene_from_path(path: Path) -> str:
    return path.name.removesuffix(".csv").removesuffix(".tsv").strip().upper()


def _selected_by_partition(
    index: int,
    *,
    unit_partitions: int,
    unit_partition_index: int,
) -> bool:
    if unit_partitions <= 1:
        return True
    return index % unit_partitions == unit_partition_index


def _variant_viewer_overall_dir(variant_viewer_root: str | Path | None) -> Path | None:
    if not variant_viewer_root:
        return None
    root = Path(variant_viewer_root)
    if root.name == "overall":
        return root
    return root / "overall"


def collect_variant_viewer_genes(variant_viewer_root: str | Path | None) -> set[str]:
    """Collect gene symbols from a legacy/DataHub variant-viewer artifact root."""

    overall_dir = _variant_viewer_overall_dir(variant_viewer_root)
    if overall_dir is None or not overall_dir.exists():
        return set()
    return {
        _safe_gene_from_path(path)
        for path in overall_dir.glob("*.csv")
        if _safe_gene_from_path(path)
    }


def read_variant_viewer_isoform_hints(
    *,
    variant_viewer_root: str | Path | None,
    gene: str,
) -> list[IsoformHint]:
    """Infer isoform IDs and protein lengths from current splicing-viewer CSVs."""

    overall_dir = _variant_viewer_overall_dir(variant_viewer_root)
    if overall_dir is None:
        return []
    path = overall_dir / f"{gene.upper()}.csv"
    if not path.exists():
        path = overall_dir / f"{gene}.csv"
    if not path.exists():
        return []

    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"lengths": [], "row_count": 0})
    with path.open("rt", encoding="utf-8", newline="") as stream:
        sample = stream.read(4096)
        stream.seek(0)
        first_line = sample.splitlines()[0] if sample.splitlines() else ""
        delimiter = "\t" if "\t" in first_line else ","
        reader = csv.DictReader(stream, delimiter=delimiter)
        for row in reader:
            identifier = str(row.get("protein_id") or "").strip()
            if not identifier:
                continue
            grouped[identifier]["row_count"] += 1
            try:
                length = int(float(row.get("max") or row.get("protein_length") or 0))
            except (TypeError, ValueError):
                length = 0
            if length > 0:
                grouped[identifier]["lengths"].append(length)

    hints: list[IsoformHint] = []
    for identifier, values in sorted(grouped.items(), key=lambda item: (-item[1]["row_count"], item[0])):
        lengths = values["lengths"]
        hints.append(
            IsoformHint(
                identifier=identifier,
                length_aa=max(lengths) if lengths else None,
                row_count=int(values["row_count"]),
            )
        )
    return hints


def _resolve_gene_list(
    *,
    include_genes: set[str] | None,
    variant_viewer_root: str | Path | None,
) -> list[str]:
    if include_genes:
        return sorted({gene.strip().upper() for gene in include_genes if gene.strip()})
    return sorted(collect_variant_viewer_genes(variant_viewer_root))


def generate_protein_context_artifacts(
    *,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    include_genes: set[str] | None = None,
    variant_viewer_root: str | Path | None = None,
    species: str = "homo_sapiens",
    cache_path: str | Path | None = None,
    timeout_seconds: float = 30.0,
    sleep_seconds: float = 0.1,
    max_retries: int = 6,
    retry_backoff_seconds: float = 1.0,
    max_isoforms: int = 12,
    use_ebi_proteins: bool = True,
    use_interpro: bool = True,
    unit_partitions: int = 1,
    unit_partition_index: int = 0,
    limit: int | None = None,
    progress_every: int = 25,
    ensembl_client: EnsemblProteinContextClient | None = None,
    proteins_client: EbiProteinsFeatureClient | None = None,
    interpro_client: InterProFeatureClient | None = None,
) -> list[SecondaryArtifactRow]:
    """Generate per-gene protein-context artifacts."""

    unit_partitions = max(int(unit_partitions), 1)
    unit_partition_index = int(unit_partition_index)
    if unit_partition_index < 0 or unit_partition_index >= unit_partitions:
        raise ValueError("unit_partition_index must be between 0 and unit_partitions - 1")

    genes = _resolve_gene_list(
        include_genes=include_genes,
        variant_viewer_root=variant_viewer_root,
    )
    selected_genes = [
        gene
        for index, gene in enumerate(genes)
        if _selected_by_partition(
            index,
            unit_partitions=unit_partitions,
            unit_partition_index=unit_partition_index,
        )
    ]
    if limit is not None:
        selected_genes = selected_genes[: max(int(limit), 0)]

    owns_clients = ensembl_client is None
    if ensembl_client is None:
        cache = JsonFileApiCache(cache_path)
        ensembl_client = EnsemblRestClient(
            cache=cache,
            timeout_seconds=timeout_seconds,
            sleep_seconds=sleep_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        proteins_client = (
            EbiProteinsApiClient(
                cache=cache,
                timeout_seconds=timeout_seconds,
                sleep_seconds=sleep_seconds,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            if use_ebi_proteins
            else None
        )
        interpro_client = (
            InterProApiClient(
                cache=cache,
                timeout_seconds=timeout_seconds,
                sleep_seconds=sleep_seconds,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
            if use_interpro
            else None
        )

    rows: list[SecondaryArtifactRow] = []
    reports: list[dict[str, Any]] = []
    progress_every = max(int(progress_every), 1)
    try:
        for index, gene in enumerate(selected_genes, start=1):
            hints = read_variant_viewer_isoform_hints(
                variant_viewer_root=variant_viewer_root,
                gene=gene,
            )
            payload, report = build_protein_context_payload(
                gene,
                ensembl_client=ensembl_client,
                proteins_client=proteins_client if use_ebi_proteins else None,
                interpro_client=interpro_client if use_interpro else None,
                isoform_hints=hints,
                species=species,
                max_isoforms=max_isoforms,
            )
            payload_json = json.dumps(payload, separators=(",", ":"))
            artifact_path = write_gene_payload_artifact(
                output_root=output_root,
                manifest=manifest,
                gene_id=gene,
                payload_json=payload_json,
            )
            rows.append(
                SecondaryArtifactRow(
                    gene_id=gene,
                    gene_id_normalized=gene.upper(),
                    payload_json=payload_json,
                    source_path=str(artifact_path),
                )
            )
            reports.append(report.to_dict())
            if index % progress_every == 0:
                logger.info(
                    "Protein context progress partition=%d/%d processed=%d/%d artifacts=%d",
                    unit_partition_index + 1,
                    unit_partitions,
                    index,
                    len(selected_genes),
                    len(rows),
                )
    finally:
        if owns_clients:
            for client in (interpro_client, proteins_client, ensembl_client):
                close = getattr(client, "close", None)
                if callable(close):
                    close()

    metadata_filename = "metadata.json"
    if unit_partitions > 1:
        width = max(3, len(str(unit_partitions - 1)))
        metadata_filename = (
            f"metadata.part{unit_partition_index:0{width}d}"
            f"of{unit_partitions:0{width}d}.json"
        )

    write_metadata(
        output_root=output_root,
        manifest=manifest,
        filename=metadata_filename,
        payload={
            "analysis_id": manifest.analysis_id,
            "version": manifest.version,
            "mode": manifest.mode,
            "row_count": len(rows),
            "candidate_gene_count": len(genes),
            "selected_gene_count": len(selected_genes),
            "filtered_gene_count": 0 if include_genes is None else len(include_genes),
            "variant_viewer_root": str(variant_viewer_root or ""),
            "species": species,
            "unit_partitions": unit_partitions,
            "unit_partition_index": unit_partition_index,
            "max_isoforms": int(max_isoforms),
            "use_ebi_proteins": bool(use_ebi_proteins),
            "use_interpro": bool(use_interpro),
            "reports": reports,
        },
    )
    return rows
