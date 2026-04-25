"""Protein-context secondary-analysis generation for the splicing viewer."""

from __future__ import annotations

import csv
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from datahub.artifact_io import open_text_artifact
from datahub.apis import (
    EbiProteinsApiClient,
    EnsemblRestClient,
    InterProApiClient,
    JsonFileApiCache,
)
from datahub.gene_ids import is_valid_gene_id
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
    name = path.name
    for suffix in (".gz", ".zip"):
        if name.lower().endswith(suffix):
            name = name[: -len(suffix)]
    for suffix in (".csv", ".tsv", ".json"):
        if name.lower().endswith(suffix):
            name = name[: -len(suffix)]
    return name.strip().upper()


def _is_variant_viewer_table(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith((".csv", ".csv.gz", ".csv.zip", ".tsv", ".tsv.gz", ".tsv.zip"))


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
        for path in overall_dir.iterdir()
        if path.is_file() and _is_variant_viewer_table(path)
        if is_valid_gene_id(_safe_gene_from_path(path))
    }


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def collect_association_db_genes(
    db_path: str | Path | None,
    *,
    source_table: str = "mvp_association_points",
) -> set[str]:
    """Collect candidate gene symbols from a DataHub serving or association DuckDB."""

    if not db_path:
        return set()
    path = Path(db_path)
    if not path.exists():
        return set()

    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError("duckdb is required to infer protein_context genes from DuckDB") from exc

    table_priority = [
        "gene_catalog",
        "association_gene_payloads",
        "overall_gene_payloads",
        source_table,
    ]
    column_priority = [
        "gene_id_normalized",
        "gene_id",
        "gene_symbol",
        "symbol",
        "hgnc_symbol",
    ]

    connection = duckdb.connect(str(path), read_only=True)
    try:
        available_tables = {
            str(row[0])
            for row in connection.execute("SHOW TABLES").fetchall()
        }
        for table in table_priority:
            if table not in available_tables:
                continue
            columns = {
                str(row[0])
                for row in connection.execute(
                    """
SELECT column_name
FROM information_schema.columns
WHERE table_name = ?
""",
                    [table],
                ).fetchall()
            }
            gene_column = next((column for column in column_priority if column in columns), None)
            if gene_column is None:
                continue

            genes: set[str] = set()
            for (raw_gene,) in connection.execute(
                f"""
SELECT DISTINCT trim(cast({_quote_identifier(gene_column)} AS VARCHAR)) AS gene
FROM {_quote_identifier(table)}
WHERE coalesce(trim(cast({_quote_identifier(gene_column)} AS VARCHAR)), '') <> ''
"""
            ).fetchall():
                gene = str(raw_gene or "").strip().upper()
                if is_valid_gene_id(gene):
                    genes.add(gene)
            if genes:
                logger.info(
                    "Protein context gene source: db=%s table=%s column=%s genes=%d",
                    path,
                    table,
                    gene_column,
                    len(genes),
                )
                return genes
    finally:
        connection.close()

    return set()


def _variant_viewer_diagnostic(variant_viewer_root: str | Path | None) -> dict[str, Any]:
    overall_dir = _variant_viewer_overall_dir(variant_viewer_root)
    if overall_dir is None:
        return {"variant_viewer_root": "", "overall_dir": "", "overall_dir_exists": False}

    files: list[str] = []
    if overall_dir.exists():
        files = sorted(path.name for path in overall_dir.iterdir() if path.is_file())[:10]
    return {
        "variant_viewer_root": str(variant_viewer_root or ""),
        "overall_dir": str(overall_dir),
        "overall_dir_exists": overall_dir.exists(),
        "sample_files": files,
    }


def _resolve_variant_viewer_gene_file(overall_dir: Path, gene: str) -> Path | None:
    candidates: list[Path] = []
    for stem in (gene.upper(), gene):
        candidates.extend(
            overall_dir / f"{stem}{suffix}"
            for suffix in (".csv", ".csv.gz", ".csv.zip", ".tsv", ".tsv.gz", ".tsv.zip")
        )
    return next((path for path in candidates if path.exists()), None)


def read_variant_viewer_isoform_hints(
    *,
    variant_viewer_root: str | Path | None,
    gene: str,
) -> list[IsoformHint]:
    """Infer isoform IDs and protein lengths from current splicing-viewer CSVs."""

    overall_dir = _variant_viewer_overall_dir(variant_viewer_root)
    if overall_dir is None:
        return []
    path = _resolve_variant_viewer_gene_file(overall_dir, gene)
    if path is None:
        return []

    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"lengths": [], "row_count": 0})
    with open_text_artifact(path, newline="") as stream:
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
    association_db_path: str | Path | None,
    association_table: str,
) -> list[str]:
    if include_genes:
        return sorted({gene.strip().upper() for gene in include_genes if gene.strip()})
    genes = collect_variant_viewer_genes(variant_viewer_root)
    if genes:
        logger.info(
            "Protein context gene source: variant_viewer_root=%s genes=%d",
            variant_viewer_root,
            len(genes),
        )
        return sorted(genes)
    return sorted(
        collect_association_db_genes(
            association_db_path,
            source_table=association_table,
        )
    )


def generate_protein_context_artifacts(
    *,
    output_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    include_genes: set[str] | None = None,
    variant_viewer_root: str | Path | None = None,
    association_db_path: str | Path | None = None,
    association_table: str = "mvp_association_points",
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
    allow_empty: bool = False,
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
        association_db_path=association_db_path,
        association_table=association_table,
    )
    if not genes and not allow_empty:
        diagnostic = _variant_viewer_diagnostic(variant_viewer_root)
        raise ValueError(
            "protein_context found zero candidate genes. "
            f"variant_viewer={diagnostic}; "
            f"association_db_path={str(association_db_path or '')!r}; "
            f"association_table={association_table!r}. "
            "Pass --variant-viewer-root pointing at the splicing-viewer artifacts, "
            "or pass --association-db-path so genes can be inferred from DuckDB."
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
            "association_db_path": str(association_db_path or ""),
            "association_table": association_table,
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
