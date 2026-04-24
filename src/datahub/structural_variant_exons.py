"""Enrich legacy structural-variant artifacts with canonical transcript exons."""

from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping, MutableMapping, Protocol


logger = logging.getLogger(__name__)


class TranscriptLookupClient(Protocol):
    """Minimal Ensembl lookup surface used by SV exon backfill."""

    def lookup_id(self, identifier: str, *, expand: bool = False) -> dict[str, Any]:
        """Return an Ensembl lookup payload."""


@dataclass
class StructuralVariantExonBackfillReport:
    """Summary of an SV exon backfill run."""

    genes_seen: int = 0
    genes_with_existing_exons: int = 0
    candidate_genes: int = 0
    selected_genes: int = 0
    genes_enriched: int = 0
    genes_unchanged: int = 0
    genes_failed: int = 0
    skipped_no_transcript_id: int = 0
    dry_run: bool = False
    force_refresh_exons: bool = False
    unit_partitions: int = 1
    unit_partition_index: int = 0
    enriched_gene_names: list[str] = field(default_factory=list)
    unchanged_gene_names: list[str] = field(default_factory=list)
    failed_gene_errors: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def transcript_has_exons(transcript: Mapping[str, Any] | None) -> bool:
    """Return true when a transcript payload has at least one exon object."""

    if not isinstance(transcript, Mapping):
        return False
    exons = transcript.get("Exon")
    return isinstance(exons, list) and len(exons) > 0


def canonical_transcript(record: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """Return the first canonical transcript payload from a legacy SV gene record."""

    if not isinstance(record, Mapping):
        return None
    transcripts = record.get("canonical_transcript")
    if not isinstance(transcripts, list) or not transcripts:
        return None
    transcript = transcripts[0]
    return transcript if isinstance(transcript, Mapping) else None


def gene_needs_exon_backfill(
    record: Mapping[str, Any] | None,
    *,
    force_refresh_exons: bool = False,
) -> bool:
    """Return true when a gene record should be considered for exon enrichment."""

    transcript = canonical_transcript(record)
    if transcript is None:
        return False
    return force_refresh_exons or not transcript_has_exons(transcript)


def _clean_identifier(value: Any) -> str:
    return str(value or "").strip()


def _strip_version(identifier: str) -> str:
    return identifier.split(".", 1)[0]


def _transcript_identifier(record: Mapping[str, Any]) -> str | None:
    transcript = canonical_transcript(record)
    if transcript is None:
        return None
    identifier = _clean_identifier(transcript.get("id"))
    return _strip_version(identifier) if identifier else None


def _same_identifier(left: Any, right: Any) -> bool:
    left_text = _strip_version(_clean_identifier(left))
    right_text = _strip_version(_clean_identifier(right))
    return bool(left_text and right_text and left_text == right_text)


def _select_transcript_payload(
    lookup_payload: Mapping[str, Any],
    *,
    transcript_id: str,
) -> Mapping[str, Any] | None:
    if _same_identifier(lookup_payload.get("id"), transcript_id):
        return lookup_payload

    transcripts = lookup_payload.get("Transcript")
    if not isinstance(transcripts, list):
        return None

    for transcript in transcripts:
        if isinstance(transcript, Mapping) and _same_identifier(transcript.get("id"), transcript_id):
            return transcript

    canonical = [
        transcript
        for transcript in transcripts
        if isinstance(transcript, Mapping) and bool(transcript.get("is_canonical"))
    ]
    if not canonical:
        canonical = [
            transcript
            for transcript in transcripts
            if isinstance(transcript, Mapping) and bool(transcript.get("gencode_primary"))
        ]
    if canonical:
        return canonical[0]
    first = transcripts[0] if transcripts else None
    return first if isinstance(first, Mapping) else None


def _merge_transcript_payload(
    existing: Mapping[str, Any],
    enriched: Mapping[str, Any],
) -> dict[str, Any]:
    merged = deepcopy(dict(enriched))

    for key, value in existing.items():
        if key == "Exon" and transcript_has_exons(enriched):
            continue
        if value not in (None, "", []):
            merged[key] = deepcopy(value)

    if not transcript_has_exons(merged) and transcript_has_exons(enriched):
        merged["Exon"] = deepcopy(enriched["Exon"])
    return merged


def _selected_by_partition(
    index: int,
    *,
    unit_partitions: int,
    unit_partition_index: int,
) -> bool:
    if unit_partitions <= 1:
        return True
    return index % unit_partitions == unit_partition_index


def _normalize_gene_set(values: Iterable[str] | None) -> set[str] | None:
    if values is None:
        return None
    normalized = {str(value).strip().upper() for value in values if str(value).strip()}
    return normalized or set()


def enrich_structural_variant_exons(
    payload: MutableMapping[str, Any],
    *,
    ensembl_client: TranscriptLookupClient,
    include_genes: Iterable[str] | None = None,
    exclude_genes: Iterable[str] | None = None,
    force_refresh_exons: bool = False,
    dry_run: bool = False,
    unit_partitions: int = 1,
    unit_partition_index: int = 0,
    limit: int | None = None,
    progress_every: int = 100,
    fail_fast: bool = False,
) -> tuple[dict[str, dict[str, Any]], StructuralVariantExonBackfillReport]:
    """Fetch missing canonical-transcript exons and optionally mutate a legacy SV payload.

    Returns a patch payload keyed by gene symbol. The patch can be applied later
    to the full artifact, which makes partitioned HPC fetch jobs safe.
    """

    unit_partitions = max(int(unit_partitions), 1)
    unit_partition_index = int(unit_partition_index)
    if unit_partition_index < 0 or unit_partition_index >= unit_partitions:
        raise ValueError("unit_partition_index must be between 0 and unit_partitions - 1")

    include = _normalize_gene_set(include_genes)
    exclude = _normalize_gene_set(exclude_genes) or set()
    report = StructuralVariantExonBackfillReport(
        genes_seen=len(payload),
        dry_run=bool(dry_run),
        force_refresh_exons=bool(force_refresh_exons),
        unit_partitions=unit_partitions,
        unit_partition_index=unit_partition_index,
    )

    candidates: list[tuple[str, MutableMapping[str, Any]]] = []
    for gene_name in sorted(payload):
        record = payload.get(gene_name)
        if not isinstance(record, MutableMapping):
            continue
        normalized_gene = gene_name.upper()
        if include is not None and normalized_gene not in include:
            continue
        if normalized_gene in exclude:
            continue
        if transcript_has_exons(canonical_transcript(record)) and not force_refresh_exons:
            report.genes_with_existing_exons += 1
            continue
        if gene_needs_exon_backfill(record, force_refresh_exons=force_refresh_exons):
            candidates.append((gene_name, record))

    report.candidate_genes = len(candidates)
    selected = [
        item
        for index, item in enumerate(candidates)
        if _selected_by_partition(
            index,
            unit_partitions=unit_partitions,
            unit_partition_index=unit_partition_index,
        )
    ]
    if limit is not None:
        selected = selected[: max(int(limit), 0)]
    report.selected_genes = len(selected)

    patch: dict[str, dict[str, Any]] = {}
    progress_every = max(int(progress_every), 1)

    for index, (gene_name, record) in enumerate(selected, start=1):
        transcript_id = _transcript_identifier(record)
        if not transcript_id:
            report.skipped_no_transcript_id += 1
            report.genes_unchanged += 1
            report.unchanged_gene_names.append(gene_name)
            continue

        try:
            lookup_payload = ensembl_client.lookup_id(transcript_id, expand=True)
            enriched = _select_transcript_payload(lookup_payload, transcript_id=transcript_id)
            existing = canonical_transcript(record) or {}
            if enriched is None or not transcript_has_exons(enriched):
                report.genes_unchanged += 1
                report.unchanged_gene_names.append(gene_name)
                continue

            merged_transcript = _merge_transcript_payload(existing, enriched)
            patch[gene_name] = {"canonical_transcript": [merged_transcript]}
            report.genes_enriched += 1
            report.enriched_gene_names.append(gene_name)
            if not dry_run:
                record["canonical_transcript"] = [merged_transcript]
        except Exception as exc:  # noqa: BLE001 - keep long HPC runs moving by default.
            if fail_fast:
                raise
            report.genes_failed += 1
            report.failed_gene_errors[gene_name] = str(exc)
            logger.warning("SV exon backfill failed for %s (%s): %s", gene_name, transcript_id, exc)

        if index % progress_every == 0:
            logger.info(
                "SV exon backfill progress partition=%d/%d processed=%d/%d enriched=%d failed=%d",
                unit_partition_index + 1,
                unit_partitions,
                index,
                len(selected),
                report.genes_enriched,
                report.genes_failed,
            )

    return patch, report


def apply_structural_variant_exon_patch(
    payload: MutableMapping[str, Any],
    patch: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply an exon patch produced by :func:`enrich_structural_variant_exons`."""

    genes_payload = patch.get("genes") if isinstance(patch.get("genes"), Mapping) else patch
    applied = 0
    missing = 0
    skipped = 0
    for gene_name, gene_patch in dict(genes_payload or {}).items():
        record = payload.get(gene_name)
        if not isinstance(record, MutableMapping) or not isinstance(gene_patch, Mapping):
            missing += 1
            continue
        transcripts = gene_patch.get("canonical_transcript")
        if not isinstance(transcripts, list) or not transcripts:
            skipped += 1
            continue
        record["canonical_transcript"] = deepcopy(transcripts)
        applied += 1

    return {
        "patch_genes": len(dict(genes_payload or {})),
        "applied_genes": applied,
        "missing_genes": missing,
        "skipped_genes": skipped,
    }
