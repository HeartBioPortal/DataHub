"""dbVar structural-variant adapter for legacy HBP SV publication."""

from __future__ import annotations

import csv
import logging
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

from datahub.adapters.base import DataAdapter
from datahub.adapters.common import expand_input_paths
from datahub.annotations import GtfGeneAnnotationIndex
from datahub.apis import EnsemblRestClient
from datahub.artifact_io import load_json_artifact, open_text_artifact
from datahub.models import CanonicalRecord


logger = logging.getLogger(__name__)

MISSING_TOKENS = {"", "nan", "none", "null", "na", "n/a"}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in MISSING_TOKENS:
        return ""
    return text


def _parse_int(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _normalize_chromosome(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith("chr"):
        text = text[3:]
    parsed = _parse_int(text)
    return str(parsed) if parsed is not None else text


def _source_file_key(path: Path) -> str:
    try:
        return str(path.resolve())
    except OSError:
        return str(path)


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = _clean_text(value)
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _split_phenotypes(value: Any) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    return _dedupe_keep_order(text.split(";"))


def _coordinate_from_row(row: Mapping[str, Any], *columns: str) -> int | None:
    for column in columns:
        parsed = _parse_int(row.get(column))
        if parsed is not None:
            return parsed
    return None


def _normalize_variant_type(row: Mapping[str, Any]) -> str | None:
    variant_type = _clean_text(row.get("Variant Call type"))
    if variant_type:
        return variant_type.lower()
    region_type = _clean_text(row.get("Variant Region type"))
    if region_type:
        return region_type.lower()
    return None


def phenotype_matches_terms(phenotypes: Iterable[str], phenotype_terms: Iterable[str]) -> bool:
    normalized_terms = [term.lower() for term in phenotype_terms if _clean_text(term)]
    if not normalized_terms:
        return True
    for phenotype in phenotypes:
        lowered = phenotype.lower()
        if any(term in lowered for term in normalized_terms):
            return True
    return False


def _count_data_rows(path: Path) -> int:
    with open_text_artifact(path, newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def load_structural_variant_metadata_seed(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload_path = Path(path)
    if not payload_path.exists():
        return {}
    loaded = load_json_artifact(payload_path)
    return loaded if isinstance(loaded, dict) else {}


def _normalize_phenotype_terms(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [term for term in (_clean_text(item) for item in value.split(",")) if term]
    return [term for term in (_clean_text(item) for item in value) if term]


@dataclass(frozen=True)
class NormalizedStructuralVariant:
    """Normalized dbVar variant row used to build canonical SV records."""

    variant_id: str
    study_id: str
    chromosome: str
    start: int
    end: int
    variant_type: str | None
    phenotypes: tuple[str, ...]
    clinical_significance: str | None
    assembly_name: str | None

    @property
    def variant_region(self) -> str:
        return f"{self.start}-{self.end}"


@dataclass(frozen=True)
class GeneMetadata:
    """Resolved gene metadata attached to a structural-variant record."""

    gene_location: str | None
    strand: int | None
    biotype: str | None
    canonical_transcript: list[dict[str, Any]]


class DbVarStructuralVariantAdapter(DataAdapter):
    """Stream dbVar structural-variant CSV rows into canonical records."""

    name = "dbvar_structural_variant"

    def __init__(
        self,
        *,
        input_paths: str | Path | Iterable[str | Path],
        dataset_id: str = "hbp_dbvar_structural_variant",
        dataset_type: str = "STRUCTURAL_VARIANT",
        source: str = "dbvar",
        phenotype_terms: str | Iterable[str] | None = None,
        metadata_seed_path: str | Path | None = None,
        reference_json_path: str | Path | None = None,
        gene_annotation_gtf_path: str | Path | None = None,
        gene_annotation_index: GtfGeneAnnotationIndex | None = None,
        resume_rows_by_file: Mapping[str, int] | None = None,
        row_completion_callback: Callable[[str, int], None] | None = None,
        ensembl_client: EnsemblRestClient | None = None,
        ensembl_cache_path: str | Path | None = None,
        ensembl_timeout_seconds: float = 30.0,
        ensembl_sleep_seconds: float = 0.0,
        max_overlap_bp: int = 5_000_000,
        progress_every: int = 5_000,
        count_rows: bool = True,
    ) -> None:
        self.input_paths = expand_input_paths(input_paths)
        if not self.input_paths:
            raise FileNotFoundError(f"No dbVar input files matched: {input_paths}")

        self.dataset_id = dataset_id
        self.dataset_type = dataset_type.upper()
        self.source = source
        self.phenotype_terms = _normalize_phenotype_terms(phenotype_terms)
        seed_path = metadata_seed_path or reference_json_path
        self.metadata_seed = load_structural_variant_metadata_seed(seed_path)
        self.progress_every = max(int(progress_every), 1)
        self.count_rows = bool(count_rows)
        self._gene_metadata_cache: dict[str, GeneMetadata] = {}
        self.resume_rows_by_file = {
            str(file_key): max(int(row_index), 0)
            for file_key, row_index in dict(resume_rows_by_file or {}).items()
        }
        self.row_completion_callback = row_completion_callback
        self.report: dict[str, Any] = {}
        self._owns_gene_annotation_index = False

        if gene_annotation_index is not None:
            self.gene_annotation_index = gene_annotation_index
        elif gene_annotation_gtf_path is not None:
            self.gene_annotation_index = GtfGeneAnnotationIndex(path=gene_annotation_gtf_path)
            self._owns_gene_annotation_index = True
        else:
            self.gene_annotation_index = None

        if ensembl_client is None:
            self.ensembl_client = EnsemblRestClient(
                cache_path=ensembl_cache_path,
                timeout_seconds=ensembl_timeout_seconds,
                sleep_seconds=ensembl_sleep_seconds,
                max_overlap_bp=max_overlap_bp,
            )
            self._owns_ensembl_client = True
        else:
            self.ensembl_client = ensembl_client
            self._owns_ensembl_client = False

    def read(self) -> Iterator[CanonicalRecord]:
        self._reset_report()
        try:
            for source_path in self.input_paths:
                yield from self._read_one_file(source_path)
        finally:
            if self._owns_gene_annotation_index and self.gene_annotation_index is not None:
                self.gene_annotation_index.close()
            if self._owns_ensembl_client:
                self.ensembl_client.close()

    def _reset_report(self) -> None:
        self.report = {
            "input_files": [str(path) for path in self.input_paths],
            "rows_seen": 0,
            "rows_skipped_missing_region": 0,
            "rows_filtered_by_phenotype": 0,
            "rows_skipped_by_resume": 0,
            "rows_missing_variant_type": 0,
            "rows_missing_phenotype": 0,
            "rows_missing_clinical_significance": 0,
            "rows_without_overlapping_genes": 0,
            "records_emitted": 0,
        }

    def _read_one_file(self, source_path: Path) -> Iterator[CanonicalRecord]:
        total_rows = 0
        if self.count_rows:
            logger.info("Counting rows in %s for progress reporting...", source_path)
            total_rows = _count_data_rows(source_path)
            logger.info("Processing %s (%d data rows)", source_path, total_rows)
        else:
            logger.info("Processing %s (row counting disabled)", source_path)

        started_at = time.perf_counter()
        file_row_index = 0
        source_file_key = _source_file_key(source_path)
        resume_row_index = self.resume_rows_by_file.get(source_file_key, 0)
        if resume_row_index:
            logger.info("Resuming %s from row %d", source_path, resume_row_index)
        for row in self._iter_rows(source_path):
            file_row_index += 1
            self.report["rows_seen"] += 1
            if file_row_index <= resume_row_index:
                self.report["rows_skipped_by_resume"] += 1
                self._log_progress(
                    source_path=source_path,
                    file_row_index=file_row_index,
                    total_rows=total_rows,
                    started_at=started_at,
                )
                continue

            variant = self._normalize_variant_row(row)
            if variant is None:
                self.report["rows_skipped_missing_region"] += 1
                self._log_progress(
                    source_path=source_path,
                    file_row_index=file_row_index,
                    total_rows=total_rows,
                    started_at=started_at,
                )
                continue

            if variant.variant_type is None:
                self.report["rows_missing_variant_type"] += 1
            if not variant.phenotypes:
                self.report["rows_missing_phenotype"] += 1
            if variant.clinical_significance is None:
                self.report["rows_missing_clinical_significance"] += 1
            if self.phenotype_terms and not phenotype_matches_terms(variant.phenotypes, self.phenotype_terms):
                self.report["rows_filtered_by_phenotype"] += 1
                self._log_progress(
                    source_path=source_path,
                    file_row_index=file_row_index,
                    total_rows=total_rows,
                    started_at=started_at,
                )
                continue

            overlap_payloads = self._overlap_region_genes(variant)
            emitted_for_row = 0
            for overlap_payload in overlap_payloads:
                record = self._to_record(
                    variant=variant,
                    overlap_payload=overlap_payload,
                    source_path=source_path,
                    source_file_key=source_file_key,
                    source_row_index=file_row_index,
                    source_total_rows=total_rows,
                )
                if record is None:
                    continue
                emitted_for_row += 1
                self.report["records_emitted"] += 1
                yield record

            if emitted_for_row == 0:
                self.report["rows_without_overlapping_genes"] += 1

            self._log_progress(
                source_path=source_path,
                file_row_index=file_row_index,
                total_rows=total_rows,
                started_at=started_at,
            )
            if self.row_completion_callback is not None:
                self.row_completion_callback(source_file_key, file_row_index)

        self._log_progress(
            source_path=source_path,
            file_row_index=file_row_index,
            total_rows=total_rows,
            started_at=started_at,
            force=True,
        )
        logger.info(
            "Finished %s in %.1fs: rows=%d records=%d",
            source_path,
            time.perf_counter() - started_at,
            file_row_index,
            self.report["records_emitted"],
        )

    def _iter_rows(self, input_path: Path) -> Iterator[dict[str, str]]:
        with open_text_artifact(input_path, newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames:
                reader.fieldnames[0] = reader.fieldnames[0].lstrip("\ufeff")
            for row in reader:
                yield dict(row)

    def _log_progress(
        self,
        *,
        source_path: Path,
        file_row_index: int,
        total_rows: int,
        started_at: float,
        force: bool = False,
    ) -> None:
        if not force and file_row_index % self.progress_every != 0:
            return

        elapsed = time.perf_counter() - started_at
        rate = file_row_index / elapsed if elapsed > 0 else 0.0
        if total_rows:
            progress_label = f"{file_row_index}/{total_rows} ({file_row_index / total_rows * 100.0:.1f}%)"
        else:
            progress_label = f"{file_row_index}/?"

        logger.info(
            "[SV] %s rows=%s rate=%.1f rows/s emitted=%d filtered=%d resumed_skip=%d missing_region=%d "
            "missing_type=%d missing_pheno=%d missing_sig=%d no_gene=%d",
            source_path.name,
            progress_label,
            rate,
            self.report["records_emitted"],
            self.report["rows_filtered_by_phenotype"],
            self.report["rows_skipped_by_resume"],
            self.report["rows_skipped_missing_region"],
            self.report["rows_missing_variant_type"],
            self.report["rows_missing_phenotype"],
            self.report["rows_missing_clinical_significance"],
            self.report["rows_without_overlapping_genes"],
        )

    def _overlap_region_genes(self, variant: NormalizedStructuralVariant) -> list[dict[str, Any]]:
        if self.gene_annotation_index is not None:
            overlap_payloads = self.gene_annotation_index.overlap_region_genes(
                chromosome=variant.chromosome,
                start=variant.start,
                end=variant.end,
            )
            if overlap_payloads:
                return overlap_payloads

        return self.ensembl_client.overlap_region_genes(
            chromosome=variant.chromosome,
            start=variant.start,
            end=variant.end,
        )

    def _normalize_variant_row(self, row: Mapping[str, Any]) -> NormalizedStructuralVariant | None:
        variant_id = _clean_text(row.get("Variant ID"))
        study_id = _clean_text(row.get("Study ID"))
        chromosome = _normalize_chromosome(row.get("Chromosome"))
        start = _coordinate_from_row(row, "Inner Start", "Start", "Outer Start")
        end = _coordinate_from_row(row, "Inner End", "End", "Outer End")
        if not variant_id or not study_id or not chromosome or start is None or end is None:
            return None
        if start > end:
            start, end = end, start
        return NormalizedStructuralVariant(
            variant_id=variant_id,
            study_id=study_id,
            chromosome=chromosome,
            start=start,
            end=end,
            variant_type=_normalize_variant_type(row),
            phenotypes=tuple(_split_phenotypes(row.get("Subject Phenotype"))),
            clinical_significance=_clean_text(row.get("Clinical Interpretation")) or None,
            assembly_name=_clean_text(row.get("Assembly Name")) or None,
        )

    def _to_record(
        self,
        *,
        variant: NormalizedStructuralVariant,
        overlap_payload: Mapping[str, Any],
        source_path: Path,
        source_file_key: str,
        source_row_index: int,
        source_total_rows: int,
    ) -> CanonicalRecord | None:
        gene_symbol = _clean_text(
            overlap_payload.get("external_name")
            or overlap_payload.get("gene_name")
            or overlap_payload.get("display_name")
        )
        gene_stable_id = _clean_text(overlap_payload.get("id"))
        if not gene_symbol or not gene_stable_id:
            return None

        metadata = self._gene_metadata(
            gene_symbol=gene_symbol,
            gene_stable_id=gene_stable_id,
            overlap_payload=overlap_payload,
        )

        phenotype = "; ".join(variant.phenotypes)
        return CanonicalRecord(
            dataset_id=self.dataset_id,
            dataset_type=self.dataset_type,
            source=self.source,
            gene_id=gene_symbol,
            variant_id=variant.variant_id,
            phenotype=phenotype,
            disease_category=None,
            variation_type=variant.variant_type,
            clinical_significance=variant.clinical_significance,
            most_severe_consequence=None,
            p_value=None,
            pmid=None,
            ancestry={},
            metadata={
                "study_id": variant.study_id,
                "assembly_name": variant.assembly_name,
                "variant_region": variant.variant_region,
                "chromosome": variant.chromosome,
                "start": variant.start,
                "end": variant.end,
                "phenotypes": list(variant.phenotypes),
                "gene_stable_id": gene_stable_id,
                "gene_location": metadata.gene_location,
                "strand": metadata.strand,
                "biotype": metadata.biotype,
                "canonical_transcript": deepcopy(metadata.canonical_transcript),
                "source_file": str(source_path),
                "source_file_key": source_file_key,
                "source_row_index": source_row_index,
                "source_total_rows": source_total_rows or None,
            },
        )

    def _gene_metadata(
        self,
        *,
        gene_symbol: str,
        gene_stable_id: str,
        overlap_payload: Mapping[str, Any],
    ) -> GeneMetadata:
        cached = self._gene_metadata_cache.get(gene_stable_id)
        if cached is not None:
            return cached

        reference = self.metadata_seed.get(gene_symbol) or {}
        reference_gene_location = _clean_text(reference.get("gene_location")) or None
        reference_strand = reference.get("strand")
        if reference_strand in ("", None):
            reference_strand = None
        reference_biotype = _clean_text(reference.get("biotype")) or None
        reference_transcript = deepcopy(reference.get("canonical_transcript") or [])
        if reference_gene_location and reference_strand is not None and reference_biotype:
            metadata = GeneMetadata(
                gene_location=reference_gene_location,
                strand=_parse_int(reference_strand),
                biotype=reference_biotype,
                canonical_transcript=reference_transcript,
            )
            self._gene_metadata_cache[gene_stable_id] = metadata
            return metadata

        lookup_payload: Mapping[str, Any] = {}
        if self.gene_annotation_index is not None:
            lookup_payload = self.gene_annotation_index.gene_lookup(gene_stable_id)
        if not lookup_payload:
            lookup_payload = self.ensembl_client.gene_lookup(gene_stable_id)
        start = _parse_int(lookup_payload.get("start")) or _parse_int(overlap_payload.get("start"))
        end = _parse_int(lookup_payload.get("end")) or _parse_int(overlap_payload.get("end"))
        gene_location = f"{start}-{end}" if start is not None and end is not None else None
        strand = _parse_int(lookup_payload.get("strand"))
        if strand is None:
            strand = _parse_int(overlap_payload.get("strand"))
        biotype = _clean_text(lookup_payload.get("biotype")) or _clean_text(overlap_payload.get("biotype")) or None
        canonical_transcript = self._canonical_transcript_from_lookup(lookup_payload)

        metadata = GeneMetadata(
            gene_location=gene_location,
            strand=strand,
            biotype=biotype,
            canonical_transcript=canonical_transcript,
        )
        self._gene_metadata_cache[gene_stable_id] = metadata
        return metadata

    @staticmethod
    def _canonical_transcript_from_lookup(lookup_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        transcripts = list(lookup_payload.get("Transcript") or [])
        if not transcripts:
            return []
        canonical = [item for item in transcripts if bool(item.get("is_canonical"))]
        if not canonical:
            canonical = [item for item in transcripts if bool(item.get("gencode_primary"))]
        if not canonical:
            canonical = [transcripts[0]]
        return [deepcopy(canonical[0])]
