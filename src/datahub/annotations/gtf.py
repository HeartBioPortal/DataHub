"""Local GTF-backed gene annotation index for overlap and metadata lookup."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from datahub.artifact_io import open_text_artifact


logger = logging.getLogger(__name__)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_chromosome(value: str) -> str:
    text = _clean_text(value)
    lowered = text.lower()
    if lowered.startswith("chr"):
        return text[3:]
    return text


def _strip_version(identifier: str) -> str:
    text = _clean_text(identifier)
    if "." not in text:
        return text
    return text.split(".", 1)[0]


def _strand_to_int(value: str) -> int | None:
    text = _clean_text(value)
    if text == "+":
        return 1
    if text == "-":
        return -1
    return None


def _parse_gtf_attributes(field: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for raw_item in field.split(";"):
        item = raw_item.strip()
        if not item:
            continue
        if " " not in item:
            key = item
            value = ""
        else:
            key, value = item.split(" ", 1)
            value = value.strip().strip('"')

        existing = parsed.get(key)
        if existing is None:
            parsed[key] = value
        elif isinstance(existing, list):
            existing.append(value)
        else:
            parsed[key] = [existing, value]
    return parsed


def _attribute_first(attributes: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = attributes.get(key)
        if isinstance(value, list):
            for item in value:
                text = _clean_text(item)
                if text:
                    return text
        else:
            text = _clean_text(value)
            if text:
                return text
    return ""


def _attribute_list(attributes: dict[str, Any], *keys: str) -> list[str]:
    values: list[str] = []
    for key in keys:
        value = attributes.get(key)
        if isinstance(value, list):
            values.extend(_clean_text(item) for item in value if _clean_text(item))
        else:
            text = _clean_text(value)
            if text:
                values.append(text)
    return values


@dataclass(frozen=True)
class GtfExonRecord:
    """Exon-level annotation extracted from a GTF."""

    exon_id: str | None
    start: int
    end: int
    exon_number: int | None = None

    def to_lookup_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "start": self.start,
            "end": self.end,
        }
        if self.exon_id:
            payload["id"] = self.exon_id
        if self.exon_number is not None:
            payload["exon_number"] = self.exon_number
        return payload


@dataclass(frozen=True)
class GtfTranscriptRecord:
    """Transcript-level annotation extracted from a GTF."""

    transcript_id: str
    transcript_name: str | None
    chromosome: str
    start: int
    end: int
    strand: int | None
    biotype: str | None
    tags: tuple[str, ...] = ()
    exons: list[GtfExonRecord] = field(default_factory=list)

    @property
    def span(self) -> int:
        return abs(self.end - self.start) + 1

    def to_lookup_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.transcript_id,
            "start": self.start,
            "end": self.end,
            "seq_region_name": self.chromosome,
        }
        if self.transcript_name:
            payload["display_name"] = self.transcript_name
        if self.biotype:
            payload["biotype"] = self.biotype
        if self.strand is not None:
            payload["strand"] = self.strand
        if "Ensembl_canonical" in self.tags:
            payload["is_canonical"] = 1
        if "basic" in self.tags:
            payload["gencode_basic"] = 1
        if self.tags:
            payload["tags"] = list(self.tags)
        if self.exons:
            payload["Exon"] = [
                exon.to_lookup_payload()
                for exon in sorted(
                    self.exons,
                    key=lambda item: (
                        item.exon_number if item.exon_number is not None else 10**9,
                        item.start,
                        item.end,
                    ),
                )
            ]
        return payload


@dataclass
class GtfGeneRecord:
    """Gene-level annotation extracted from a GTF."""

    gene_id: str
    gene_name: str
    chromosome: str
    start: int
    end: int
    strand: int | None
    biotype: str | None
    transcripts: list[GtfTranscriptRecord] = field(default_factory=list)

    def overlaps(self, *, start: int, end: int) -> bool:
        return self.start <= end and start <= self.end

    def best_transcript(self) -> GtfTranscriptRecord | None:
        if not self.transcripts:
            return None

        def sort_key(record: GtfTranscriptRecord) -> tuple[int, int, int, int, int]:
            tags = set(record.tags)
            return (
                0 if "Ensembl_canonical" in tags else 1,
                0 if "MANE_Select" in tags else 1,
                0 if any(tag.startswith("appris_principal") for tag in tags) else 1,
                0 if (record.biotype or "") == "protein_coding" else 1,
                -record.span,
            )

        return sorted(self.transcripts, key=sort_key)[0]

    def to_overlap_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.gene_id,
            "external_name": self.gene_name,
            "start": self.start,
            "end": self.end,
        }
        if self.strand is not None:
            payload["strand"] = self.strand
        if self.biotype:
            payload["biotype"] = self.biotype
        return payload

    def to_lookup_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.gene_id,
            "display_name": self.gene_name,
            "start": self.start,
            "end": self.end,
            "Transcript": [],
        }
        if self.strand is not None:
            payload["strand"] = self.strand
        if self.biotype:
            payload["biotype"] = self.biotype

        best = self.best_transcript()
        if best is not None:
            payload["Transcript"] = [best.to_lookup_payload()]
        return payload


class GtfGeneAnnotationIndex:
    """In-memory interval index built from a GTF gene annotation file."""

    def __init__(
        self,
        *,
        path: str | Path,
        bin_size_bp: int = 1_000_000,
        progress_every_lines: int = 500_000,
    ) -> None:
        self.path = Path(path)
        self.bin_size_bp = max(int(bin_size_bp), 1)
        self.progress_every_lines = max(int(progress_every_lines), 1)
        self._genes_by_id: dict[str, GtfGeneRecord] = {}
        self._genes_by_chromosome_bin: dict[str, dict[int, list[str]]] = {}
        self._load()

    def _load(self) -> None:
        logger.info("Loading local GTF gene annotations from %s", self.path)
        line_count = 0
        gene_count = 0
        transcript_count = 0

        with open_text_artifact(self.path, newline="") as handle:
            for raw_line in handle:
                line_count += 1
                if line_count % self.progress_every_lines == 0:
                    logger.info(
                        "[GTF] %s lines=%d genes=%d transcripts=%d",
                        self.path.name,
                        line_count,
                        gene_count,
                        transcript_count,
                    )

                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue

                columns = line.split("\t")
                if len(columns) != 9:
                    continue

                chromosome, _source, feature_type, start_text, end_text, _score, strand_text, _frame, attributes_text = columns
                if feature_type not in {"gene", "transcript", "exon"}:
                    continue

                try:
                    start = int(start_text)
                    end = int(end_text)
                except ValueError:
                    continue

                attributes = _parse_gtf_attributes(attributes_text)
                gene_id = _strip_version(_attribute_first(attributes, "gene_id"))
                if not gene_id:
                    continue

                chromosome = _normalize_chromosome(chromosome)
                strand = _strand_to_int(strand_text)

                if feature_type == "gene":
                    gene_name = _attribute_first(attributes, "gene_name") or gene_id
                    biotype = _attribute_first(attributes, "gene_biotype", "gene_type")
                    gene_record = GtfGeneRecord(
                        gene_id=gene_id,
                        gene_name=gene_name,
                        chromosome=chromosome,
                        start=start,
                        end=end,
                        strand=strand,
                        biotype=biotype or None,
                    )
                    self._genes_by_id[gene_id] = gene_record
                    gene_count += 1
                    continue

                transcript_id = _strip_version(_attribute_first(attributes, "transcript_id"))
                if not transcript_id:
                    continue

                gene_record = self._genes_by_id.get(gene_id)
                if gene_record is None:
                    gene_record = GtfGeneRecord(
                        gene_id=gene_id,
                        gene_name=_attribute_first(attributes, "gene_name") or gene_id,
                        chromosome=chromosome,
                        start=start,
                        end=end,
                        strand=strand,
                        biotype=_attribute_first(attributes, "gene_biotype", "gene_type") or None,
                    )
                    self._genes_by_id[gene_id] = gene_record
                    gene_count += 1

                if feature_type == "transcript":
                    transcript = GtfTranscriptRecord(
                        transcript_id=transcript_id,
                        transcript_name=_attribute_first(attributes, "transcript_name") or None,
                        chromosome=chromosome,
                        start=start,
                        end=end,
                        strand=strand,
                        biotype=_attribute_first(attributes, "transcript_biotype", "transcript_type") or None,
                        tags=tuple(_attribute_list(attributes, "tag")),
                    )
                    gene_record.transcripts.append(transcript)
                    transcript_count += 1
                    continue

                exon_number_text = _attribute_first(attributes, "exon_number")
                try:
                    exon_number = int(exon_number_text) if exon_number_text else None
                except ValueError:
                    exon_number = None
                exon = GtfExonRecord(
                    exon_id=_strip_version(_attribute_first(attributes, "exon_id")) or None,
                    start=start,
                    end=end,
                    exon_number=exon_number,
                )
                for transcript in gene_record.transcripts:
                    if transcript.transcript_id == transcript_id:
                        transcript.exons.append(exon)
                        break

        self._build_bins()
        logger.info(
            "Loaded local GTF annotations from %s: genes=%d transcripts=%d chromosomes=%d",
            self.path,
            len(self._genes_by_id),
            sum(len(record.transcripts) for record in self._genes_by_id.values()),
            len(self._genes_by_chromosome_bin),
        )

    def _build_bins(self) -> None:
        bins: dict[str, dict[int, list[str]]] = {}
        for gene_id, record in self._genes_by_id.items():
            chromosome_bins = bins.setdefault(record.chromosome, {})
            start_bin = record.start // self.bin_size_bp
            end_bin = record.end // self.bin_size_bp
            for bin_index in range(start_bin, end_bin + 1):
                chromosome_bins.setdefault(bin_index, []).append(gene_id)
        self._genes_by_chromosome_bin = bins

    def overlap_region_genes(
        self,
        *,
        chromosome: str,
        start: int,
        end: int,
        species: str = "human",
    ) -> list[dict[str, Any]]:
        del species
        chromosome = _normalize_chromosome(chromosome)
        if start > end:
            start, end = end, start

        chromosome_bins = self._genes_by_chromosome_bin.get(chromosome)
        if not chromosome_bins:
            return []

        start_bin = start // self.bin_size_bp
        end_bin = end // self.bin_size_bp
        candidate_gene_ids: set[str] = set()
        for bin_index in range(start_bin, end_bin + 1):
            candidate_gene_ids.update(chromosome_bins.get(bin_index, []))

        overlapping: list[dict[str, Any]] = []
        for gene_id in sorted(candidate_gene_ids):
            record = self._genes_by_id[gene_id]
            if record.overlaps(start=start, end=end):
                overlapping.append(record.to_overlap_payload())
        return overlapping

    def gene_lookup(self, gene_id: str) -> dict[str, Any]:
        record = self._genes_by_id.get(_strip_version(gene_id))
        if record is None:
            return {}
        return record.to_lookup_payload()

    def lookup_id(self, identifier: str, *, expand: bool = False) -> dict[str, Any]:
        del expand
        return self.gene_lookup(identifier)

    def close(self) -> None:
        return None

    def __len__(self) -> int:
        return len(self._genes_by_id)
