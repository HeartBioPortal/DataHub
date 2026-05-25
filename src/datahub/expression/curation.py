"""Curation manifest helpers for expression v3."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from .geo_discovery import GeoStudyCandidate


CURATION_COLUMNS = [
    "approved",
    "review_status",
    "study_accession",
    "source_database",
    "source_url",
    "matched_term",
    "disease_name",
    "disease_id",
    "phenotype_label_original",
    "phenotype_label_normalized",
    "assay_type",
    "platform",
    "species",
    "tissue",
    "cell_type",
    "case_group_label",
    "control_group_label",
    "case_sample_accessions",
    "control_sample_accessions",
    "n_case",
    "n_control",
    "contrast_name",
    "analysis_method",
    "notes",
]


@dataclass
class ExpressionCurationRow:
    approved: str
    review_status: str
    study_accession: str
    source_database: str
    source_url: str
    matched_term: str
    disease_name: str
    disease_id: str
    phenotype_label_original: str
    phenotype_label_normalized: str
    assay_type: str
    platform: str
    species: str
    tissue: str
    cell_type: str
    case_group_label: str
    control_group_label: str
    case_sample_accessions: str
    control_sample_accessions: str
    n_case: str
    n_control: str
    contrast_name: str
    analysis_method: str
    notes: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _normalize_label(value: str) -> str:
    return str(value or "").strip().lower().replace("/", " ").replace("-", " ").replace(" ", "_")


def candidate_to_curation_row(candidate: GeoStudyCandidate) -> ExpressionCurationRow:
    disease_name = candidate.matched_term
    return ExpressionCurationRow(
        approved="false",
        review_status="needs_review",
        study_accession=candidate.study_accession,
        source_database="GEO",
        source_url=candidate.source_url,
        matched_term=candidate.matched_term,
        disease_name=disease_name,
        disease_id="",
        phenotype_label_original=disease_name,
        phenotype_label_normalized=_normalize_label(disease_name),
        assay_type="microarray_or_processed_matrix",
        platform=candidate.platform or "",
        species=candidate.organism or "Homo sapiens",
        tissue="",
        cell_type="",
        case_group_label="case",
        control_group_label="control",
        case_sample_accessions="",
        control_sample_accessions="",
        n_case="",
        n_control="",
        contrast_name="case_vs_control",
        analysis_method="geoquery_limma",
        notes=(
            "Review GEO sample metadata and fill case/control sample accessions "
            "before setting approved=true."
        ),
    )


def write_curation_manifest(
    *,
    candidates: Iterable[GeoStudyCandidate],
    output_csv: str | Path,
) -> Path:
    path = Path(output_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [candidate_to_curation_row(candidate).to_dict() for candidate in candidates]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CURATION_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def read_curation_manifest(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="") as handle:
        return list(csv.DictReader(handle))


def approved_curation_rows(path: str | Path) -> list[dict[str, str]]:
    return [
        row
        for row in read_curation_manifest(path)
        if str(row.get("approved", "")).strip().lower() in {"true", "1", "yes", "y"}
    ]

