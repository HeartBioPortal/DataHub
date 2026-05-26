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
    "phenotype_tree_path",
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
    phenotype_tree_path: str
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
        phenotype_tree_path=candidate.phenotype_tree_path or "",
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


CONTROL_TERMS = (
    "healthy control",
    "control",
    "ctrl",
    "donor",
    "normal",
    "reference",
    "sham",
)

CASE_TERMS = (
    "case",
    "patient",
    "disease",
    "cvd",
    "cardiovascular",
    "coronary",
    "myocardial",
    "infarction",
    "heart failure",
    "cardiomyopathy",
    "atrial fibrillation",
    "atherosclerosis",
    "aneurysm",
    "aaa",
    "pulmonary hypertension",
    "hypertension",
    "stroke",
    "myocarditis",
)

CVD_ANCHOR_TERMS = (
    "cvd",
    "cardiovascular",
    "coronary",
    "myocardial",
    "infarction",
    "heart failure",
    "cardiomyopathy",
    "atrial fibrillation",
    "atherosclerosis",
    "aneurysm",
    "aaa",
    "pulmonary hypertension",
    "hypertension",
    "stroke",
    "myocarditis",
)

EXCLUDE_GROUP_TERMS = (
    "after medication",
    "post medication",
    "treated",
    "treatment",
    "therapy",
    "drug",
)

GROUP_COLUMN_TERMS = (
    "condition",
    "diagnosis",
    "disease",
    "group",
    "phenotype",
    "status",
    "source_name",
    "title",
    "description",
)


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _lower(value: object) -> str:
    return _clean_text(value).lower()


def _contains_any(value: str, terms: Iterable[str]) -> bool:
    text = value.lower()
    return any(term and term.lower() in text for term in terms)


def _label_without_prefix(value: object) -> str:
    text = _clean_text(value)
    if ":" in text:
        prefix, suffix = text.split(":", 1)
        if len(prefix.strip()) <= 40 and suffix.strip():
            return suffix.strip()
    return text


def _load_sample_metadata(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _sample_accession(row: dict[str, str]) -> str:
    for column in ("geo_accession", "sample_accession", "gsm", "Unnamed: 0"):
        value = _clean_text(row.get(column))
        if value.startswith("GSM"):
            return value
    for value in row.values():
        cleaned = _clean_text(value)
        if cleaned.startswith("GSM"):
            return cleaned
    return ""


def _metadata_text(rows: list[dict[str, str]]) -> str:
    return " ".join(_lower(value) for row in rows for value in row.values())


def _metadata_terms(row: dict[str, str]) -> list[str]:
    terms = [
        row.get("matched_term", ""),
        row.get("disease_name", ""),
        row.get("phenotype_label_original", ""),
        row.get("phenotype_label_normalized", "").replace("_", " "),
        row.get("phenotype_tree_path", "").replace("/", " ").replace("_", " "),
    ]
    if _contains_any(" ".join(terms), ("abdominal aortic aneurysm", "aneurysm")):
        terms.extend(["aaa", "abdominal aortic aneurysm"])
    return [term.strip().lower() for term in terms if term and term.strip()]


def _infer_assay_type(rows: list[dict[str, str]], fallback: str) -> str:
    text = _metadata_text(rows)
    if "circrna" in text or "circular rna" in text:
        return "circRNA_microarray"
    if "mirna" in text or "micro-rna" in text or "micro rna" in text:
        return "miRNA_microarray"
    if any(term in text for term in ("affymetrix", "illumina", "agilent", "array", "beadchip")):
        return "microarray_or_processed_matrix"
    if "rna-seq" in text or "rna seq" in text or "sequencing" in text:
        return "rnaseq_or_processed_matrix"
    return fallback


def _most_common_metadata_value(rows: list[dict[str, str]], columns: Iterable[str]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        for column in columns:
            if column not in row:
                continue
            value = _label_without_prefix(row.get(column))
            if value:
                counts[value] = counts.get(value, 0) + 1
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))[0][0]


def _infer_tissue(rows: list[dict[str, str]]) -> str:
    columns = [column for column in (rows[0].keys() if rows else []) if "tissue" in column.lower()]
    return _most_common_metadata_value(rows, columns)


def _infer_cell_type(rows: list[dict[str, str]]) -> str:
    columns = [column for column in (rows[0].keys() if rows else []) if "cell type" in column.lower()]
    return _most_common_metadata_value(rows, columns)


def _group_columns(rows: list[dict[str, str]]) -> list[str]:
    if not rows:
        return []
    columns = []
    for column in rows[0]:
        lower = column.lower()
        if any(term in lower for term in GROUP_COLUMN_TERMS):
            columns.append(column)
    return columns


def _classify_group_label(label: str, disease_terms: Iterable[str]) -> str:
    text = label.lower()
    if not text:
        return "unknown"
    if _contains_any(text, EXCLUDE_GROUP_TERMS):
        return "exclude"
    if _contains_any(text, CONTROL_TERMS):
        return "control"
    if _contains_any(text, disease_terms) or _contains_any(text, CASE_TERMS):
        return "case"
    return "unknown"


def _score_group_column(column: str) -> int:
    lower = column.lower()
    score = 0
    for index, term in enumerate(("condition", "diagnosis", "disease", "group", "source_name", "title", "description")):
        if term in lower:
            score += 20 - index
    return score


def _infer_case_control(
    rows: list[dict[str, str]],
    *,
    disease_terms: Iterable[str],
    min_case: int,
    min_control: int,
) -> dict[str, str | int]:
    best: dict[str, str | int] = {
        "case_group_label": "",
        "control_group_label": "",
        "case_sample_accessions": "",
        "control_sample_accessions": "",
        "n_case": 0,
        "n_control": 0,
        "group_column": "",
    }
    best_score = -1
    for column in _group_columns(rows):
        case_accessions: list[str] = []
        control_accessions: list[str] = []
        case_labels: dict[str, int] = {}
        control_labels: dict[str, int] = {}
        for row in rows:
            label = _label_without_prefix(row.get(column))
            label_class = _classify_group_label(label, disease_terms)
            accession = _sample_accession(row)
            if label_class == "case":
                if accession:
                    case_accessions.append(accession)
                case_labels[label] = case_labels.get(label, 0) + 1
            elif label_class == "control":
                if accession:
                    control_accessions.append(accession)
                control_labels[label] = control_labels.get(label, 0) + 1
        n_case = len(case_accessions)
        n_control = len(control_accessions)
        if not n_case and not n_control:
            continue
        score = n_case + n_control + _score_group_column(column)
        if n_case >= min_case and n_control >= min_control:
            score += 200
        elif n_case >= min_case:
            score += 50
        if score <= best_score:
            continue
        best_score = score
        case_label = sorted(case_labels.items(), key=lambda item: (-item[1], item[0].lower()))[0][0] if case_labels else ""
        control_label = (
            sorted(control_labels.items(), key=lambda item: (-item[1], item[0].lower()))[0][0]
            if control_labels
            else ""
        )
        best = {
            "case_group_label": case_label,
            "control_group_label": control_label,
            "case_sample_accessions": ",".join(case_accessions),
            "control_sample_accessions": ",".join(control_accessions),
            "n_case": n_case,
            "n_control": n_control,
            "group_column": column,
        }
    return best


def _append_note(existing: str, note: str) -> str:
    existing = _clean_text(existing)
    return f"{existing} {note}".strip() if existing else note


def suggest_geo_curation_from_sample_metadata(
    *,
    curation_csv: str | Path,
    metadata_dir: str | Path,
    output_csv: str | Path,
    min_case: int = 2,
    min_control: int = 2,
) -> dict[str, object]:
    rows = read_curation_manifest(curation_csv)
    metadata_root = Path(metadata_dir)
    status_counts: dict[str, int] = {}
    output_rows: list[dict[str, str]] = []
    for row in rows:
        accession = row.get("study_accession", "")
        metadata_path = metadata_root / f"{accession}_samples.csv"
        suggested = {column: row.get(column, "") for column in CURATION_COLUMNS}
        suggested["approved"] = "false"
        if not metadata_path.exists():
            suggested["review_status"] = "needs_review_missing_sample_metadata"
            suggested["notes"] = _append_note(suggested.get("notes", ""), "Sample metadata CSV was not found.")
            output_rows.append(suggested)
            status_counts[suggested["review_status"]] = status_counts.get(suggested["review_status"], 0) + 1
            continue

        metadata_rows = _load_sample_metadata(metadata_path)
        disease_terms = _metadata_terms(row)
        assay_type = _infer_assay_type(metadata_rows, row.get("assay_type", ""))
        group = _infer_case_control(
            metadata_rows,
            disease_terms=disease_terms,
            min_case=min_case,
            min_control=min_control,
        )
        n_case = int(group["n_case"])
        n_control = int(group["n_control"])
        metadata_text = _metadata_text(metadata_rows)
        topic_match = _contains_any(metadata_text, disease_terms) or _contains_any(metadata_text, CVD_ANCHOR_TERMS)

        suggested["assay_type"] = assay_type
        suggested["tissue"] = suggested.get("tissue") or _infer_tissue(metadata_rows)
        suggested["cell_type"] = suggested.get("cell_type") or _infer_cell_type(metadata_rows)
        suggested["case_group_label"] = str(group["case_group_label"])
        suggested["control_group_label"] = str(group["control_group_label"])
        suggested["case_sample_accessions"] = str(group["case_sample_accessions"])
        suggested["control_sample_accessions"] = str(group["control_sample_accessions"])
        suggested["n_case"] = str(n_case) if n_case else ""
        suggested["n_control"] = str(n_control) if n_control else ""

        if n_case >= min_case and n_control >= min_control:
            if not topic_match:
                review_status = "needs_review_possible_topic_mismatch"
            elif assay_type in {"miRNA_microarray", "circRNA_microarray"}:
                review_status = "suggested_non_mrna_case_control"
            else:
                review_status = "suggested_case_control"
        elif n_case >= min_case and n_control == 0:
            review_status = "needs_review_no_control"
        else:
            review_status = "needs_review_ambiguous_groups"
        suggested["review_status"] = review_status
        group_column = str(group.get("group_column") or "")
        note = (
            f"Automated metadata suggestion from {metadata_path.name}; "
            f"group column={group_column or 'none'}; final scientific approval is still required."
        )
        suggested["notes"] = _append_note(suggested.get("notes", ""), note)
        output_rows.append(suggested)
        status_counts[review_status] = status_counts.get(review_status, 0) + 1

    output = Path(output_csv)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CURATION_COLUMNS)
        writer.writeheader()
        writer.writerows(output_rows)
    return {
        "output_csv": str(output),
        "row_count": len(output_rows),
        "status_counts": status_counts,
        "min_case": min_case,
        "min_control": min_control,
    }
