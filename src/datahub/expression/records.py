"""Data models for expression evidence."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from statistics import median
from typing import Any, Iterable


def normalize_label(value: Any) -> str:
    return str(value or "").strip()


def normalize_phenotype(value: Any) -> str:
    return normalize_label(value).lower().replace("/", " ").replace("-", " ").replace(" ", "_")


def normalize_direction(value: Any, log2_fold_change: float | None = None) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if text in {"up", "upregulated", "up_regulated"}:
        return "up"
    if text in {"down", "downregulated", "down_regulated"}:
        return "down"
    if text in {"not_significant", "unchanged", "ns"}:
        return "not_significant"
    if log2_fold_change is not None:
        if log2_fold_change > 0:
            return "up"
        if log2_fold_change < 0:
            return "down"
    return "not_significant"


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"na", "nan", "none"}:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(parsed) else parsed


def _optional_int(value: Any) -> int | None:
    parsed = _optional_float(value)
    if parsed is None:
        return None
    return int(parsed)


def provenance_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass
class DifferentialExpressionRow:
    gene_id: str
    gene_symbol: str
    gene_id_source: str | None
    study_accession: str
    source_database: str
    source_url: str | None
    assay_type: str | None
    platform: str | None
    species: str | None
    tissue: str | None
    cell_type: str | None
    disease_id: str | None
    disease_name: str
    phenotype_label_original: str
    phenotype_label_normalized: str
    contrast_name: str
    case_group_label: str
    control_group_label: str
    n_case: int | None
    n_control: int | None
    log2_fold_change: float | None
    p_value: float | None
    adjusted_p_value: float | None
    fdr_method: str
    direction: str
    significance_threshold: float
    analysis_method: str
    analysis_package_version: str | None
    preprocessing_method: str | None
    covariates_used: list[str] = field(default_factory=list)
    batch_correction_method: str | None = None
    date_processed: str = field(default_factory=lambda: datetime.now(UTC).date().isoformat())
    provenance_hash: str = ""
    quality_score: float | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        self.gene_id = normalize_label(self.gene_id)
        self.gene_symbol = normalize_label(self.gene_symbol)
        self.study_accession = normalize_label(self.study_accession)
        self.disease_name = normalize_label(self.disease_name)
        self.phenotype_label_original = normalize_label(self.phenotype_label_original)
        self.phenotype_label_normalized = normalize_phenotype(
            self.phenotype_label_normalized or self.phenotype_label_original
        )
        self.direction = normalize_direction(self.direction, self.log2_fold_change)
        if not self.provenance_hash:
            self.provenance_hash = provenance_hash(
                {
                    "gene_id": self.gene_id,
                    "study_accession": self.study_accession,
                    "disease_name": self.disease_name,
                    "contrast_name": self.contrast_name,
                    "log2_fold_change": self.log2_fold_change,
                    "adjusted_p_value": self.adjusted_p_value,
                    "direction": self.direction,
                }
            )

    @classmethod
    def from_mapping(cls, row: dict[str, Any], **defaults: Any) -> "DifferentialExpressionRow":
        gene = normalize_label(row.get("gene_id") or row.get("Gene.symbol") or row.get("gene_symbol"))
        disease = normalize_label(
            row.get("disease_name")
            or row.get("diseases_associated")
            or row.get("disease")
            or row.get("phenotype")
        )
        study = normalize_label(row.get("study_accession") or row.get("gse_id") or row.get("geo_id"))
        logfc = _optional_float(row.get("log2_fold_change") or row.get("logFC"))
        adjusted = _optional_float(row.get("adjusted_p_value") or row.get("adj.P.Val"))
        p_value = _optional_float(row.get("p_value") or row.get("P.Value"))
        source_database = normalize_label(row.get("source_database") or defaults.get("source_database") or "GEO")
        source_url = row.get("source_url")
        if not source_url and study.upper().startswith("GSE"):
            source_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={study}"
        direction = normalize_direction(row.get("direction") or row.get("regulation"), logfc)
        return cls(
            gene_id=gene,
            gene_symbol=gene,
            gene_id_source=row.get("gene_id_source") or "symbol",
            study_accession=study or "UNKNOWN",
            source_database=source_database,
            source_url=source_url,
            assay_type=row.get("assay_type") or defaults.get("assay_type"),
            platform=row.get("platform") or defaults.get("platform"),
            species=row.get("species") or defaults.get("species") or "Homo sapiens",
            tissue=row.get("tissue") or defaults.get("tissue"),
            cell_type=row.get("cell_type") or defaults.get("cell_type"),
            disease_id=row.get("disease_id"),
            disease_name=disease,
            phenotype_label_original=normalize_label(row.get("phenotype_label_original") or disease),
            phenotype_label_normalized=normalize_label(row.get("phenotype_label_normalized") or disease),
            contrast_name=row.get("contrast_name") or "case_vs_control",
            case_group_label=row.get("case_group_label") or "PERT",
            control_group_label=row.get("control_group_label") or "CTRL",
            n_case=_optional_int(row.get("n_case")),
            n_control=_optional_int(row.get("n_control")),
            log2_fold_change=logfc,
            p_value=p_value,
            adjusted_p_value=adjusted,
            fdr_method=row.get("fdr_method") or defaults.get("fdr_method") or "BH",
            direction=direction,
            significance_threshold=float(defaults.get("adjusted_p_value_threshold", 0.05)),
            analysis_method=row.get("analysis_method") or defaults.get("analysis_method") or "UNKNOWN",
            analysis_package_version=row.get("analysis_package_version")
            or defaults.get("analysis_package_version"),
            preprocessing_method=row.get("preprocessing_method") or defaults.get("preprocessing_method"),
            covariates_used=list(defaults.get("covariates_used") or []),
            batch_correction_method=row.get("batch_correction_method")
            or defaults.get("batch_correction_method"),
            date_processed=row.get("date_processed") or defaults.get("date_processed")
            or datetime.now(UTC).date().isoformat(),
            quality_score=_optional_float(row.get("quality_score")),
            notes=row.get("notes") or defaults.get("notes"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class GenePhenotypeSummary:
    gene_id: str
    phenotype_id: str
    phenotype_name: str
    number_of_studies_upregulated: int
    number_of_studies_downregulated: int
    number_of_studies_not_significant: int
    number_of_total_studies: int
    direction_consistency_score: float
    median_log2_fold_change: float | None
    minimum_adjusted_p_value: float | None
    evidence_strength: str
    source_study_accessions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def summarize_gene_phenotypes(rows: Iterable[DifferentialExpressionRow]) -> list[GenePhenotypeSummary]:
    grouped: dict[tuple[str, str], list[DifferentialExpressionRow]] = {}
    for row in rows:
        grouped.setdefault((row.gene_id, row.phenotype_label_normalized), []).append(row)

    summaries: list[GenePhenotypeSummary] = []
    for (gene_id, phenotype_id), group in sorted(grouped.items()):
        studies = {item.study_accession for item in group if item.study_accession}
        up_studies = {item.study_accession for item in group if item.direction == "up"}
        down_studies = {item.study_accession for item in group if item.direction == "down"}
        ns_studies = {item.study_accession for item in group if item.direction == "not_significant"}
        effect_values = [item.log2_fold_change for item in group if item.log2_fold_change is not None]
        adjusted_values = [
            item.adjusted_p_value for item in group if item.adjusted_p_value is not None
        ]
        directional = len(up_studies) + len(down_studies)
        consistency = 0.0 if directional == 0 else abs(len(up_studies) - len(down_studies)) / directional
        total = len(studies) or len(group)
        if total >= 5 and consistency >= 0.75:
            strength = "strong"
        elif total >= 2:
            strength = "moderate"
        else:
            strength = "limited"
        summaries.append(
            GenePhenotypeSummary(
                gene_id=gene_id,
                phenotype_id=phenotype_id,
                phenotype_name=group[0].disease_name,
                number_of_studies_upregulated=len(up_studies),
                number_of_studies_downregulated=len(down_studies),
                number_of_studies_not_significant=len(ns_studies),
                number_of_total_studies=total,
                direction_consistency_score=round(consistency, 6),
                median_log2_fold_change=median(effect_values) if effect_values else None,
                minimum_adjusted_p_value=min(adjusted_values) if adjusted_values else None,
                evidence_strength=strength,
                source_study_accessions=sorted(studies),
            )
        )
    return summaries

