"""Configuration for CVD expression ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field


DEFAULT_CVD_TERMS = [
    "coronary artery disease",
    "myocardial infarction",
    "heart failure",
    "congestive heart failure",
    "dilated cardiomyopathy",
    "hypertrophic cardiomyopathy",
    "cardiomyopathy",
    "atrial fibrillation",
    "arrhythmia",
    "atherosclerosis",
    "pulmonary hypertension",
    "pulmonary embolism",
    "congenital heart disease",
    "valvular heart disease",
    "valve disease",
    "myocarditis",
    "stroke",
    "cerebrovascular disease",
    "hypertension",
    "ischemia",
    "ischemic heart disease",
    "angina",
    "cardiac arrest",
]


@dataclass(frozen=True)
class ExpressionBuildConfig:
    """Versioned expression build parameters."""

    adjusted_p_value_threshold: float = 0.05
    fdr_method: str = "BH"
    minimum_abs_log2_fold_change: float | None = None
    significance_threshold_label: str = "adjusted_p_value < 0.05"
    analysis_method: str = "legacy_cardioquilt_limma"
    analysis_package_version: str | None = None
    pipeline_name: str = "datahub_expression_v2"
    pipeline_version: str = "1"
    artifact_subdir: str = "expression_v2"
    source_database: str = "GEO/CREEDS"
    source_role: str = "disease_case_control"
    cvd_terms: list[str] = field(default_factory=lambda: list(DEFAULT_CVD_TERMS))
