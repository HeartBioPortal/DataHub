"""DuckDB schema for the production-scale association evidence v2 sidecar."""

from __future__ import annotations

from typing import Any

from .contracts import SCHEMA_VERSION


DDL = r"""
CREATE TABLE IF NOT EXISTS build_metadata (
    schema_version VARCHAR NOT NULL,
    release_id VARCHAR NOT NULL,
    built_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR NOT NULL,
    input_root VARCHAR NOT NULL,
    variant_index_root VARCHAR,
    builder_git_commit VARCHAR,
    builder_command VARCHAR,
    runtime_json JSON,
    counts_json JSON,
    limitations_json JSON
);

CREATE TABLE IF NOT EXISTS phenotype_registry (
    dataset_type VARCHAR NOT NULL,
    phenotype_slug VARCHAR NOT NULL,
    phenotype_path_json JSON NOT NULL,
    phenotype_path_key VARCHAR NOT NULL,
    PRIMARY KEY (dataset_type, phenotype_slug)
);

CREATE TABLE IF NOT EXISTS source_completeness (
    source VARCHAR PRIMARY KEY,
    dataset_types_json JSON NOT NULL,
    provider_detail_status VARCHAR NOT NULL,
    provider_detail_reason VARCHAR,
    missing_fields_json JSON NOT NULL,
    retained_source_summary_artifact VARCHAR,
    input_paths_json JSON NOT NULL,
    source_version VARCHAR,
    source_release VARCHAR,
    notes VARCHAR
);

CREATE TABLE IF NOT EXISTS source_summary_artifacts (
    source_summary_artifact_id VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    dataset_type VARCHAR NOT NULL,
    gene_id VARCHAR NOT NULL,
    logical_artifact VARCHAR NOT NULL,
    file_format VARCHAR NOT NULL,
    size_bytes UBIGINT NOT NULL,
    modified_at_ns UBIGINT NOT NULL,
    provider_detail_status VARCHAR NOT NULL,
    provider_detail_reason VARCHAR,
    missing_fields_json JSON NOT NULL,
    content_contract VARCHAR NOT NULL,
    publication_mode VARCHAR NOT NULL,
    UNIQUE (source, dataset_type, gene_id, logical_artifact)
);

CREATE TABLE IF NOT EXISTS provider_records (
    provider_record_id VARCHAR NOT NULL,
    association_record_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    dataset_type VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    source_file_logical VARCHAR NOT NULL,
    source_file_absolute VARCHAR NOT NULL,
    source_line UBIGINT NOT NULL,
    source_row_sha256 VARCHAR NOT NULL,
    marker_id_raw VARCHAR,
    p_value_raw VARCHAR,
    gwas_summary_path_raw VARCHAR,
    study_id_raw VARCHAR,
    phenotype_raw VARCHAR,
    phenotype_slug VARCHAR,
    study_raw VARCHAR,
    pmid_raw VARCHAR,
    study_genome_build_raw VARCHAR,
    variant_id_raw VARCHAR,
    dbsnp_build_raw VARCHAR,
    allele_string_raw VARCHAR,
    chromosome_raw VARCHAR,
    hg19_start_raw VARCHAR,
    hg19_end_raw VARCHAR,
    source_variation_type_raw VARCHAR,
    frequency_total_raw VARCHAR,
    frequency_afr_raw VARCHAR,
    frequency_amr_raw VARCHAR,
    frequency_asj_raw VARCHAR,
    frequency_eas_raw VARCHAR,
    frequency_fin_raw VARCHAR,
    frequency_nfe_raw VARCHAR,
    frequency_oth_raw VARCHAR,
    gene_id_raw VARCHAR,
    consequence_raw VARCHAR,
    putative_impact_raw VARCHAR,
    feature_id_raw VARCHAR,
    hgvs_p_raw VARCHAR,
    protein_length_raw VARCHAR,
    dbnsfp_chrom_raw VARCHAR,
    dbnsfp_hg18_start_raw VARCHAR,
    dbnsfp_hg18_end_raw VARCHAR,
    dbnsfp_hg19_start_raw VARCHAR,
    dbnsfp_hg19_end_raw VARCHAR,
    dbnsfp_hg38_start_raw VARCHAR,
    dbnsfp_hg38_end_raw VARCHAR,
    ensembl_protein_id_raw VARCHAR,
    ensembl_transcript_id_raw VARCHAR,
    clinical_significance_raw VARCHAR,
    ingested_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS variants (
    variant_id VARCHAR PRIMARY KEY,
    normalized_allele_contexts_json JSON NOT NULL,
    coordinate_contexts_json JSON NOT NULL,
    source_reported_variation_types_json JSON NOT NULL,
    variation_type VARCHAR,
    variation_type_status VARCHAR NOT NULL,
    variation_type_method VARCHAR NOT NULL,
    variation_type_reason VARCHAR,
    ref_allele_status VARCHAR NOT NULL,
    alt_allele_status VARCHAR NOT NULL,
    effect_allele_status VARCHAR NOT NULL,
    multiallelic_observed BOOLEAN NOT NULL,
    build_or_position_conflict BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS association_records (
    association_record_id VARCHAR NOT NULL,
    record_kind VARCHAR NOT NULL,
    evidence_granularity VARCHAR NOT NULL,
    source_display_name VARCHAR,
    dataset_id VARCHAR,
    dataset_type VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    gene_id_scope VARCHAR,
    variant_id VARCHAR NOT NULL,
    phenotype_raw VARCHAR,
    phenotype_slug VARCHAR NOT NULL,
    phenotype_path_json JSON NOT NULL,
    phenotype_path_key VARCHAR NOT NULL,
    study_id VARCHAR,
    study_title VARCHAR,
    pmid VARCHAR,
    gwas_summary_path VARCHAR,
    reported_genome_build VARCHAR,
    reported_p_value DOUBLE,
    variation_type VARCHAR,
    effect_allele VARCHAR,
    effect_allele_status VARCHAR NOT NULL,
    effect_size DOUBLE,
    effect_size_type VARCHAR,
    effect_size_status VARCHAR NOT NULL,
    standard_error DOUBLE,
    standard_error_status VARCHAR NOT NULL,
    sample_size DOUBLE,
    sample_size_status VARCHAR NOT NULL,
    ancestry_json JSON,
    ancestry_status VARCHAR NOT NULL,
    fine_mapping_json JSON,
    fine_mapping_status VARCHAR NOT NULL,
    provider_detail_status VARCHAR NOT NULL,
    provider_detail_reason VARCHAR,
    missing_fields_json JSON NOT NULL,
    retained_source_summary_artifact VARCHAR,
    retained_source_summary_json JSON,
    provider_record_count UBIGINT NOT NULL,
    source_file_count UBIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS association_record_genes (
    association_record_id VARCHAR NOT NULL,
    gene_id VARCHAR NOT NULL,
    gene_linkage_method VARCHAR NOT NULL,
    provider_record_count UBIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS association_record_provider_records (
    association_record_id VARCHAR NOT NULL,
    provider_record_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS consequence_annotations (
    consequence_annotation_id VARCHAR PRIMARY KEY,
    variant_id VARCHAR NOT NULL,
    gene_id VARCHAR,
    consequence VARCHAR NOT NULL,
    putative_impact VARCHAR,
    transcript_or_feature_id VARCHAR,
    ensembl_transcript_id VARCHAR,
    ensembl_protein_id VARCHAR,
    hgvs_protein VARCHAR,
    protein_length DOUBLE,
    annotation_source VARCHAR NOT NULL,
    annotation_version VARCHAR,
    reported_genome_build VARCHAR,
    provider_detail_status VARCHAR NOT NULL,
    severity_selection VARCHAR NOT NULL,
    provider_record_count UBIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS consequence_annotation_provider_records (
    consequence_annotation_id VARCHAR NOT NULL,
    provider_record_id VARCHAR NOT NULL,
    association_record_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS clinical_assertions (
    clinical_assertion_id VARCHAR PRIMARY KEY,
    variant_id VARCHAR NOT NULL,
    clinical_significance VARCHAR NOT NULL,
    raw_source_value VARCHAR NOT NULL,
    assertion_source VARCHAR NOT NULL,
    assertion_version VARCHAR,
    condition_name VARCHAR,
    condition_status VARCHAR NOT NULL,
    provider_detail_status VARCHAR NOT NULL,
    provider_record_count UBIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS clinical_assertion_provider_records (
    clinical_assertion_id VARCHAR NOT NULL,
    provider_record_id VARCHAR NOT NULL,
    association_record_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS population_observations (
    population_observation_id VARCHAR NOT NULL,
    variant_id VARCHAR NOT NULL,
    frequency_source VARCHAR NOT NULL,
    frequency_source_version VARCHAR,
    population VARCHAR NOT NULL,
    reported_frequency DOUBLE NOT NULL,
    allele_context_json JSON NOT NULL,
    reported_genome_build VARCHAR,
    linkage_method VARCHAR NOT NULL,
    association_allele_status VARCHAR NOT NULL,
    frequency_allele_status VARCHAR NOT NULL,
    provider_detail_status VARCHAR NOT NULL,
    provider_record_count UBIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS population_observation_provider_records (
    population_observation_id VARCHAR NOT NULL,
    provider_record_id VARCHAR NOT NULL,
    association_record_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS variant_phenotype_summaries (
    variant_phenotype_summary_id VARCHAR NOT NULL,
    dataset_type VARCHAR NOT NULL,
    gene_id VARCHAR NOT NULL,
    variant_id VARCHAR NOT NULL,
    phenotype_slug VARCHAR NOT NULL,
    phenotype_path_json JSON NOT NULL,
    phenotype_path_key VARCHAR NOT NULL,
    source_observation_count UBIGINT NOT NULL,
    association_record_count UBIGINT NOT NULL,
    provider_association_record_count UBIGINT NOT NULL,
    retained_source_summary_count UBIGINT NOT NULL,
    study_count UBIGINT NOT NULL,
    population_observation_count UBIGINT NOT NULL,
    consequence_annotation_count UBIGINT NOT NULL,
    clinical_assertion_count UBIGINT NOT NULL,
    minimum_reported_p_value DOUBLE,
    minimum_reported_p_value_association_record_id VARCHAR,
    minimum_reported_p_value_tied_record_ids_json JSON NOT NULL,
    maximum_sample_size DOUBLE,
    maximum_sample_size_status VARCHAR NOT NULL,
    effect_direction_summary_json JSON,
    effect_direction_status VARCHAR NOT NULL,
    fine_mapping_summary_json JSON,
    fine_mapping_status VARCHAR NOT NULL,
    consequence_conflict BOOLEAN NOT NULL,
    clinical_significance_conflict BOOLEAN NOT NULL,
    variation_type VARCHAR,
    variation_type_status VARCHAR NOT NULL,
    source_completeness_json JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS summary_consequence_annotations (
    variant_phenotype_summary_id VARCHAR NOT NULL,
    consequence_annotation_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS summary_clinical_assertions (
    variant_phenotype_summary_id VARCHAR NOT NULL,
    clinical_assertion_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS summary_association_records (
    variant_phenotype_summary_id VARCHAR NOT NULL,
    association_record_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS qa_results (
    check_name VARCHAR PRIMARY KEY,
    status VARCHAR NOT NULL,
    observed_value VARCHAR,
    expected_value VARCHAR,
    details_json JSON NOT NULL,
    checked_at TIMESTAMP NOT NULL
);
"""


INDEX_DDL = r"""
CREATE INDEX IF NOT EXISTS idx_source_summary_gene ON source_summary_artifacts(gene_id, dataset_type);
CREATE INDEX IF NOT EXISTS idx_association_id ON association_records(association_record_id);
CREATE INDEX IF NOT EXISTS idx_summary_gene ON variant_phenotype_summaries(gene_id);
"""


def create_schema(connection: Any) -> None:
    """Create all v2 tables without replacing existing sidecar contents."""

    connection.execute(DDL)


def create_indexes(connection: Any) -> None:
    """Create bounded serving indexes after bulk publication.

    Billion-row provenance links are served from hash-partitioned Parquet sidecars;
    building global ART indexes for those tables is intentionally unsupported.
    """

    connection.execute(INDEX_DDL)


def schema_version() -> str:
    return SCHEMA_VERSION
