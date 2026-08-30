"""Shared contracts for association evidence model v2."""

from __future__ import annotations

SCHEMA_VERSION = "2.4.0-rc1"

SOURCE_DETAIL_UNAVAILABLE_FIELDS = (
    "effect_allele",
    "effect_size",
    "standard_error",
    "sample_size",
    "ancestry",
    "fine_mapping",
)

PROVIDER_DETAIL_AVAILABLE = "available"
PROVIDER_DETAIL_UNAVAILABLE = "unavailable"
PROVIDER_DETAIL_NOT_APPLICABLE = "not_applicable"

ASSOCIATION_RECORD_KIND_PROVIDER = "source_association_observation"
ASSOCIATION_RECORD_KIND_SOURCE_SUMMARY = "source_summary_association"
EVIDENCE_GRANULARITY_PROVIDER_RECORD = "provider_record"
EVIDENCE_GRANULARITY_SOURCE_SUMMARY = "source_summary"

SOURCE_PRIORITY = (
    "legacy_cvd_raw",
    "legacy_trait_raw",
    "million_veteran_program",
)

POPULATION_FIELDS = (
    ("gnomad_genome.af.af", "Total"),
    ("gnomad_genome.af.af_afr", "African"),
    ("gnomad_genome.af.af_amr", "Admixed American"),
    ("gnomad_genome.af.af_asj", "Ashkenazi Jewish"),
    ("gnomad_genome.af.af_eas", "East Asian"),
    ("gnomad_genome.af.af_fin", "Finnish"),
    ("gnomad_genome.af.af_nfe", "European"),
    ("gnomad_genome.af.af_oth", "Other"),
)
