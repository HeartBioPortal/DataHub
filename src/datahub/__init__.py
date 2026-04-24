"""Core DataHub pipeline primitives.

This package provides the reusable building blocks for DataHub ingestion,
validation, enrichment, storage and publication.
"""

from .apis import (
    ApiClientError,
    EnsemblRestClient,
    JsonFileApiCache,
    NcbiVariationApiClient,
    RestApiClient,
)
from .annotations import GtfExonRecord, GtfGeneAnnotationIndex, GtfGeneRecord, GtfTranscriptRecord
from .artifact_qa import build_artifact_qa_report, write_artifact_qa_report
from .checkpoints import StructuralVariantCheckpoint, write_json_atomic
from .config import (
    ASSOCIATION_AXIS_FIELDS,
    ASSOCIATION_CORE_FIELDS,
    DatasetContract,
    FieldPolicy,
    MissingFieldStrategy,
    SourcePriority,
    build_association_contract,
)
from .config_schemas import (
    ConfigSchemaFamily,
    ConfigValidationIssue,
    format_config_validation_issues,
    validate_config_file,
    validate_default_config_tree,
)
from .artifact_io import load_json_artifact, open_text_artifact
from .export_helpers import ExportHelperRegistry, build_default_export_helper_registry
from .export_manifest import (
    AssociationExportManifest,
    AssociationExportManifestCatalog,
    AssociationExportManifestLoader,
    AssociationExportRuntime,
)
from .models import CanonicalRecord
from .output_contracts import OutputContract, OutputContractLoader
from .pipeline import DataHubPipeline, DataHubRunReport
from .prep import (
    PREPARED_ASSOCIATION_COLUMNS,
    AssociationRawPreparer,
    PreparationReport,
    RawAssociationPreparationProfile,
    RawAssociationPreparationProfileLoader,
)
from .profiles import DatasetProfile, DatasetProfileLoader
from .registry import AdapterPluginSpec, AdapterRegistry, build_default_adapter_registry
from .publishers import StructuralVariantLegacyPublisher
from .working_duckdb import (
    ANALYSIS_READY_ASSOCIATION_TABLE,
    SOURCE_NORMALIZED_ASSOCIATION_TABLE,
    SchemaDriftResult,
    compare_schema,
    compare_schema_candidate_groups,
    ensure_working_schema,
    expected_column_groups_from_prep_profile,
    inspect_csv_schema,
    load_source_normalized_association_csv,
    materialize_analysis_ready_association_from_points,
    register_raw_release,
)
from .sources import (
    ClinVarSourceConnector,
    DbVarSourceConnector,
    EnsemblSourceConnector,
    GWASSourceConnector,
    ManifestSourceConnector,
    SourceAccessMode,
    SourceConnector,
    SourceConnectorPluginSpec,
    SourceManifest,
    SourceManifestLoader,
    SourceRegistry,
    build_default_source_registry,
)
from .structural_variant_exons import (
    StructuralVariantExonBackfillReport,
    apply_structural_variant_exon_patch,
    enrich_structural_variant_exons,
    gene_needs_exon_backfill,
    transcript_has_exons,
)

__all__ = [
    "CanonicalRecord",
    "OutputContract",
    "OutputContractLoader",
    "ApiClientError",
    "RestApiClient",
    "JsonFileApiCache",
    "EnsemblRestClient",
    "NcbiVariationApiClient",
    "GtfGeneAnnotationIndex",
    "GtfGeneRecord",
    "GtfTranscriptRecord",
    "GtfExonRecord",
    "StructuralVariantCheckpoint",
    "write_json_atomic",
    "build_artifact_qa_report",
    "write_artifact_qa_report",
    "AssociationExportManifest",
    "AssociationExportManifestCatalog",
    "AssociationExportManifestLoader",
    "AssociationExportRuntime",
    "ASSOCIATION_AXIS_FIELDS",
    "ASSOCIATION_CORE_FIELDS",
    "DatasetContract",
    "DataHubPipeline",
    "DataHubRunReport",
    "PREPARED_ASSOCIATION_COLUMNS",
    "open_text_artifact",
    "load_json_artifact",
    "AssociationRawPreparer",
    "PreparationReport",
    "RawAssociationPreparationProfile",
    "RawAssociationPreparationProfileLoader",
    "DatasetProfile",
    "DatasetProfileLoader",
    "FieldPolicy",
    "MissingFieldStrategy",
    "SourcePriority",
    "ConfigSchemaFamily",
    "ConfigValidationIssue",
    "AdapterRegistry",
    "AdapterPluginSpec",
    "SourceAccessMode",
    "SourceManifest",
    "SourceManifestLoader",
    "SourceConnector",
    "ManifestSourceConnector",
    "GWASSourceConnector",
    "EnsemblSourceConnector",
    "ClinVarSourceConnector",
    "DbVarSourceConnector",
    "SourceRegistry",
    "SourceConnectorPluginSpec",
    "StructuralVariantLegacyPublisher",
    "ANALYSIS_READY_ASSOCIATION_TABLE",
    "SOURCE_NORMALIZED_ASSOCIATION_TABLE",
    "SchemaDriftResult",
    "build_association_contract",
    "compare_schema",
    "compare_schema_candidate_groups",
    "ensure_working_schema",
    "expected_column_groups_from_prep_profile",
    "format_config_validation_issues",
    "ExportHelperRegistry",
    "inspect_csv_schema",
    "load_source_normalized_association_csv",
    "materialize_analysis_ready_association_from_points",
    "register_raw_release",
    "validate_config_file",
    "validate_default_config_tree",
    "build_default_export_helper_registry",
    "build_default_adapter_registry",
    "build_default_source_registry",
    "StructuralVariantExonBackfillReport",
    "apply_structural_variant_exon_patch",
    "enrich_structural_variant_exons",
    "gene_needs_exon_backfill",
    "transcript_has_exons",
]
