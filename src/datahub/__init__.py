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
from .config import (
    ASSOCIATION_AXIS_FIELDS,
    ASSOCIATION_CORE_FIELDS,
    DatasetContract,
    FieldPolicy,
    MissingFieldStrategy,
    SourcePriority,
    build_association_contract,
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

__all__ = [
    "CanonicalRecord",
    "OutputContract",
    "OutputContractLoader",
    "ApiClientError",
    "RestApiClient",
    "JsonFileApiCache",
    "EnsemblRestClient",
    "NcbiVariationApiClient",
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
    "build_association_contract",
    "ExportHelperRegistry",
    "build_default_export_helper_registry",
    "build_default_adapter_registry",
    "build_default_source_registry",
]
