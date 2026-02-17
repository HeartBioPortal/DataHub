"""Core DataHub pipeline primitives.

This package provides the reusable building blocks for DataHub ingestion,
validation, enrichment, storage and publication.
"""

from .config import (
    ASSOCIATION_AXIS_FIELDS,
    ASSOCIATION_CORE_FIELDS,
    DatasetContract,
    FieldPolicy,
    MissingFieldStrategy,
    SourcePriority,
    build_association_contract,
)
from .models import CanonicalRecord
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
from .sources import (
    ClinVarSourceConnector,
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
    "ASSOCIATION_AXIS_FIELDS",
    "ASSOCIATION_CORE_FIELDS",
    "DatasetContract",
    "DataHubPipeline",
    "DataHubRunReport",
    "PREPARED_ASSOCIATION_COLUMNS",
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
    "SourceRegistry",
    "SourceConnectorPluginSpec",
    "build_association_contract",
    "build_default_adapter_registry",
    "build_default_source_registry",
]
