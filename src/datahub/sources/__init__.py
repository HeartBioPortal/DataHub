"""Source manifests and connector registry primitives."""

from .connectors import (
    ClinVarSourceConnector,
    EnsemblSourceConnector,
    GWASSourceConnector,
    ManifestSourceConnector,
    SourceConnector,
)
from .manifest import SourceAccessMode, SourceManifest, SourceManifestLoader
from .registry import (
    SourceConnectorPluginSpec,
    SourceRegistry,
    build_default_source_registry,
)

__all__ = [
    "SourceAccessMode",
    "SourceManifest",
    "SourceManifestLoader",
    "SourceConnector",
    "ManifestSourceConnector",
    "GWASSourceConnector",
    "EnsemblSourceConnector",
    "ClinVarSourceConnector",
    "SourceConnectorPluginSpec",
    "SourceRegistry",
    "build_default_source_registry",
]
