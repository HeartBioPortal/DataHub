"""Registry for DataHub source connectors."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

from datahub.adapters import DataAdapter
from datahub.registry import AdapterRegistry
from datahub.sources.connectors import (
    ClinVarSourceConnector,
    EnsemblSourceConnector,
    GWASSourceConnector,
    SourceConnector,
)
from datahub.sources.manifest import SourceManifestLoader


@dataclass(frozen=True)
class SourceConnectorPluginSpec:
    """Spec describing a dynamically imported source connector class."""

    module: str
    class_name: str
    source_id: str


class SourceRegistry:
    """Registry mapping source IDs to connector implementations."""

    def __init__(self) -> None:
        self._connectors: dict[str, SourceConnector] = {}

    def register(self, connector: SourceConnector) -> None:
        """Register a source connector under its manifest source ID."""

        source_id = connector.manifest.source_id.strip().lower()
        if not source_id:
            raise ValueError("Source ID cannot be empty")
        if source_id in self._connectors:
            raise ValueError(f"Source already registered: {source_id}")
        self._connectors[source_id] = connector

    def register_plugin(
        self,
        plugin: SourceConnectorPluginSpec,
        *,
        manifest_loader: SourceManifestLoader,
    ) -> None:
        """Register a source connector from a dynamic module import."""

        module = importlib.import_module(plugin.module)
        connector_cls = getattr(module, plugin.class_name)
        manifest = manifest_loader.load(plugin.source_id)
        self.register(connector_cls(manifest))

    def create_adapter(
        self,
        source_id: str,
        *,
        adapter_registry: AdapterRegistry,
        params: dict[str, Any] | None = None,
    ) -> DataAdapter:
        """Create an adapter by source ID using the source connector."""

        key = source_id.strip().lower()
        if key not in self._connectors:
            raise KeyError(
                f"Unknown source '{source_id}'. Available: {', '.join(self.available())}"
            )
        return self._connectors[key].create_adapter(
            adapter_registry=adapter_registry,
            params=params,
        )

    def available(self) -> list[str]:
        """Return sorted registered source IDs."""

        return sorted(self._connectors.keys())


def build_default_source_registry(
    manifest_loader: SourceManifestLoader | None = None,
) -> SourceRegistry:
    """Build the default source registry for known built-in sources."""

    loader = manifest_loader or SourceManifestLoader()
    registry = SourceRegistry()

    for source_id, connector_cls in (
        ("gwas_catalog", GWASSourceConnector),
        ("ensembl_variation", EnsemblSourceConnector),
        ("clinvar", ClinVarSourceConnector),
    ):
        manifest = loader.load(source_id)
        registry.register(connector_cls(manifest))

    return registry
