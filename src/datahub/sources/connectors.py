"""Source connector implementations for DataHub."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from datahub.adapters import DataAdapter
from datahub.registry import AdapterRegistry
from datahub.sources.manifest import SourceManifest


class SourceConnector(ABC):
    """Base connector contract for source-aware ingestion."""

    @property
    @abstractmethod
    def manifest(self) -> SourceManifest:
        """Return source metadata and adapter defaults."""

    @abstractmethod
    def create_adapter(
        self,
        *,
        adapter_registry: AdapterRegistry,
        params: Mapping[str, Any] | None = None,
    ) -> DataAdapter:
        """Build a configured adapter from this source definition."""


@dataclass(frozen=True)
class ManifestSourceConnector(SourceConnector):
    """Generic connector that maps source manifests to adapter instances."""

    source_manifest: SourceManifest

    @property
    def manifest(self) -> SourceManifest:
        return self.source_manifest

    def create_adapter(
        self,
        *,
        adapter_registry: AdapterRegistry,
        params: Mapping[str, Any] | None = None,
    ) -> DataAdapter:
        merged = self.manifest.merged_params(params)
        return adapter_registry.create(self.manifest.adapter_name, **merged)


class _AdapterValidatedConnector(ManifestSourceConnector):
    """Base class for connectors that enforce adapter compatibility."""

    expected_adapter: str

    def __init__(self, source_manifest: SourceManifest) -> None:
        if source_manifest.adapter_name != self.expected_adapter:
            raise ValueError(
                f"{type(self).__name__} expected adapter '{self.expected_adapter}', "
                f"received '{source_manifest.adapter_name}' for source "
                f"'{source_manifest.source_id}'."
            )
        super().__init__(source_manifest=source_manifest)


class GWASSourceConnector(_AdapterValidatedConnector):
    """Connector for GWAS Catalog-derived source data."""

    expected_adapter = "gwas_association"


class EnsemblSourceConnector(_AdapterValidatedConnector):
    """Connector for Ensembl variation-derived source data."""

    expected_adapter = "ensembl_association"


class ClinVarSourceConnector(_AdapterValidatedConnector):
    """Connector for ClinVar-derived source data."""

    expected_adapter = "clinvar_association"
