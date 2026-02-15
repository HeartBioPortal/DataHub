"""Adapter registry for pluggable community ingestion."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from datahub.adapters import (
    ClinVarAssociationAdapter,
    DataAdapter,
    EnsemblAssociationAdapter,
    GWASAssociationAdapter,
    LegacyAssociationCsvAdapter,
)

AdapterFactory = Callable[..., DataAdapter]


@dataclass(frozen=True)
class AdapterPluginSpec:
    """Spec describing a dynamically imported adapter implementation."""

    name: str
    module: str
    class_name: str


class AdapterRegistry:
    """Registry that maps stable adapter names to constructors."""

    def __init__(self) -> None:
        self._factories: dict[str, AdapterFactory] = {}

    def register(self, name: str, factory: AdapterFactory) -> None:
        """Register an adapter factory under a unique name."""

        key = name.strip().lower()
        if not key:
            raise ValueError("Adapter name cannot be empty")
        if key in self._factories:
            raise ValueError(f"Adapter already registered: {name}")
        self._factories[key] = factory

    def register_plugin(self, plugin: AdapterPluginSpec) -> None:
        """Register an adapter by importing a module/class at runtime."""

        module = importlib.import_module(plugin.module)
        adapter_cls = getattr(module, plugin.class_name)
        self.register(plugin.name, adapter_cls)

    def create(self, name: str, **kwargs: Any) -> DataAdapter:
        """Instantiate a registered adapter."""

        key = name.strip().lower()
        if key not in self._factories:
            raise KeyError(
                f"Unknown adapter '{name}'. Available: {', '.join(self.available())}"
            )
        return self._factories[key](**kwargs)

    def available(self) -> list[str]:
        """Return sorted list of known adapter names."""

        return sorted(self._factories.keys())


def build_default_adapter_registry() -> AdapterRegistry:
    """Create a registry preloaded with built-in DataHub adapters."""

    registry = AdapterRegistry()
    registry.register(LegacyAssociationCsvAdapter.name, LegacyAssociationCsvAdapter)
    registry.register(GWASAssociationAdapter.name, GWASAssociationAdapter)
    registry.register(EnsemblAssociationAdapter.name, EnsemblAssociationAdapter)
    registry.register(ClinVarAssociationAdapter.name, ClinVarAssociationAdapter)
    return registry
