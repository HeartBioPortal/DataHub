import csv
import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.adapters import DataAdapter  # noqa: E402
from datahub.registry import (  # noqa: E402
    AdapterPluginSpec,
    AdapterRegistry,
    build_default_adapter_registry,
)


class _InlineAdapter(DataAdapter):
    name = "inline"

    def read(self):
        return []


def test_default_registry_contains_builtin_adapters() -> None:
    registry = build_default_adapter_registry()
    names = registry.available()

    assert "legacy_association_csv" in names
    assert "gwas_association" in names
    assert "ensembl_association" in names
    assert "clinvar_association" in names


def test_registry_create_builds_adapter_instance(tmp_path: Path) -> None:
    csv_path = tmp_path / "gwas.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["MarkerID", "Phenotype", "gene"])
        writer.writeheader()

    registry = build_default_adapter_registry()
    adapter = registry.create("gwas_association", input_paths=csv_path)

    assert adapter.name == "gwas_association"


def test_registry_plugin_registration(tmp_path: Path) -> None:
    module_path = tmp_path / "plugin_adapter.py"
    module_path.write_text(
        "from datahub.adapters import DataAdapter\n"
        "class CustomAdapter(DataAdapter):\n"
        "    name='custom_adapter'\n"
        "    def __init__(self, value=0):\n"
        "        self.value=value\n"
        "    def read(self):\n"
        "        return []\n"
    )

    sys.path.insert(0, str(tmp_path))
    try:
        importlib.invalidate_caches()
        registry = AdapterRegistry()
        registry.register_plugin(
            AdapterPluginSpec(
                name="custom_adapter",
                module="plugin_adapter",
                class_name="CustomAdapter",
            )
        )

        instance = registry.create("custom_adapter", value=7)
        assert getattr(instance, "value") == 7
    finally:
        sys.path = [path for path in sys.path if path != str(tmp_path)]
        sys.modules.pop("plugin_adapter", None)


def test_registry_rejects_duplicate_registration() -> None:
    registry = AdapterRegistry()
    registry.register("inline", _InlineAdapter)

    with pytest.raises(ValueError):
        registry.register("inline", _InlineAdapter)
