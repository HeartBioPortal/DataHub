import csv
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.registry import build_default_adapter_registry  # noqa: E402
from datahub.sources import (  # noqa: E402
    GWASSourceConnector,
    SourceManifestLoader,
    build_default_source_registry,
)


def test_source_manifest_loader_lists_expected_default_manifests() -> None:
    loader = SourceManifestLoader()
    available = loader.list_sources()

    assert "gwas_catalog" in available
    assert "ensembl_variation" in available
    assert "clinvar" in available


def test_source_registry_creates_adapter_from_source_manifest(tmp_path: Path) -> None:
    csv_path = tmp_path / "gwas.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["MarkerID", "Phenotype", "gene"])
        writer.writeheader()

    manifest_loader = SourceManifestLoader()
    source_registry = build_default_source_registry(manifest_loader)
    adapter_registry = build_default_adapter_registry()

    adapter = source_registry.create_adapter(
        "gwas_catalog",
        adapter_registry=adapter_registry,
        params={
            "input_paths": str(csv_path),
            "dataset_id": "test_dataset",
        },
    )

    assert adapter.name == "gwas_association"
    assert getattr(adapter, "dataset_id") == "test_dataset"


def test_gwas_connector_rejects_wrong_adapter_manifest() -> None:
    loader = SourceManifestLoader()
    wrong_manifest = loader.load("clinvar")

    with pytest.raises(ValueError):
        GWASSourceConnector(wrong_manifest)
