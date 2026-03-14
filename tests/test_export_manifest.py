import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.export_manifest import (  # noqa: E402
    AssociationExportManifestLoader,
    AssociationExportRuntime,
)


def test_manifest_loader_merges_source_override_and_extracts_metadata() -> None:
    loader = AssociationExportManifestLoader()
    manifest = loader.load("association/base", source_id="million_veteran_program")
    runtime = AssociationExportRuntime(manifest)

    metadata = runtime.extract_metadata(
        {
            "phenotype_key": "qt_interval",
            "source_file": "/tmp/mvp.csv.gz",
        }
    )

    assert metadata["phenotype_key"] == "qt_interval"
    assert metadata["source_file"] == "/tmp/mvp.csv.gz"
    assert metadata["provenance"]["source_family"] == "mvp"


def test_manifest_loader_rejects_unknown_helpers(tmp_path: Path) -> None:
    manifest_path = tmp_path / "broken.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_id": "broken",
                "version": 1,
                "dataset_types": ["CVD"],
                "promoted_fields": ["gene_id"],
                "metadata_fields": [],
                "derived_fields": [
                    {
                        "name": "bad_helper",
                        "helper": "does_not_exist",
                        "scope": "entry",
                        "target": "meta.bad",
                        "params": {},
                    }
                ],
                "publish_fields": {
                    "entry_meta": ["manifest_id"],
                    "overall_meta": ["manifest_version"],
                },
                "serving_fields": {"top_level_keys": ["_datahub"]},
                "source_overrides": {},
            }
        )
    )

    loader = AssociationExportManifestLoader(manifests_dir=tmp_path)
    with pytest.raises(KeyError):
        loader.load(manifest_path)
