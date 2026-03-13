import importlib.util
import sys
from pathlib import Path


def _load_publish_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "publish_unified_from_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("unified_publish_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_unified_publish_records_preserve_source_ancestry_metadata() -> None:
    module = _load_publish_module()

    row = (
        "hbp_mvp_association",
        "CVD",
        "million_veteran_program",
        "ANK2",
        "rs1",
        "arrhythmia_(cardiac)_nos",
        "cardiac_dysrhythmias",
        "SNP",
        None,
        None,
        0.123,
        "African",
        0.456,
        "AFR",
        "African",
        "Phe_427_5",
        "/tmp/source.csv.gz",
    )

    record = module._new_record(row)

    assert record.ancestry == {"African": 0.456}
    assert record.metadata["phenotype_key"] == "Phe_427_5"
    assert record.metadata["source_file"] == "/tmp/source.csv.gz"
    assert record.metadata["source_ancestry"] == {
        "AFR": {
            "source_ancestry_code": "AFR",
            "source_ancestry_label": "African",
            "canonical_ancestry_group": "African",
            "af": 0.456,
        }
    }
