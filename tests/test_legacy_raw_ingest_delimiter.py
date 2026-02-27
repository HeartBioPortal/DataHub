import importlib.util
import sys
from pathlib import Path


def _load_legacy_ingest_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "ingest_legacy_raw_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("legacy_ingest_script", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_resolve_csv_delimiter_auto_detects_tab(tmp_path: Path) -> None:
    module = _load_legacy_ingest_module()
    sample = tmp_path / "trait.txt"
    sample.write_text("col1\tcol2\tcol3\n1\t2\t3\n")
    assert module._resolve_csv_delimiter("auto", sample) == "tab"


def test_resolve_csv_delimiter_auto_detects_comma(tmp_path: Path) -> None:
    module = _load_legacy_ingest_module()
    sample = tmp_path / "cvd.txt"
    sample.write_text("col1,col2,col3\n1,2,3\n")
    assert module._resolve_csv_delimiter("auto", sample) == "comma"
