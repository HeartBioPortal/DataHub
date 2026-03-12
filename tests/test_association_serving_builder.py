import gzip
import importlib.util
import json
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover - test environment dependent
    duckdb = None


def _load_builder_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "build_association_serving_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("association_serving_builder", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _write_json_gz(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream)


def test_builder_creates_compact_serving_db_from_published_outputs(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    module = _load_builder_module()

    output_root = tmp_path / "analyzed_data_unified"
    final_root = output_root / "association" / "final"

    assoc_cvd = [{"disease": ["arrhythmia", "atrial_fibrillation"], "vc": [], "msc": [], "cs": [], "ancestry": []}]
    assoc_trait = [{"trait": ["blood_pressure", "systolic_blood_pressure"], "vc": [], "msc": [], "cs": [], "ancestry": []}]
    overall_cvd = {"data": {"vc": {"SNP": 1}, "msc": {}, "cs": {}, "ancestry": {}}, "pvals": {}}
    overall_trait = {"data": {"vc": {}, "msc": {}, "cs": {}, "ancestry": {}}, "pvals": {}}

    _write_json_gz(final_root / "association" / "CVD" / "ANK2.json.gz", assoc_cvd)
    _write_json(final_root / "association" / "TRAIT" / "ANK2.json", assoc_trait)
    _write_json_gz(final_root / "overall" / "CVD" / "ANK2.json.gz", overall_cvd)
    _write_json(final_root / "overall" / "TRAIT" / "ANK2.json", overall_trait)

    db_path = tmp_path / "association_serving.duckdb"

    old_argv = sys.argv[:]
    try:
        sys.argv = [
            "build_association_serving_duckdb.py",
            "--input-root",
            str(output_root),
            "--db-path",
            str(db_path),
            "--include-genes",
            "ANK2",
            "--log-level",
            "ERROR",
        ]
        exit_code = module.main()
    finally:
        sys.argv = old_argv

    assert exit_code == 0
    assert db_path.exists()

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM association_gene_payloads").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM overall_gene_payloads").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM gene_catalog").fetchone()[0] == 1

        row = con.execute(
            """
SELECT dataset_type, gene_id, payload_json
FROM association_gene_payloads
WHERE dataset_type = 'CVD' AND gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert row[0] == "CVD"
        assert row[1] == "ANK2"
        assert json.loads(row[2]) == assoc_cvd

        catalog = con.execute(
            """
SELECT has_cvd, has_trait, has_cvd_association, has_trait_association, has_cvd_overall, has_trait_overall
FROM gene_catalog
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert catalog == (True, True, True, True, True, True)
    finally:
        con.close()
