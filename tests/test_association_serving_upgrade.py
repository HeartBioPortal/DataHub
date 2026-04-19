import importlib.util
import json
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover - test environment dependent
    duckdb = None


def _load_upgrade_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "upgrade_association_serving_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("association_serving_upgrade", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_upgrade_adds_summary_tables_in_place(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    db_path = tmp_path / "association_serving.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
CREATE TABLE association_gene_payloads (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
        )
        con.execute(
            """
CREATE TABLE overall_gene_payloads (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
        )
        con.execute(
            "INSERT INTO association_gene_payloads VALUES (?, ?, ?, ?, ?)",
            [
                "CVD",
                "TTN",
                "TTN",
                json.dumps(
                    [
                        {
                            "disease": ["cardiomyopathy"],
                            "vc": {"SNP": 10},
                            "msc": {"missense_variant": 2},
                            "cs": {"benign": 1},
                            "ancestry": {"European": {"rs1": 0.1}},
                        }
                    ]
                ),
                "/tmp/TTN.json.gz",
            ],
        )
        con.execute(
            "INSERT INTO overall_gene_payloads VALUES (?, ?, ?, ?, ?)",
            [
                "CVD",
                "TTN",
                "TTN",
                json.dumps(
                    {
                        "data": {
                            "vc": {"SNP": 10},
                            "msc": {"missense_variant": 2},
                            "cs": {"benign": 1},
                            "ancestry": {"European": {"rs1": 0.1}},
                        },
                        "pvals": {"p<5E-8": 3},
                    }
                ),
                "/tmp/TTN.json.gz",
            ],
        )
    finally:
        con.close()

    module = _load_upgrade_module()
    old_argv = sys.argv[:]
    try:
        sys.argv = [
            "upgrade_association_serving_duckdb.py",
            "--db-path",
            str(db_path),
            "--log-level",
            "ERROR",
        ]
        exit_code = module.main()
    finally:
        sys.argv = old_argv

    assert exit_code == 0

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM association_gene_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM association_summary_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM overall_summary_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM serving_summary_metadata").fetchone()[0] == 1

        association_summary = json.loads(
            con.execute("SELECT payload_json FROM association_summary_payloads").fetchone()[0]
        )
        assert association_summary == [
            {
                "disease": ["cardiomyopathy"],
                "vc": {"SNP": 10},
                "msc": {"missense_variant": 2},
                "cs": {"benign": 1},
            }
        ]

        overall_summary = json.loads(
            con.execute("SELECT payload_json FROM overall_summary_payloads").fetchone()[0]
        )
        assert overall_summary == {
            "data": {
                "vc": {"SNP": 10},
                "msc": {"missense_variant": 2},
                "cs": {"benign": 1},
            },
            "pvals": {"p<5E-8": 3},
        }
    finally:
        con.close()
