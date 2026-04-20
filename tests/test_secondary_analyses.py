import gzip
import importlib.util
import json
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:  # pragma: no cover - test environment dependent
    duckdb = None


def _load_script_module(script_name: str):
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / script_name
    )
    spec = importlib.util.spec_from_file_location(script_name.replace(".py", ""), module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _create_base_serving_db(path: Path) -> None:
    con = duckdb.connect(str(path))
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
            """
CREATE TABLE gene_catalog (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    has_cvd BOOLEAN,
    has_trait BOOLEAN,
    has_cvd_association BOOLEAN,
    has_trait_association BOOLEAN,
    has_cvd_overall BOOLEAN,
    has_trait_overall BOOLEAN,
    has_expression BOOLEAN,
    has_sga BOOLEAN
)
"""
        )
        con.execute(
            """
INSERT INTO association_gene_payloads VALUES
('CVD', 'ANK2', 'ANK2', '[]', '/tmp/assoc/ANK2.json.gz')
"""
        )
        con.execute(
            """
INSERT INTO overall_gene_payloads VALUES
('CVD', 'ANK2', 'ANK2', '{"data":{"vc":{},"msc":{},"cs":{},"ancestry":{}},"pvals":{}}', '/tmp/overall/ANK2.json.gz')
"""
        )
        con.execute(
            """
INSERT INTO gene_catalog VALUES
('ANK2', 'ANK2', true, false, true, false, true, false, false, false)
"""
        )
    finally:
        con.close()


def test_expression_secondary_generate_and_apply(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    module = _load_script_module("run_secondary_analyses.py")
    secondary_root = tmp_path / "secondary"
    expression_path = tmp_path / "expression.json"
    expression_path.write_text(
        json.dumps(
            {
                "ANK2": {"cardiomyopathy": {"upregulated": 2, "downregulated": 1}},
                "TTN": {"heart_failure": {"upregulated": 4, "downregulated": 0}},
                "THRA1/BTR": {"arrhythmia": {"upregulated": 1, "downregulated": 0}},
            }
        )
    )
    serving_db = tmp_path / "association_serving.duckdb"
    _create_base_serving_db(serving_db)

    old_argv = sys.argv[:]
    try:
        sys.argv = [
            "run_secondary_analyses.py",
            "generate",
            "--analyses",
            "expression",
            "--output-root",
            str(secondary_root),
            "--expression-json-path",
            str(expression_path),
            "--log-level",
            "ERROR",
        ]
        assert module.main() == 0

        sys.argv = [
            "run_secondary_analyses.py",
            "apply",
            "--analyses",
            "expression",
            "--secondary-root",
            str(secondary_root),
            "--serving-db-path",
            str(serving_db),
            "--log-level",
            "ERROR",
        ]
        assert module.main() == 0
    finally:
        sys.argv = old_argv

    artifact_path = secondary_root / "final" / "expression" / "genes" / "ANK2.json.gz"
    with gzip.open(artifact_path, "rt", encoding="utf-8") as stream:
        assert json.loads(stream.read()) == {"cardiomyopathy": {"up": 2, "down": 1}}

    con = duckdb.connect(str(serving_db), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM expression_gene_payloads").fetchone()[0] == 3
        assert con.execute("SELECT COUNT(*) FROM secondary_analysis_metadata WHERE analysis_id = 'expression'").fetchone()[0] == 1

        ank2 = con.execute(
            """
SELECT has_expression, has_sga
FROM gene_catalog
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert ank2 == (True, False)

        ttn = con.execute(
            """
SELECT has_cvd_association, has_trait_association, has_expression
FROM gene_catalog
WHERE gene_id_normalized = 'TTN'
"""
        ).fetchone()
        assert ttn == (False, False, True)

        slash_gene = con.execute(
            """
SELECT gene_id, gene_id_normalized, has_expression
FROM gene_catalog
WHERE gene_id_normalized = 'THRA1/BTR'
"""
        ).fetchone()
        assert slash_gene == ("THRA1/BTR", "THRA1/BTR", True)
    finally:
        con.close()

    slash_artifact_path = secondary_root / "final" / "expression" / "genes" / "THRA1%2FBTR.json.gz"
    with gzip.open(slash_artifact_path, "rt", encoding="utf-8") as stream:
        assert json.loads(stream.read()) == {"arrhythmia": {"up": 1, "down": 0}}


def test_sga_secondary_generate_and_apply(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    module = _load_script_module("run_secondary_analyses.py")
    association_db = tmp_path / "points.duckdb"
    con = duckdb.connect(str(association_db))
    try:
        con.execute(
            """
CREATE TABLE mvp_association_points (
    dataset_type VARCHAR,
    gene_id VARCHAR,
    variant_id VARCHAR,
    phenotype VARCHAR
)
"""
        )
        con.executemany(
            "INSERT INTO mvp_association_points VALUES (?, ?, ?, ?)",
            [
                ("CVD", "ANK2", "rs1", "angina"),
                ("CVD", "ANK2", "rs2", "angina"),
                ("CVD", "ANK2", "rs3", "atrial_fibrillation"),
                ("TRAIT", "ANK2", "rs2", "blood_pressure"),
                ("TRAIT", "ANK2", "rs3", "blood_pressure"),
                ("TRAIT", "ANK2", "rs3", "qt_interval"),
                ("TRAIT", "TTN", "rs9", "qt_interval"),
            ],
        )
    finally:
        con.close()

    secondary_root = tmp_path / "secondary"
    serving_db = tmp_path / "association_serving.duckdb"
    _create_base_serving_db(serving_db)

    old_argv = sys.argv[:]
    try:
        for partition_index in range(2):
            sys.argv = [
                "run_secondary_analyses.py",
                "generate",
                "--analyses",
                "sga",
                "--output-root",
                str(secondary_root),
                "--association-db-path",
                str(association_db),
                "--association-table",
                "mvp_association_points",
                "--unit-partitions",
                "2",
                "--unit-partition-index",
                str(partition_index),
                "--log-level",
                "ERROR",
            ]
            assert module.main() == 0

        sys.argv = [
            "run_secondary_analyses.py",
            "apply",
            "--analyses",
            "sga",
            "--secondary-root",
            str(secondary_root),
            "--serving-db-path",
            str(serving_db),
            "--log-level",
            "ERROR",
        ]
        assert module.main() == 0
    finally:
        sys.argv = old_argv

    artifact_path = secondary_root / "final" / "sga" / "genes" / "ANK2.json.gz"
    assert len(list((secondary_root / "final" / "sga").glob("metadata.part*of*.json"))) == 2
    with gzip.open(artifact_path, "rt", encoding="utf-8") as stream:
        payload = json.loads(stream.read())
    assert [item["name"] for item in payload] == [
        "angina",
        "atrial_fibrillation",
        "blood_pressure",
        "qt_interval",
    ]
    assert payload[0]["_datahub"]["encoding"] == "rsid_identity_interval"
    assert sorted(payload[0]["data"].keys()) == ["rs2"]
    assert sorted(payload[1]["data"].keys()) == ["rs3"]

    con = duckdb.connect(str(serving_db), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM sga_gene_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM secondary_analysis_metadata WHERE analysis_id = 'sga'").fetchone()[0] == 1

        row = con.execute(
            """
SELECT payload_json
FROM sga_gene_payloads
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        loaded = json.loads(row[0])
        assert loaded[2]["name"] == "blood_pressure"
        assert sorted(loaded[2]["data"].keys()) == ["rs2", "rs3"]

        catalog = con.execute(
            """
SELECT has_cvd_association, has_expression, has_sga
FROM gene_catalog
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert catalog == (True, False, True)
    finally:
        con.close()
