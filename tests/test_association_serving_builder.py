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
    expression_path = tmp_path / "expression.json"
    sga_root = tmp_path / "SGA"
    tree_path = tmp_path / "phenotype_tree.json"
    tree_path.write_text(
        json.dumps({"CVD": {"cardiac_dysrhythmias": ["atrial_fibrillation"]}})
    )

    assoc_cvd = [
        {
            "disease": ["", "atrial_fibrillation"],
            "vc": [
                {"name": "indel", "value": 2},
                {"name": "INDEL", "value": 1},
            ],
            "msc": [
                {"name": "Missense_Variant", "value": 2},
                {"name": "missense_variant", "value": 1},
            ],
            "cs": [
                {"name": "['benign', 'benign', 'likely benign']", "value": 2},
                {"name": "likely benign", "value": 1},
            ],
            "ancestry": [],
            "_datahub": {
                "manifest_id": "association_export_v1",
                "manifest_version": 1,
                "provenance": {
                    "sources": ["legacy_cvd_raw", "million_veteran_program"],
                    "source_counts": {
                        "legacy_cvd_raw": 2,
                        "million_veteran_program": 1,
                    },
                    "source_families": ["legacy_raw", "mvp"],
                    "source_file_count": 2,
                },
            },
        }
    ]
    assoc_trait = [{"trait": ["blood_pressure", "systolic_blood_pressure"], "vc": [], "msc": [], "cs": [], "ancestry": []}]
    overall_cvd = {
        "data": {
            "vc": {"indel": 2, "INDEL": 1},
            "msc": {"Missense_Variant": 2, "missense_variant": 1},
            "cs": {
                "['benign', 'benign', 'likely benign']": 2,
                "likely benign": 1,
            },
            "ancestry": {},
        },
        "pvals": {},
        "_datahub": {
            "manifest_id": "association_export_v1",
            "manifest_version": 1,
        },
    }
    overall_trait = {"data": {"vc": {}, "msc": {}, "cs": {}, "ancestry": {}}, "pvals": {}}

    _write_json_gz(final_root / "association" / "CVD" / "ANK2.json.gz", assoc_cvd)
    _write_json(final_root / "association" / "TRAIT" / "ANK2.json", assoc_trait)
    _write_json_gz(final_root / "overall" / "CVD" / "ANK2.json.gz", overall_cvd)
    _write_json(final_root / "overall" / "TRAIT" / "ANK2.json", overall_trait)
    expression_path.write_text(
        json.dumps(
            {
                "ANK2": {
                    "cardiomyopathy": {
                        "upregulated": 2,
                        "downregulated": 1,
                    }
                }
            }
        )
    )
    _write_json(
        sga_root / "cvd" / "angina.json",
        {"ANK2": {"rs1": [1.0, 2.0]}},
    )
    _write_json(
        sga_root / "trait" / "QT_interval.json",
        {"ANK2": {"rs2": [3.0, 4.0]}},
    )

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
            "--phenotype-tree-json",
            str(tree_path),
            "--expression-json-path",
            str(expression_path),
            "--sga-root",
            str(sga_root),
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
        assert con.execute("SELECT COUNT(*) FROM expression_gene_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM sga_gene_payloads").fetchone()[0] == 1
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
        assert json.loads(row[2]) == [
            {
                "disease": ["cardiac_dysrhythmias", "atrial_fibrillation"],
                "vc": [{"name": "INDEL", "value": 3}],
                "msc": [{"name": "missense variant", "value": 3}],
                "cs": [{"name": "likely benign", "value": 3}],
                "ancestry": [],
                "_datahub": {
                    "manifest_id": "association_export_v1",
                    "manifest_version": 1,
                    "provenance": {
                        "sources": ["legacy_cvd_raw", "million_veteran_program"],
                        "source_counts": {
                            "legacy_cvd_raw": 2,
                            "million_veteran_program": 1,
                        },
                        "source_families": ["legacy_raw", "mvp"],
                        "source_file_count": 2,
                    },
                },
            }
        ]

        overall_row = con.execute(
            """
SELECT payload_json
FROM overall_gene_payloads
WHERE dataset_type = 'CVD' AND gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert json.loads(overall_row[0])["data"]["vc"] == {"INDEL": 3}
        assert json.loads(overall_row[0])["data"]["msc"] == {"missense variant": 3}
        assert json.loads(overall_row[0])["data"]["cs"] == {"likely benign": 3}
        assert json.loads(overall_row[0])["_datahub"] == {
            "manifest_id": "association_export_v1",
            "manifest_version": 1,
        }

        catalog = con.execute(
            """
SELECT has_cvd, has_trait, has_cvd_association, has_trait_association, has_cvd_overall, has_trait_overall, has_expression, has_sga
FROM gene_catalog
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert catalog == (True, True, True, True, True, True, True, True)

        expression_row = con.execute(
            """
SELECT payload_json
FROM expression_gene_payloads
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert json.loads(expression_row[0]) == {
            "cardiomyopathy": {"up": 2, "down": 1}
        }

        sga_row = con.execute(
            """
SELECT payload_json
FROM sga_gene_payloads
WHERE gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        assert json.loads(sga_row[0]) == [
            {"gene": "ANK2", "data": {"rs1": [1.0, 2.0]}, "type": "cvd", "name": "angina"},
            {"gene": "ANK2", "data": {"rs2": [3.0, 4.0]}, "type": "trait", "name": "QT_interval"},
        ]

        build_meta = con.execute(
            "SELECT export_manifest_id, export_manifest_version FROM build_metadata"
        ).fetchone()
        assert build_meta == ("association_export_v1", 1)
    finally:
        con.close()


def test_builder_can_trust_already_canonical_published_payloads(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    module = _load_builder_module()

    output_root = tmp_path / "analyzed_data_unified"
    final_root = output_root / "association" / "final"
    assoc_cvd = [
        {
            "disease": ["cardiac_dysrhythmias", "atrial_fibrillation"],
            "vc": [{"name": "INDEL", "value": 3}],
            "msc": [{"name": "missense variant", "value": 3}],
            "cs": [{"name": "likely benign", "value": 3}],
            "ancestry": [],
            "_datahub": {
                "manifest_id": "association_export_v1",
                "manifest_version": 1,
            },
        }
    ]
    overall_cvd = {
        "data": {
            "vc": {"INDEL": 3},
            "msc": {"missense variant": 3},
            "cs": {"likely benign": 3},
            "ancestry": {},
        },
        "pvals": {},
        "_datahub": {
            "manifest_id": "association_export_v1",
            "manifest_version": 1,
        },
    }

    _write_json_gz(final_root / "association" / "CVD" / "ANK2.json.gz", assoc_cvd)
    _write_json_gz(final_root / "overall" / "CVD" / "ANK2.json.gz", overall_cvd)

    db_path = tmp_path / "association_serving_fast.duckdb"

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
            "--trust-published-payloads",
            "--log-level",
            "ERROR",
        ]
        exit_code = module.main()
    finally:
        sys.argv = old_argv

    assert exit_code == 0

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assoc_row = con.execute(
            """
SELECT payload_json
FROM association_gene_payloads
WHERE dataset_type = 'CVD' AND gene_id_normalized = 'ANK2'
"""
        ).fetchone()
        overall_row = con.execute(
            """
SELECT payload_json
FROM overall_gene_payloads
WHERE dataset_type = 'CVD' AND gene_id_normalized = 'ANK2'
"""
        ).fetchone()

        assert json.loads(assoc_row[0]) == assoc_cvd
        assert json.loads(overall_row[0]) == overall_cvd
    finally:
        con.close()
