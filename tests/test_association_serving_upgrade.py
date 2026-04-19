import importlib.util
import gzip
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


def _create_base_tables(db_path: Path) -> None:
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
    finally:
        con.close()


def _insert_gene(
    db_path: Path,
    gene: str,
    *,
    value: int = 1,
    source_path: Path | None = None,
) -> None:
    association_source_path = str(source_path) if source_path is not None else f"/tmp/{gene}.json.gz"
    overall_source_path = str(source_path) if source_path is not None else f"/tmp/{gene}.json.gz"
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "INSERT INTO association_gene_payloads VALUES (?, ?, ?, ?, ?)",
            [
                "CVD",
                gene,
                gene,
                json.dumps(
                    [
                        {
                            "disease": ["cardiomyopathy"],
                            "vc": {"SNP": value},
                            "msc": {"missense_variant": 2},
                            "cs": {"benign": 1},
                            "ancestry": {"European": {"rs1": 0.1}},
                        }
                    ]
                ),
                association_source_path,
            ],
        )
        con.execute(
            "INSERT INTO overall_gene_payloads VALUES (?, ?, ?, ?, ?)",
            [
                "CVD",
                gene,
                gene,
                json.dumps(
                    {
                        "data": {
                            "vc": {"SNP": value},
                            "msc": {"missense_variant": 2},
                            "cs": {"benign": 1},
                            "ancestry": {"European": {"rs1": 0.1}},
                        },
                        "pvals": {"p<5E-8": 3},
                    }
                ),
                overall_source_path,
            ],
        )
    finally:
        con.close()


def _write_gz_json(path: Path, payload: object) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _run_upgrade(module, db_path: Path, *args: str) -> int:
    old_argv = sys.argv[:]
    try:
        sys.argv = [
            "upgrade_association_serving_duckdb.py",
            "--db-path",
            str(db_path),
            "--log-level",
            "ERROR",
            *args,
        ]
        return module.main()
    finally:
        sys.argv = old_argv


def test_upgrade_adds_summary_tables_in_place(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    db_path = tmp_path / "association_serving.duckdb"
    _create_base_tables(db_path)
    _insert_gene(db_path, "TTN", value=10)

    module = _load_upgrade_module()
    assert _run_upgrade(module, db_path) == 0

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


def test_upgrade_can_resume_after_row_limited_run(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    db_path = tmp_path / "association_serving.duckdb"
    _create_base_tables(db_path)
    for gene in ("ANK2", "TTN"):
        _insert_gene(db_path, gene)

    module = _load_upgrade_module()
    assert _run_upgrade(module, db_path, "--max-rows", "1", "--batch-size", "1") == 0

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM association_summary_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM overall_summary_payloads").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM serving_summary_metadata").fetchone()[0] == 0
    finally:
        con.close()

    assert _run_upgrade(module, db_path, "--batch-size", "1") == 0

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM association_summary_payloads").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM overall_summary_payloads").fetchone()[0] == 2
        assert con.execute("SELECT COUNT(*) FROM serving_summary_metadata").fetchone()[0] == 1
    finally:
        con.close()


def test_upgrade_prefers_source_path_payloads(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    db_path = tmp_path / "association_serving.duckdb"
    payload_path = tmp_path / "TTN.json.gz"
    _write_gz_json(
        payload_path,
        [
            {
                "disease": ["cardiomyopathy"],
                "vc": {"SNP": 99},
                "msc": {"missense_variant": 3},
                "cs": {"pathogenic": 2},
                "ancestry": {"European": {"rs1": 0.1}},
            }
        ],
    )
    _create_base_tables(db_path)
    _insert_gene(db_path, "TTN", value=10, source_path=payload_path)

    module = _load_upgrade_module()
    assert _run_upgrade(module, db_path, "--tables", "association") == 0

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        association_summary = json.loads(
            con.execute("SELECT payload_json FROM association_summary_payloads").fetchone()[0]
        )
        assert association_summary == [
            {
                "disease": ["cardiomyopathy"],
                "vc": {"SNP": 99},
                "msc": {"missense_variant": 3},
                "cs": {"pathogenic": 2},
            }
        ]
    finally:
        con.close()


def test_upgrade_source_path_mode_fails_when_payload_file_missing(tmp_path: Path) -> None:
    if duckdb is None:
        import pytest

        pytest.skip("duckdb is not installed in this Python environment")

    db_path = tmp_path / "association_serving.duckdb"
    _create_base_tables(db_path)
    _insert_gene(db_path, "TTN", source_path=tmp_path / "missing.json.gz")

    module = _load_upgrade_module()
    import pytest

    with pytest.raises(FileNotFoundError):
        _run_upgrade(module, db_path, "--tables", "association", "--payload-source", "source-path")
