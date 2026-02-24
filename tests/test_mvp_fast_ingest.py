import csv
import json
import subprocess
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")


def _write_mvp_csv(path: Path, *, rsid: str, ancestry: str, af: float, pval: float) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "SNP_ID",
                "gene_symbol",
                "phenotype_description",
                "parent_phenotype",
                "ancestry",
                "af",
                "ref",
                "alt",
                "pval",
                "phenotype_key",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "SNP_ID": rsid,
                "gene_symbol": "GENE1",
                "phenotype_description": "Arrhythmia (cardiac) NOS",
                "parent_phenotype": "cardiac_dysrhythmias",
                "ancestry": ancestry,
                "af": str(af),
                "ref": "A",
                "alt": "G",
                "pval": str(pval),
                "phenotype_key": "Phe_427_5",
            }
        )


def _run_fast_ingest(repo_root: Path, args: list[str]) -> dict[str, object]:
    result = subprocess.run(
        ["python3", "scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_fast_ingest_duckdb_supports_resume(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    file_one = tmp_path / "mvp_1.csv"
    file_two = tmp_path / "mvp_2.csv"
    db_path = tmp_path / "mvp_fast.duckdb"
    checkpoint_path = tmp_path / "mvp_fast_checkpoint.json"
    map_path = tmp_path / "phenotype_map.json"

    _write_mvp_csv(file_one, rsid="rs1", ancestry="EUR", af=0.1, pval=0.01)
    _write_mvp_csv(file_two, rsid="rs2", ancestry="AFR", af=0.2, pval=0.02)

    map_path.write_text(
        json.dumps(
            {
                "arrhythmia_(cardiac)_nos": {
                    "dataset_type": "CVD",
                    "category": "cardiac_dysrhythmias",
                }
            }
        )
    )

    base_args = [
        "--db-path",
        str(db_path),
        "--checkpoint-path",
        str(checkpoint_path),
        "--phenotype-map-json",
        str(map_path),
        "--table-name",
        "mvp_points_test",
        "--log-level",
        "WARNING",
    ]

    first = _run_fast_ingest(
        repo_root,
        args=[
            "--input-path",
            str(file_one),
            "--input-path",
            str(file_two),
            *base_args,
        ],
    )
    assert first["processed_files"] == 2
    assert first["skipped_files"] == 0
    assert first["rows_inserted"] == 2

    second = _run_fast_ingest(
        repo_root,
        args=[
            "--input-path",
            str(file_one),
            "--input-path",
            str(file_two),
            *base_args,
        ],
    )
    assert second["processed_files"] == 0
    assert second["skipped_files"] == 2
    assert second["rows_inserted"] == 0

    con = duckdb.connect(str(db_path))
    try:
        row_count = con.execute("SELECT COUNT(*) FROM mvp_points_test").fetchone()[0]
        assert row_count == 2

        rows = con.execute(
            "SELECT dataset_type, disease_category, variation_type, ancestry, ancestry_af "
            "FROM mvp_points_test ORDER BY variant_id"
        ).fetchall()
    finally:
        con.close()

    assert rows[0][0] == "CVD"
    assert rows[0][1] == "cardiac_dysrhythmias"
    assert rows[0][2] == "SNP"
    assert rows[0][3] == "European"
    assert rows[0][4] == pytest.approx(0.1)
