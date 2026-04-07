import csv
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.prep import PREPARED_ASSOCIATION_COLUMNS
from datahub.working_duckdb import (
    compare_schema,
    compare_schema_candidate_groups,
    ensure_working_schema,
    load_source_normalized_association_csv,
    materialize_analysis_ready_association_from_points,
    register_raw_release,
)


duckdb = pytest.importorskip("duckdb")


def test_schema_drift_reports_added_and_missing_columns() -> None:
    added = compare_schema(
        detected_columns=["gene", "rsid", "pval", "extra"],
        expected_columns=["gene", "rsid", "pval"],
    )
    assert added.status == "compatible_with_additions"
    assert added.added_columns == ("extra",)
    assert not added.removed_columns

    missing = compare_schema(
        detected_columns=["gene", "rsid"],
        expected_columns=["gene", "rsid", "pval"],
    )
    assert missing.status == "breaking"
    assert missing.removed_columns == ("pval",)


def test_schema_drift_candidate_groups_accept_alternative_columns() -> None:
    drift = compare_schema_candidate_groups(
        detected_columns=["MarkerID", "gene", "phenotype"],
        expected_column_groups=[("dbsnp.rsid", "MarkerID"), ("gene",), ("phenotype",)],
    )
    assert drift.status == "compatible"

    missing = compare_schema_candidate_groups(
        detected_columns=["gene", "phenotype"],
        expected_column_groups=[("dbsnp.rsid", "MarkerID"), ("gene",), ("phenotype",)],
    )
    assert missing.status == "breaking"
    assert missing.removed_columns == ("one_of(dbsnp.rsid|MarkerID)",)


def test_register_raw_release_inventories_files_and_drift(tmp_path: Path) -> None:
    raw_path = tmp_path / "legacy.csv"
    raw_path.write_text("gene,rsid,pval,extra\nTTN,rs1,0.01,keep\n")
    removed_path = tmp_path / "removed.csv"
    removed_path.write_text("gene,rsid,pval\nMYH7,rs2,0.02\n")
    db_path = tmp_path / "working.duckdb"

    con = duckdb.connect(str(db_path))
    try:
        register_raw_release(
            con,
            source_id="legacy_cvd_raw",
            release_id="legacy_v1",
            modality="association",
            input_paths=[raw_path, removed_path],
            expected_columns=["gene", "rsid", "pval"],
            compute_checksum=False,
        )
        registrations = register_raw_release(
            con,
            source_id="legacy_cvd_raw",
            release_id="legacy_v1",
            modality="association",
            input_paths=[raw_path],
            expected_columns=["gene", "rsid", "pval"],
            compute_checksum=False,
        )
        assert len(registrations) == 1
        assert registrations[0].drift.status == "compatible_with_additions"

        assert con.execute("select count(*) from raw_file_inventory").fetchone()[0] == 1
        assert con.execute("select count(*) from schema_drift_reports").fetchone()[0] == 1
        inventory = con.execute(
            "select source_id, release_id, columns_json, sha256 from raw_file_inventory"
        ).fetchone()
        assert inventory[0] == "legacy_cvd_raw"
        assert inventory[1] == "legacy_v1"
        assert json.loads(inventory[2]) == ["gene", "rsid", "pval", "extra"]
        assert inventory[3] is None

        drift = con.execute("select status, added_columns_json from schema_drift_reports").fetchone()
        assert drift[0] == "compatible_with_additions"
        assert json.loads(drift[1]) == ["extra"]
    finally:
        con.close()


def test_load_source_normalized_association_csv(tmp_path: Path) -> None:
    prepared_path = tmp_path / "prepared.csv"
    with prepared_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=PREPARED_ASSOCIATION_COLUMNS)
        writer.writeheader()
        writer.writerow(
            {
                "dataset_type": "CVD",
                "source_name": "legacy_cvd_gwas",
                "marker_id": "rs1-A-G",
                "study_id": "study-1",
                "study": "Example GWAS",
                "pmid": "123",
                "study_genome_build": "GRCh37",
                "rsid": "rs1",
                "pval": "0.001",
                "gene": "TTN",
                "phenotype": "Cardiomyopathy",
                "functional_class": "MODERATE",
                "var_class": "SNP",
                "clinical_significance": "Benign",
                "most_severe_consequence": "missense_variant",
                "allele_string": "A/G",
                "protein_start": "",
                "protein_end": "",
                "ancestry_data": '{"European":{"MAF":0.1,"change":"A/G"}}',
            }
        )

    con = duckdb.connect(str(tmp_path / "working.duckdb"))
    try:
        inserted = load_source_normalized_association_csv(
            con,
            prepared_csv=prepared_path,
            source_id="legacy_cvd_raw",
            release_id="legacy_v1",
        )
        assert inserted == 1
        row = con.execute(
            "select source_id, release_id, variant_id, gene_id, p_value from source_normalized_association"
        ).fetchone()
        assert row == ("legacy_cvd_raw", "legacy_v1", "rs1", "TTN", 0.001)
    finally:
        con.close()


def test_materialize_analysis_ready_association_from_existing_points_table(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "working.duckdb"))
    try:
        ensure_working_schema(con)
        con.execute(
            """
CREATE TABLE mvp_association_points (
    dataset_id VARCHAR,
    dataset_type VARCHAR,
    source VARCHAR,
    gene_id VARCHAR,
    variant_id VARCHAR,
    phenotype VARCHAR,
    disease_category VARCHAR,
    variation_type VARCHAR,
    clinical_significance VARCHAR,
    most_severe_consequence VARCHAR,
    p_value DOUBLE,
    ancestry VARCHAR,
    ancestry_af DOUBLE,
    ancestry_source_code VARCHAR,
    ancestry_source_label VARCHAR,
    phenotype_key VARCHAR,
    source_file VARCHAR,
    ingested_at TIMESTAMP
)
"""
        )
        con.execute(
            """
INSERT INTO mvp_association_points VALUES
('hbp_mvp_association', 'CVD', 'million_veteran_program', 'Ttn', 'rs1',
 'atrial_fibrillation', 'arrhythmia', 'SNP', NULL, NULL, 1e-8,
 'European', 0.2, 'EUR', 'European', 'phe_1', '/tmp/mvp.csv.gz', now())
"""
        )
        inserted = materialize_analysis_ready_association_from_points(
            con,
            source_table="mvp_association_points",
            replace=True,
        )
        assert inserted == 1
        row = con.execute(
            "select source_id, gene_id, gene_id_normalized, variant_id from analysis_ready_association"
        ).fetchone()
        assert row == ("million_veteran_program", "Ttn", "TTN", "rs1")
    finally:
        con.close()
