import csv
import gzip
import json
import sqlite3
from pathlib import Path

import pytest

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None

from datahub.expression.config import ExpressionBuildConfig
from datahub.expression.curation import (
    read_curation_manifest,
    suggest_geo_curation_from_sample_metadata,
    write_curation_manifest,
)
from datahub.expression.geo_discovery import (
    discover_geo_cvd_candidates,
    discover_geo_cvd_candidates_from_phenotype_tree,
    download_geometadb_sqlite,
    load_cvd_terms_from_phenotype_tree,
)
from datahub.expression.legacy_cardioquilt import read_cardioquilt_csv
from datahub.expression.pipeline import build_expression_outputs
from datahub.expression.v3_results import read_expression_v3_results


def _write_cardioquilt_csv(path: Path) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "gse_id",
                "diseases_associated",
                "pmid",
                "Gene.symbol",
                "logFC",
                "regulation",
                "adj.P.Val",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "gse_id": "GSE1",
                "diseases_associated": "cardiomyopathy",
                "pmid": "123",
                "Gene.symbol": "ANK2",
                "logFC": "1.5",
                "regulation": "up",
                "adj.P.Val": "0.001",
            }
        )
        writer.writerow(
            {
                "gse_id": "GSE2",
                "diseases_associated": "cardiomyopathy",
                "pmid": "456",
                "Gene.symbol": "ANK2",
                "logFC": "-2.0",
                "regulation": "down",
                "adj.P.Val": "0.02",
            }
        )
        writer.writerow(
            {
                "gse_id": "GSE3",
                "diseases_associated": "hypertension",
                "pmid": "789",
                "Gene.symbol": "TTN",
                "logFC": "0.8",
                "regulation": "up",
                "adj.P.Val": "0.2",
            }
        )


def test_cardioquilt_import_and_expression_outputs(tmp_path: Path) -> None:
    source_csv = tmp_path / "cardioquilt_CREEDS_GEO.csv"
    _write_cardioquilt_csv(source_csv)
    config = ExpressionBuildConfig(adjusted_p_value_threshold=0.05)

    rows = read_cardioquilt_csv(source_csv, config=config)
    assert len(rows) == 2
    assert {row.direction for row in rows} == {"up", "down"}
    assert rows[0].source_url == "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE1"

    manifest = build_expression_outputs(
        rows=rows,
        output_root=tmp_path / "out",
        config=config,
    )

    assert manifest["row_count"] == 2
    assert manifest["summary_row_count"] == 1
    legacy_path = Path(manifest["outputs"]["legacy_compatible_expression_json"])
    legacy_payload = json.loads(legacy_path.read_text())
    assert legacy_payload == {
        "ANK2": {
            "cardiomyopathy": {
                "upregulated": 1,
                "downregulated": 1,
            }
        }
    }


def test_expression_outputs_duckdb(tmp_path: Path) -> None:
    if duckdb is None:
        pytest.skip("duckdb is not installed")

    source_csv = tmp_path / "cardioquilt_CREEDS_GEO.csv"
    _write_cardioquilt_csv(source_csv)
    rows = read_cardioquilt_csv(source_csv)
    db_path = tmp_path / "expression.duckdb"

    build_expression_outputs(
        rows=rows,
        output_root=tmp_path / "out",
        duckdb_path=db_path,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        assert con.execute("SELECT COUNT(*) FROM expression_differential_results").fetchone()[0] == 2
        summary = con.execute(
            """
SELECT number_of_studies_upregulated, number_of_studies_downregulated
FROM expression_gene_phenotype_summary
WHERE gene_id = 'ANK2'
"""
        ).fetchone()
        assert summary == (1, 1)
    finally:
        con.close()


def test_geo_discovery_from_local_geometadb(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "GEOmetadb.sqlite"
    con = sqlite3.connect(sqlite_path)
    try:
        con.execute("CREATE TABLE gse (gse TEXT, title TEXT, summary TEXT, pubmed_id TEXT, overall_design TEXT)")
        con.execute("CREATE TABLE gse_gpl (gse TEXT, gpl TEXT)")
        con.execute("CREATE TABLE gpl (gpl TEXT, organism TEXT)")
        con.execute(
            "INSERT INTO gse VALUES (?, ?, ?, ?, ?)",
            ("GSE42", "heart failure expression", "case control", "123", "disease vs control"),
        )
        con.execute("INSERT INTO gse_gpl VALUES (?, ?)", ("GSE42", "GPL1"))
        con.execute("INSERT INTO gpl VALUES (?, ?)", ("GPL1", "Homo sapiens"))
        con.commit()
    finally:
        con.close()

    candidates = discover_geo_cvd_candidates(sqlite_path, terms=["heart failure"])

    assert len(candidates) == 1
    assert candidates[0].study_accession == "GSE42"
    assert candidates[0].matched_term == "heart failure"


def test_geo_discovery_uses_hbp_phenotype_tree_paths(tmp_path: Path) -> None:
    tree_path = tmp_path / "phenotype_tree.json"
    tree_path.write_text(
        json.dumps(
            {
                "CVD": {
                    "coronary artery diseases": [
                        "Angina",
                        "Myocardial infarction",
                    ]
                }
            }
        )
    )
    terms = load_cvd_terms_from_phenotype_tree(tree_path)
    assert terms["Angina"] == "CVD/coronary_artery_diseases/angina"

    sqlite_path = tmp_path / "GEOmetadb.sqlite"
    con = sqlite3.connect(sqlite_path)
    try:
        con.execute("CREATE TABLE gse (gse TEXT, title TEXT, summary TEXT, pubmed_id TEXT, overall_design TEXT)")
        con.execute("CREATE TABLE gse_gpl (gse TEXT, gpl TEXT)")
        con.execute("CREATE TABLE gpl (gpl TEXT, organism TEXT)")
        con.execute(
            "INSERT INTO gse VALUES (?, ?, ?, ?, ?)",
            ("GSE105449", "angina monocyte expression", "case control", "123", "CVD vs control"),
        )
        con.execute("INSERT INTO gse_gpl VALUES (?, ?)", ("GSE105449", "GPL1"))
        con.execute("INSERT INTO gpl VALUES (?, ?)", ("GPL1", "Homo sapiens"))
        con.commit()
    finally:
        con.close()

    candidates = discover_geo_cvd_candidates_from_phenotype_tree(sqlite_path, tree_path)

    assert len(candidates) == 1
    assert candidates[0].matched_term == "Angina"
    assert candidates[0].phenotype_tree_path == "CVD/coronary_artery_diseases/angina"


def test_expression_v3_curation_manifest_from_candidates(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "GEOmetadb.sqlite"
    con = sqlite3.connect(sqlite_path)
    try:
        con.execute("CREATE TABLE gse (gse TEXT, title TEXT, summary TEXT, pubmed_id TEXT, overall_design TEXT)")
        con.execute("CREATE TABLE gse_gpl (gse TEXT, gpl TEXT)")
        con.execute("CREATE TABLE gpl (gpl TEXT, organism TEXT)")
        con.execute(
            "INSERT INTO gse VALUES (?, ?, ?, ?, ?)",
            ("GSE99", "dilated cardiomyopathy expression", "case control", "321", "DCM vs donor"),
        )
        con.execute("INSERT INTO gse_gpl VALUES (?, ?)", ("GSE99", "GPL570"))
        con.execute("INSERT INTO gpl VALUES (?, ?)", ("GPL570", "Homo sapiens"))
        con.commit()
    finally:
        con.close()

    candidates = discover_geo_cvd_candidates(sqlite_path, terms=["dilated cardiomyopathy"])
    manifest_path = write_curation_manifest(
        candidates=candidates,
        output_csv=tmp_path / "expression_v3_curation.csv",
    )
    rows = read_curation_manifest(manifest_path)

    assert rows[0]["approved"] == "false"
    assert rows[0]["review_status"] == "needs_review"
    assert rows[0]["study_accession"] == "GSE99"
    assert "phenotype_tree_path" in rows[0]
    assert rows[0]["phenotype_label_normalized"] == "dilated_cardiomyopathy"
    assert rows[0]["analysis_method"] == "geoquery_limma"


def _write_sample_metadata(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_geo_curation_suggestion_detects_case_control_and_non_mrna_assay(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    with manifest.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "approved",
                "review_status",
                "study_accession",
                "source_database",
                "source_url",
                "matched_term",
                "phenotype_tree_path",
                "disease_name",
                "disease_id",
                "phenotype_label_original",
                "phenotype_label_normalized",
                "assay_type",
                "platform",
                "species",
                "tissue",
                "cell_type",
                "case_group_label",
                "control_group_label",
                "case_sample_accessions",
                "control_sample_accessions",
                "n_case",
                "n_control",
                "contrast_name",
                "analysis_method",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "approved": "false",
                "review_status": "needs_review",
                "study_accession": "GSE144431",
                "source_database": "GEO",
                "source_url": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE144431",
                "matched_term": "abdominal aortic aneurysm",
                "phenotype_tree_path": "CVD/aneurysm/abdominal_aortic_aneurysm",
                "disease_name": "abdominal aortic aneurysm",
                "phenotype_label_original": "abdominal aortic aneurysm",
                "phenotype_label_normalized": "abdominal_aortic_aneurysm",
                "assay_type": "microarray_or_processed_matrix",
                "contrast_name": "case_vs_control",
                "analysis_method": "geoquery_limma",
            }
        )
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    _write_sample_metadata(
        metadata_dir / "GSE144431_samples.csv",
        [
            {
                "geo_accession": "GSM1",
                "title": "Abdominal aortic aneurysm sample 1",
                "description": "AAA 1",
                "source_name_ch1": "full thickness abdominal aortic aneurysm tissue",
                "tissue:ch1": "abdominal aorta",
                "hyb_protocol": "Arraystar Human circRNA Arrays V2",
            },
            {
                "geo_accession": "GSM2",
                "title": "Abdominal aortic aneurysm sample 2",
                "description": "AAA 2",
                "source_name_ch1": "full thickness abdominal aortic aneurysm tissue",
                "tissue:ch1": "abdominal aorta",
                "hyb_protocol": "Arraystar Human circRNA Arrays V2",
            },
            {
                "geo_accession": "GSM3",
                "title": "Donor sample 1",
                "description": "Control 1",
                "source_name_ch1": "abdominal aorta",
                "tissue:ch1": "abdominal aorta",
                "hyb_protocol": "Arraystar Human circRNA Arrays V2",
            },
            {
                "geo_accession": "GSM4",
                "title": "Donor sample 2",
                "description": "Control 2",
                "source_name_ch1": "abdominal aorta",
                "tissue:ch1": "abdominal aorta",
                "hyb_protocol": "Arraystar Human circRNA Arrays V2",
            },
        ],
    )

    payload = suggest_geo_curation_from_sample_metadata(
        curation_csv=manifest,
        metadata_dir=metadata_dir,
        output_csv=tmp_path / "suggested.csv",
    )
    rows = read_curation_manifest(payload["output_csv"])

    assert payload["status_counts"] == {"suggested_non_mrna_case_control": 1}
    assert rows[0]["approved"] == "false"
    assert rows[0]["review_status"] == "suggested_non_mrna_case_control"
    assert rows[0]["assay_type"] == "circRNA_microarray"
    assert rows[0]["n_case"] == "2"
    assert rows[0]["n_control"] == "2"
    assert rows[0]["case_sample_accessions"] == "GSM1,GSM2"
    assert rows[0]["control_sample_accessions"] == "GSM3,GSM4"


def test_geo_curation_suggestion_keeps_disease_only_studies_unapproved(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    with manifest.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "approved",
                "review_status",
                "study_accession",
                "source_database",
                "source_url",
                "matched_term",
                "phenotype_tree_path",
                "disease_name",
                "disease_id",
                "phenotype_label_original",
                "phenotype_label_normalized",
                "assay_type",
                "platform",
                "species",
                "tissue",
                "cell_type",
                "case_group_label",
                "control_group_label",
                "case_sample_accessions",
                "control_sample_accessions",
                "n_case",
                "n_control",
                "contrast_name",
                "analysis_method",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "approved": "false",
                "review_status": "needs_review",
                "study_accession": "GSE165470",
                "source_database": "GEO",
                "source_url": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE165470",
                "matched_term": "abdominal aortic aneurysm",
                "phenotype_tree_path": "CVD/aneurysm/abdominal_aortic_aneurysm",
                "disease_name": "abdominal aortic aneurysm",
                "phenotype_label_original": "abdominal aortic aneurysm",
                "phenotype_label_normalized": "abdominal_aortic_aneurysm",
                "assay_type": "microarray_or_processed_matrix",
                "contrast_name": "case_vs_control",
                "analysis_method": "geoquery_limma",
            }
        )
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    _write_sample_metadata(
        metadata_dir / "GSE165470_samples.csv",
        [
            {
                "geo_accession": "GSM1",
                "source_name_ch1": "Human abdominal aortic aneurysm tissue",
                "tissue:ch1": "abdominal aortic aneurysm (AAA) tissue",
                "hyb_protocol": "Affymetrix Clariom D Array",
            },
            {
                "geo_accession": "GSM2",
                "source_name_ch1": "Human abdominal aortic aneurysm tissue",
                "tissue:ch1": "abdominal aortic aneurysm (AAA) tissue",
                "hyb_protocol": "Affymetrix Clariom D Array",
            },
        ],
    )

    payload = suggest_geo_curation_from_sample_metadata(
        curation_csv=manifest,
        metadata_dir=metadata_dir,
        output_csv=tmp_path / "suggested.csv",
    )
    rows = read_curation_manifest(payload["output_csv"])

    assert payload["status_counts"] == {"needs_review_no_control": 1}
    assert rows[0]["approved"] == "false"
    assert rows[0]["review_status"] == "needs_review_no_control"
    assert rows[0]["n_case"] == "2"
    assert rows[0]["n_control"] == ""


def test_geo_curation_suggestion_flags_possible_topic_mismatch(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.csv"
    with manifest.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "approved",
                "review_status",
                "study_accession",
                "source_database",
                "source_url",
                "matched_term",
                "phenotype_tree_path",
                "disease_name",
                "disease_id",
                "phenotype_label_original",
                "phenotype_label_normalized",
                "assay_type",
                "platform",
                "species",
                "tissue",
                "cell_type",
                "case_group_label",
                "control_group_label",
                "case_sample_accessions",
                "control_sample_accessions",
                "n_case",
                "n_control",
                "contrast_name",
                "analysis_method",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "approved": "false",
                "review_status": "needs_review",
                "study_accession": "GSE59216",
                "source_database": "GEO",
                "source_url": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE59216",
                "matched_term": "stroke",
                "phenotype_tree_path": "CVD/cerebrovascular/stroke",
                "disease_name": "stroke",
                "phenotype_label_original": "stroke",
                "phenotype_label_normalized": "stroke",
                "assay_type": "microarray_or_processed_matrix",
                "contrast_name": "case_vs_control",
                "analysis_method": "geoquery_limma",
            }
        )
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    _write_sample_metadata(
        metadata_dir / "GSE59216_samples.csv",
        [
            {
                "geo_accession": "GSM1",
                "diagnosis:ch1": "patient",
                "asd status:ch1": "ASD",
                "tissue:ch1": "whole blood",
                "data_processing": "Contrast analysis was performed by LIMMA.",
            },
            {
                "geo_accession": "GSM2",
                "diagnosis:ch1": "patient",
                "asd status:ch1": "ASD",
                "tissue:ch1": "whole blood",
                "data_processing": "Contrast analysis was performed by LIMMA.",
            },
            {
                "geo_accession": "GSM3",
                "diagnosis:ch1": "healthy control",
                "asd status:ch1": "non",
                "tissue:ch1": "whole blood",
                "data_processing": "Contrast analysis was performed by LIMMA.",
            },
            {
                "geo_accession": "GSM4",
                "diagnosis:ch1": "healthy control",
                "asd status:ch1": "non",
                "tissue:ch1": "whole blood",
                "data_processing": "Contrast analysis was performed by LIMMA.",
            },
        ],
    )

    payload = suggest_geo_curation_from_sample_metadata(
        curation_csv=manifest,
        metadata_dir=metadata_dir,
        output_csv=tmp_path / "suggested.csv",
    )
    rows = read_curation_manifest(payload["output_csv"])

    assert payload["status_counts"] == {"needs_review_possible_topic_mismatch": 1}
    assert rows[0]["approved"] == "false"
    assert rows[0]["review_status"] == "needs_review_possible_topic_mismatch"
    assert rows[0]["n_case"] == "2"
    assert rows[0]["n_control"] == "2"


def test_import_expression_v3_results_counts_not_significant(tmp_path: Path) -> None:
    results_csv = tmp_path / "geo_limma_results.csv"
    with results_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "gene_id",
                "gene_symbol",
                "study_accession",
                "source_database",
                "disease_name",
                "phenotype_label_normalized",
                "log2_fold_change",
                "p_value",
                "adjusted_p_value",
                "direction",
                "analysis_method",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "gene_id": "ANK2",
                "gene_symbol": "ANK2",
                "study_accession": "GSE10",
                "source_database": "GEO",
                "disease_name": "heart failure",
                "phenotype_label_normalized": "heart_failure",
                "log2_fold_change": "1.2",
                "p_value": "0.0005",
                "adjusted_p_value": "0.01",
                "direction": "up",
                "analysis_method": "GEOquery_limma",
            }
        )
        writer.writerow(
            {
                "gene_id": "ANK2",
                "gene_symbol": "ANK2",
                "study_accession": "GSE11",
                "source_database": "GEO",
                "disease_name": "heart failure",
                "phenotype_label_normalized": "heart_failure",
                "log2_fold_change": "0.1",
                "p_value": "0.5",
                "adjusted_p_value": "0.8",
                "direction": "not_significant",
                "analysis_method": "GEOquery_limma",
            }
        )

    config = ExpressionBuildConfig(
        analysis_method="expression_v3_public_geo",
        pipeline_name="datahub_expression_v3",
        pipeline_version="3",
        source_database="GEO",
    )
    rows = read_expression_v3_results(results_csv, config=config)
    manifest = build_expression_outputs(rows=rows, output_root=tmp_path / "out", config=config)

    serving = json.loads(Path(manifest["outputs"]["serving_summary_json"]).read_text())
    assert serving["ANK2"]["heart_failure"] == {
        "up": 1,
        "down": 0,
        "not_significant": 1,
        "total": 2,
    }


def test_download_geometadb_sqlite_from_gzip_url(tmp_path: Path) -> None:
    sqlite_bytes = b"SQLite format 3\000test fixture"
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    gzip_path = source_dir / "GEOmetadb.sqlite.gz"
    with gzip.open(gzip_path, "wb") as handle:
        handle.write(sqlite_bytes)

    output = download_geometadb_sqlite(
        output_path=tmp_path / "GEOmetadb.sqlite",
        url=gzip_path.as_uri(),
    )

    assert output.read_bytes() == sqlite_bytes
