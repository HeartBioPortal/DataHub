import csv
import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.adapters import MVPAssociationAdapter, PhenotypeMapper  # noqa: E402


def test_mvp_adapter_merges_ancestry_rows_into_single_record(tmp_path: Path) -> None:
    csv_path = tmp_path / "mvp.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "SNP_ID",
                "chrom",
                "pos",
                "ref",
                "alt",
                "ea",
                "af",
                "num_samples",
                "case_af",
                "num_cases",
                "control_af",
                "num_controls",
                "or",
                "ci",
                "pval",
                "r2",
                "q_pval",
                "i2",
                "direction",
                "ancestry",
                "gene_symbol",
                "phenotype_key",
                "phenotype_description",
                "parent_phenotype",
                "is_sub_phenotype",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "SNP_ID": "rs573833675",
                "chrom": "1",
                "pos": "129971",
                "ref": "G",
                "alt": "GA",
                "ea": "GA",
                "af": "0.9989",
                "num_samples": "55402",
                "case_af": "0.9988",
                "num_cases": "2664",
                "control_af": "0.9989",
                "num_controls": "52738",
                "or": "0.5836",
                "ci": "0.1304,2.612",
                "pval": "0.4812",
                "r2": "0.3307",
                "q_pval": "",
                "i2": "",
                "direction": "",
                "ancestry": "AMR",
                "gene_symbol": "LOC729737",
                "phenotype_key": "Phe_427_5",
                "phenotype_description": "Arrhythmia (cardiac) NOS",
                "parent_phenotype": "cardiac_dysrhythmias",
                "is_sub_phenotype": "True",
            }
        )
        writer.writerow(
            {
                "SNP_ID": "rs573833675",
                "chrom": "1",
                "pos": "129971",
                "ref": "G",
                "alt": "GA",
                "ea": "GA",
                "af": "0.8888",
                "num_samples": "55402",
                "case_af": "0.7777",
                "num_cases": "2664",
                "control_af": "0.9990",
                "num_controls": "52738",
                "or": "0.7000",
                "ci": "0.20,3.00",
                "pval": "0.2000",
                "r2": "0.3500",
                "q_pval": "",
                "i2": "",
                "direction": "",
                "ancestry": "AFR",
                "gene_symbol": "LOC729737",
                "phenotype_key": "Phe_427_5",
                "phenotype_description": "Arrhythmia (cardiac) NOS",
                "parent_phenotype": "cardiac_dysrhythmias",
                "is_sub_phenotype": "True",
            }
        )

    mapper = PhenotypeMapper(
        mapping={
            "arrhythmia_(cardiac)_nos": ("CVD", "cardiac_dysrhythmias"),
        }
    )
    adapter = MVPAssociationAdapter(input_paths=csv_path, phenotype_mapper=mapper)
    records = list(adapter.read())

    assert len(records) == 1
    record = records[0]
    assert record.dataset_type == "CVD"
    assert record.gene_id == "LOC729737"
    assert record.variant_id == "rs573833675"
    assert record.variation_type == "INDEL"
    assert record.phenotype == "arrhythmia_(cardiac)_nos"
    assert record.disease_category == "cardiac_dysrhythmias"
    assert record.ancestry["Admixed American"] == 0.9989
    assert record.ancestry["African"] == 0.8888

    # Best summary metrics track the row with smaller p-value.
    assert record.p_value == 0.2
    assert record.metadata["best_or"] == 0.7
    assert record.metadata["best_num_cases"] == 2664
    assert record.metadata["mvp_by_ancestry"]["African"]["case_af"] == 0.7777


def test_mvp_adapter_chunk_mode_merges_records_split_on_chunk_boundary(tmp_path: Path) -> None:
    csv_path = tmp_path / "mvp.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "SNP_ID",
                "gene_symbol",
                "phenotype_description",
                "ancestry",
                "af",
                "ref",
                "alt",
                "pval",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "SNP_ID": "rs1",
                "gene_symbol": "GENE1",
                "phenotype_description": "arrhythmia",
                "ancestry": "EUR",
                "af": "0.11",
                "ref": "A",
                "alt": "G",
                "pval": "0.9",
            }
        )
        writer.writerow(
            {
                "SNP_ID": "rs1",
                "gene_symbol": "GENE1",
                "phenotype_description": "arrhythmia",
                "ancestry": "AFR",
                "af": "0.22",
                "ref": "A",
                "alt": "G",
                "pval": "0.8",
            }
        )
        writer.writerow(
            {
                "SNP_ID": "rs2",
                "gene_symbol": "GENE1",
                "phenotype_description": "arrhythmia",
                "ancestry": "EUR",
                "af": "0.33",
                "ref": "A",
                "alt": "T",
                "pval": "0.7",
            }
        )

    mapper = PhenotypeMapper(mapping={"arrhythmia": ("CVD", "cardiac_dysrhythmias")})
    adapter = MVPAssociationAdapter(
        input_paths=csv_path,
        phenotype_mapper=mapper,
        chunksize=1,
        merge_mode="chunk",
    )
    records = list(adapter.read())

    assert len(records) == 2
    by_variant = {record.variant_id: record for record in records}
    assert set(by_variant.keys()) == {"rs1", "rs2"}
    assert by_variant["rs1"].ancestry["European"] == 0.11
    assert by_variant["rs1"].ancestry["African"] == 0.22
    assert by_variant["rs2"].ancestry["European"] == 0.33
    assert by_variant["rs1"].p_value == 0.8


def test_mvp_adapter_respects_dataset_type_filter(tmp_path: Path) -> None:
    csv_path = tmp_path / "mvp.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "SNP_ID",
                "gene_symbol",
                "phenotype_description",
                "ancestry",
                "af",
                "ref",
                "alt",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "SNP_ID": "rs1",
                "gene_symbol": "GENE1",
                "phenotype_description": "arrhythmia",
                "ancestry": "EUR",
                "af": "0.1",
                "ref": "A",
                "alt": "G",
            }
        )

    mapper = PhenotypeMapper(mapping={"arrhythmia": ("CVD", "cardiac_dysrhythmias")})
    adapter = MVPAssociationAdapter(
        input_paths=csv_path,
        phenotype_mapper=mapper,
        include_dataset_types={"TRAIT"},
    )
    assert list(adapter.read()) == []


def test_mvp_adapter_emits_progress_logs(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    csv_path = tmp_path / "mvp.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "SNP_ID",
                "gene_symbol",
                "phenotype_description",
                "ancestry",
                "af",
                "ref",
                "alt",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "SNP_ID": "rs1",
                "gene_symbol": "GENE1",
                "phenotype_description": "arrhythmia",
                "ancestry": "EUR",
                "af": "0.1",
                "ref": "A",
                "alt": "G",
            }
        )

    mapper = PhenotypeMapper(mapping={"arrhythmia": ("CVD", "cardiac_dysrhythmias")})
    adapter = MVPAssociationAdapter(
        input_paths=csv_path,
        phenotype_mapper=mapper,
        log_progress=True,
        progress_every_rows=1,
    )

    with caplog.at_level(logging.INFO):
        records = list(adapter.read())

    assert records
    assert any("MVP adapter file start" in line for line in caplog.messages)
    assert any("MVP adapter progress" in line for line in caplog.messages)
    assert any("MVP adapter file complete" in line for line in caplog.messages)
