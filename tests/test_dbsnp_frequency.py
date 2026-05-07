from __future__ import annotations

import io
import tarfile
from pathlib import Path

import duckdb

from datahub.secondary_analyses.dbsnp_frequency import (
    build_dbsnp_frequency_index,
    parse_frequency_member,
)


FREQUENCY_TEXT = """#URL\thttps://www.ncbi.nlm.nih.gov/snp/rs123/download/frequency
#NCBI Reference SNP (rs) Report ALPHA\trs123
#Current Build\t157
#Released\tNovember 1, 2024
#Organism\tHomo sapiens
#Position\tchr1:101 (GRCh38.p14)
#Alleles\tT>C
#Variation Type\tSNV (Single Nucleotide Variation)
#Frequency Data Table
#################
#Study\tPopulation\tGroup\tSamplesize\tRef Allele\tAlt Allele\tBioProject ID\tBioSample ID
TopMed\tGlobal\tStudy-wide\t264690\tT=0.999811\tC=0.000189\tPRJNA400167\t
gnomAD v4 - Genomes\tAfrican/African American\tAFR\t1000\tT=0.9\tC=0.1\tPRJNA398795\tSAMN1
"""


def test_parse_frequency_member_preserves_metadata_and_rows() -> None:
    records = parse_frequency_member(
        text=FREQUENCY_TEXT,
        source_archive="dbsnp_frequency_data_batch1.tar.gz",
        source_member="batch1/rs123_frequency.csv",
    )

    assert len(records) == 2
    first = records[0]
    assert first.rsid == "rs123"
    assert first.study == "TopMed"
    assert first.population == "Global"
    assert first.sample_size == 264690
    assert first.ref_allele == "T"
    assert first.ref_frequency == 0.999811
    assert first.alt_allele == "C"
    assert first.alt_frequency == 0.000189
    assert first.ncbi_build == "157"
    assert first.position == "chr1:101 (GRCh38.p14)"
    assert first.variation_type == "SNV (Single Nucleotide Variation)"


def test_build_dbsnp_frequency_index_keeps_new_and_legacy_rows(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw_data" / "dbsnp"
    raw_root.mkdir(parents=True)
    _write_archive(raw_root / "dbsnp_frequency_data_batch1.tar.gz", "batch1/rs123_frequency.csv", FREQUENCY_TEXT)

    legacy_root = tmp_path / "analyzed_data" / "dbSNP"
    legacy_csv_root = legacy_root / "csvs"
    legacy_csv_root.mkdir(parents=True)
    (legacy_csv_root / "rs123.csv").write_text(
        "Study,Population,Group,Sample Size,Ref Allele,Alt Allele,rsid\n"
        "LegacyStudy,LegacyPopulation,LegacyGroup,10,T=0.8,C=0.2,rs123\n"
        "LegacyStudy,LegacyPopulation,LegacyGroup,10,T=0.8,C=0.2,rs123\n",
        encoding="utf-8",
    )

    output_db = tmp_path / "datamart" / "dbsnp_frequency.duckdb"
    summary = build_dbsnp_frequency_index(
        raw_root=raw_root,
        output_db=output_db,
        legacy_dbsnp_root=legacy_root,
        include_legacy=True,
        progress=False,
    )

    assert summary.rows_loaded == 3
    assert summary.distinct_rsids == 1
    assert (tmp_path / "datamart" / "dbsnp_frequency.checkpoint.json").exists()

    connection = duckdb.connect(str(output_db), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT source_system, study, population, alt_frequency
            FROM dbsnp_frequency_records
            ORDER BY source_system, study
            """
        ).fetchall()
        assert rows == [
            ("hbp_legacy_dbsnp", "LegacyStudy", "LegacyPopulation", 0.2),
            ("ncbi_dbsnp_frequency", "TopMed", "Global", 0.000189),
            ("ncbi_dbsnp_frequency", "gnomAD v4 - Genomes", "African/African American", 0.1),
        ]

        rsid_summary = connection.execute(
            "SELECT row_count, study_count, population_count, max_sample_size FROM dbsnp_frequency_rsid_summary"
        ).fetchone()
        assert rsid_summary == (3, 3, 3, 264690)

        population_summary = connection.execute(
            """
            SELECT max_sample_size, min_alt_frequency, mean_alt_frequency, max_alt_frequency
            FROM dbsnp_frequency_population_summary
            WHERE study = 'LegacyStudy'
            """
        ).fetchone()
        assert population_summary == (10, 0.2, 0.2, 0.2)
    finally:
        connection.close()

    resumed_summary = build_dbsnp_frequency_index(
        raw_root=raw_root,
        output_db=output_db,
        legacy_dbsnp_root=legacy_root,
        include_legacy=True,
        progress=False,
    )
    assert resumed_summary.rows_loaded == 3


def _write_archive(path: Path, member_name: str, content: str) -> None:
    data = content.encode("utf-8")
    info = tarfile.TarInfo(member_name)
    info.size = len(data)
    with tarfile.open(path, "w:gz") as tar:
        tar.addfile(info, io.BytesIO(data))
