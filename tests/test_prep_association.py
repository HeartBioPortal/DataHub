import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.prep import (  # noqa: E402
    AssociationRawPreparer,
    RawAssociationPreparationProfileLoader,
)


def test_prep_profile_loader_lists_default_profiles() -> None:
    loader = RawAssociationPreparationProfileLoader()
    profiles = loader.list_profiles()

    assert "legacy_cvd_raw" in profiles
    assert "legacy_trait_raw" in profiles


def test_association_raw_preparer_normalizes_legacy_cvd_row(tmp_path: Path) -> None:
    input_csv = tmp_path / "cvd_raw.csv"
    output_csv = tmp_path / "cvd_prepared.csv"

    with input_csv.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "MarkerID",
                "pval",
                "Phenotype",
                "Study",
                "PMID",
                "StudyGenomeBuild",
                "dbsnp.rsid",
                "dbsnp.alleles.allele",
                "dbsnp.vartype",
                "gnomad_genome.af.af",
                "gnomad_genome.af.af_afr",
                "snpeff.ann.gene_id",
                "snpeff.ann.effect",
                "snpeff.ann.putative_impact",
                "clinvar.rcv.clinical_significance",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "MarkerID": "rs1000020884-A",
                "pval": "",
                "Phenotype": "familial dilated cardiomyopathy",
                "Study": "study_a",
                "PMID": "12345",
                "StudyGenomeBuild": "GRCh37",
                "dbsnp.rsid": "rs1000020884",
                "dbsnp.alleles.allele": "G/C",
                "dbsnp.vartype": "snp",
                "gnomad_genome.af.af": "0.0",
                "gnomad_genome.af.af_afr": "0.1",
                "snpeff.ann.gene_id": "LDB3",
                "snpeff.ann.effect": "missense_variant",
                "snpeff.ann.putative_impact": "MODERATE",
                "clinvar.rcv.clinical_significance": "uncertain significance",
            }
        )

    profile_loader = RawAssociationPreparationProfileLoader()
    profile = profile_loader.load("legacy_cvd_raw")
    report = AssociationRawPreparer(profile=profile).prepare_csv(
        input_csv=input_csv,
        output_csv=output_csv,
    )

    assert report.input_rows == 1
    assert report.output_rows == 1
    assert report.dropped_rows == 0

    with output_csv.open() as stream:
        rows = list(csv.DictReader(stream))

    assert len(rows) == 1
    row = rows[0]
    assert row["dataset_type"] == "CVD"
    assert row["rsid"] == "rs1000020884"
    assert row["gene"] == "LDB3"
    assert row["var_class"] == "SNP"
    assert row["most_severe_consequence"] == "missense_variant"

    ancestry = json.loads(row["ancestry_data"])
    assert ancestry["Total"]["MAF"] == 0.0
    assert ancestry["African"]["MAF"] == 0.1
    assert ancestry["Total"]["change"] == "G/C"
