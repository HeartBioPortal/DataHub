import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.adapters import (  # noqa: E402
    ClinVarAssociationAdapter,
    EnsemblAssociationAdapter,
    GWASAssociationAdapter,
    PhenotypeMapper,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _mapper() -> PhenotypeMapper:
    return PhenotypeMapper(
        mapping={
            "cardiomyopathy": ("CVD", "cardiomyopathies"),
            "qrs_interval": ("TRAIT", "electrocardiogram_(ecg)_measures"),
            "familial_cardiomyopathy": ("CVD", "cardiomyopathies"),
        }
    )


def test_gwas_adapter_maps_expected_fields(tmp_path: Path) -> None:
    path = tmp_path / "gwas.csv"
    _write_csv(
        path,
        [
            {
                "MarkerID": "rs100-A",
                "pval": "1e-8",
                "Phenotype": "cardiomyopathy",
                "Study": "GCST9001",
                "PMID": "12345",
                "gene": "MYH7",
                "functional_class": "coding",
            },
            {
                "MarkerID": "rs200-C",
                "pval": "2e-6",
                "Phenotype": "cardiomyopathy",
                "Study": "GCST9002",
                "PMID": "67890",
                "gene": "",
                "functional_class": "intergenic",
            },
        ],
    )

    adapter = GWASAssociationAdapter(input_paths=path, phenotype_mapper=_mapper())
    records = list(adapter.read())

    assert len(records) == 1
    record = records[0]
    assert record.gene_id == "MYH7"
    assert record.variant_id == "rs100"
    assert record.phenotype == "cardiomyopathy"
    assert record.dataset_type == "CVD"
    assert record.metadata["study"] == "GCST9001"


def test_ensembl_adapter_supports_population_columns_and_type_inference(tmp_path: Path) -> None:
    path = tmp_path / "TRAITS" / "qrs_interval.csv"
    _write_csv(
        path,
        [
            {
                "diseases_associated": "qrs interval",
                "pmid": "98765",
                "Gene.symbol": "SCN5A",
                "rsID": "rs999",
                "var_class": "SNP",
                "clinical_significance": "Likely benign",
                "most_severe_consequence": "missense_variant",
                "adj.P.Val": "4e-9",
                "African": "0.11",
                "Total": "0.19",
            }
        ],
    )

    adapter = EnsemblAssociationAdapter(input_paths=path, phenotype_mapper=_mapper())
    records = list(adapter.read())

    assert len(records) == 1
    record = records[0]
    assert record.dataset_type == "TRAIT"
    assert record.gene_id == "SCN5A"
    assert record.variation_type == "SNP"
    assert record.clinical_significance == "Likely benign"
    assert record.ancestry["African"] == "0.11"


def test_clinvar_adapter_supports_disease_and_structural_variant_shapes(tmp_path: Path) -> None:
    disease_path = tmp_path / "DISEASE" / "familial_cardiomyopathy.csv"
    sv_path = tmp_path / "SV_ClinVar.csv"

    _write_csv(
        disease_path,
        [
            {
                "rsID": "123",
                "phenotype": "familial_cardiomyopathy",
                "primary_gene": "TNNT2",
                "mutation": "missense_variant",
                "amino_acid": "R/H",
                "pmid": "112233",
            }
        ],
    )

    _write_csv(
        sv_path,
        [
            {
                "Variant ID": "456",
                "Subject Phenotype": "familial_cardiomyopathy",
                "Gene Name": "TTN",
                "Clinical Interpretation": "Pathogenic",
                "Variant Call type": "deletion",
            }
        ],
    )

    adapter = ClinVarAssociationAdapter(
        input_paths=[disease_path, sv_path],
        phenotype_mapper=_mapper(),
    )
    records = list(adapter.read())

    assert len(records) == 2

    first = next(record for record in records if record.gene_id == "TNNT2")
    second = next(record for record in records if record.gene_id == "TTN")

    assert first.dataset_type == "CVD"
    assert first.variant_id == "123"
    assert first.metadata["mutation"] == "missense_variant"

    assert second.clinical_significance == "Pathogenic"
    assert second.variation_type == "deletion"
