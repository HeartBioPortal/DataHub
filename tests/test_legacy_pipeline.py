import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub import MissingFieldStrategy, build_association_contract
from datahub.adapters import LegacyAssociationCsvAdapter, PhenotypeMapper
from datahub.models import CanonicalRecord
from datahub.pipeline import DataHubPipeline
from datahub.publishers import LegacyAssociationPublisher


def _write_csv(path: Path) -> None:
    rows = [
        {
            "rsid": "rs1",
            "gene": "GENE1",
            "phenotype": "cardiomyopathy",
            "var_class": "SNP",
            "clinical_significance": "Pathogenic",
            "most_severe_consequence": "missense_variant",
            "pval": "1e-8",
            "pmid": "1001",
            "ancestry_data": json.dumps({"African": {"MAF": 0.1}}),
            "functional_class": "coding",
        },
        {
            "rsid": "rs2",
            "gene": "GENE1",
            "phenotype": "cardiomyopathy",
            "var_class": "",
            "clinical_significance": "Likely benign",
            "most_severe_consequence": "intron_variant",
            "pval": "",
            "pmid": "1002",
            "ancestry_data": json.dumps({"African": {"MAF": "Unknown"}}),
            "functional_class": "intron",
        },
        {
            "rsid": "rs3",
            "gene": "GENE1",
            "phenotype": "platelet_traits",
            "var_class": "indel",
            "clinical_significance": "",
            "most_severe_consequence": "intron_variant",
            "pval": "2e-6",
            "pmid": "2001",
            "ancestry_data": json.dumps({"East Asian": {"MAF": 0.22}}),
            "functional_class": "intron",
        },
        {
            "rsid": "rs4",
            "gene": "",
            "phenotype": "cardiomyopathy",
            "var_class": "SNP",
            "clinical_significance": "Pathogenic",
            "most_severe_consequence": "missense_variant",
            "pval": "1e-5",
            "pmid": "3001",
            "ancestry_data": json.dumps({"African": {"MAF": 0.05}}),
            "functional_class": "coding",
        },
    ]

    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _mapper() -> PhenotypeMapper:
    return PhenotypeMapper(
        mapping={
            "cardiomyopathy": ("CVD", "cardiomyopathies"),
            "platelet_traits": ("TRAIT", "blood_cell_traits"),
        },
        fallback_dataset_type="CVD",
    )


def test_legacy_pipeline_emits_legacy_json(tmp_path: Path) -> None:
    csv_path = tmp_path / "legacy.csv"
    _write_csv(csv_path)

    adapter = LegacyAssociationCsvAdapter(csv_path=csv_path, phenotype_mapper=_mapper())
    contract = build_association_contract(
        dataset_type="ASSOCIATION",
        required_fields={"gene_id", "variant_id", "phenotype"},
        axis_missing_strategy=MissingFieldStrategy.EXCLUDE,
    )

    report = DataHubPipeline(
        contract=contract,
        adapters=[adapter],
        publishers=[LegacyAssociationPublisher(output_root=tmp_path / "out")],
    ).run()

    assert report.ingested_records == 3
    assert report.validated_records == 3
    assert report.dropped_records == 0

    cvd_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    trait_path = tmp_path / "out" / "association" / "final" / "association" / "TRAIT" / "GENE1.json"
    overall_path = tmp_path / "out" / "association" / "final" / "overall" / "CVD" / "GENE1.json"

    cvd_payload = json.loads(cvd_path.read_text())
    trait_payload = json.loads(trait_path.read_text())
    overall_payload = json.loads(overall_path.read_text())

    assert cvd_payload[0]["disease"] == ["cardiomyopathies", "cardiomyopathy"]
    assert {item["name"]: item["value"] for item in cvd_payload[0]["vc"]} == {"SNP": 1}

    cvd_cs = {item["name"]: item["value"] for item in cvd_payload[0]["cs"]}
    assert cvd_cs["pathogenic"] == 1
    assert cvd_cs["likely benign"] == 1

    assert trait_payload[0]["trait"] == ["blood_cell_traits", "platelet_traits"]

    assert overall_payload["data"]["vc"]["SNP"] == 1
    assert overall_payload["data"]["ancestry"]["African"]["rs1"] == 0.1


def test_missing_axis_can_be_bucketed_as_unknown(tmp_path: Path) -> None:
    csv_path = tmp_path / "legacy.csv"
    _write_csv(csv_path)

    adapter = LegacyAssociationCsvAdapter(csv_path=csv_path, phenotype_mapper=_mapper())
    contract = build_association_contract(
        dataset_type="ASSOCIATION",
        required_fields={"gene_id", "variant_id", "phenotype", "variation_type"},
        axis_missing_strategy=MissingFieldStrategy.UNKNOWN,
    )

    report = DataHubPipeline(
        contract=contract,
        adapters=[adapter],
        publishers=[
            LegacyAssociationPublisher(
                output_root=tmp_path / "out",
                skip_unknown_axis_values=False,
            )
        ],
    ).run()

    assert report.dropped_records == 0
    assert len(report.issues) >= 1

    cvd_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    cvd_payload = json.loads(cvd_path.read_text())
    vc = {item["name"]: item["value"] for item in cvd_payload[0]["vc"]}

    assert vc["SNP"] == 1
    assert vc["Unknown"] == 1


def test_publisher_deduplicates_ancestry_points_by_rsid(tmp_path: Path) -> None:
    records = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="s1",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            ancestry={"African": 0.1234567},
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="s1",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            ancestry={"African": 0.1234567},
        ),
    ]

    publisher = LegacyAssociationPublisher(
        output_root=tmp_path / "out",
        ancestry_value_precision=4,
        deduplicate_ancestry_points=True,
    )
    publisher.publish(records)

    cvd_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    payload = json.loads(cvd_path.read_text())
    ancestry_items = {
        item["name"]: item["data"]
        for item in payload[0]["ancestry"]
    }

    assert len(ancestry_items["African"]) == 1
    assert ancestry_items["African"][0]["rsid"] == "rs1"
    assert ancestry_items["African"][0]["value"] == 0.1235
