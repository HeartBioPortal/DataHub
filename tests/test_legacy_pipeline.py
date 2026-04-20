import csv
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub import MissingFieldStrategy, build_association_contract
from datahub.adapters import LegacyAssociationCsvAdapter, PhenotypeMapper
from datahub.export_manifest import AssociationExportManifestCatalog
from datahub.models import CanonicalRecord
from datahub.pipeline import DataHubPipeline
from datahub.phenotype_paths import PhenotypePathResolver
from datahub.publishers import (
    LegacyAssociationPublisher,
    VariantIndexPublisher,
    VariantIndexStreamWriter,
)


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
        {
            "rsid": "rs_bad",
            "gene": "0.799091",
            "phenotype": "cardiomyopathy",
            "var_class": "SNP",
            "clinical_significance": "Pathogenic",
            "most_severe_consequence": "missense_variant",
            "pval": "8e-6",
            "pmid": "3002",
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
    invalid_gene_path = (
        tmp_path / "out" / "association" / "final" / "association" / "CVD" / "0.799091.json"
    )

    assert cvd_payload[0]["disease"] == ["cardiomyopathies", "cardiomyopathy"]
    assert {item["name"]: item["value"] for item in cvd_payload[0]["vc"]} == {"SNP": 1}

    cvd_cs = {item["name"]: item["value"] for item in cvd_payload[0]["cs"]}
    assert cvd_cs["pathogenic"] == 1
    assert cvd_cs["likely benign"] == 1

    assert trait_payload[0]["trait"] == ["blood_cell_traits", "platelet_traits"]

    assert overall_payload["data"]["vc"]["SNP"] == 1
    assert overall_payload["data"]["ancestry"]["African"]["rs1"] == 0.1
    assert not invalid_gene_path.exists()


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


def test_publisher_normalizes_list_like_clinical_significance(tmp_path: Path) -> None:
    records = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            clinical_significance="['benign', 'benign', 'likely benign']",
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            clinical_significance="benign",
        ),
    ]

    publisher = LegacyAssociationPublisher(output_root=tmp_path / "out")
    publisher.publish(records)

    cvd_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    overall_path = tmp_path / "out" / "association" / "final" / "overall" / "CVD" / "GENE1.json"

    association_payload = json.loads(cvd_path.read_text())
    overall_payload = json.loads(overall_path.read_text())

    cs = {item["name"]: item["value"] for item in association_payload[0]["cs"]}
    assert cs == {"benign": 1, "likely benign": 1}
    assert overall_payload["data"]["cs"] == {"benign": 1, "likely benign": 1}


def test_publisher_normalizes_variation_and_consequence_axes(tmp_path: Path) -> None:
    records = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="indel",
            most_severe_consequence="Missense_Variant",
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="INDEL",
            most_severe_consequence="missense_variant",
        ),
    ]

    publisher = LegacyAssociationPublisher(output_root=tmp_path / "out")
    publisher.publish(records)

    cvd_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    overall_path = tmp_path / "out" / "association" / "final" / "overall" / "CVD" / "GENE1.json"

    association_payload = json.loads(cvd_path.read_text())
    overall_payload = json.loads(overall_path.read_text())

    vc = {item["name"]: item["value"] for item in association_payload[0]["vc"]}
    msc = {item["name"]: item["value"] for item in association_payload[0]["msc"]}

    assert vc == {"INDEL": 2}
    assert msc == {"missense variant": 2}
    assert overall_payload["data"]["vc"] == {"INDEL": 2}
    assert overall_payload["data"]["msc"] == {"missense variant": 2}


def test_publisher_deduplicates_variant_counts_within_and_across_phenotypes(tmp_path: Path) -> None:
    records = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            p_value=1e-8,
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            p_value=1e-6,
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="arrhythmia",
            disease_category="cardiac_dysrhythmias",
            variation_type="SNP",
            p_value=1e-5,
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="arrhythmia",
            disease_category="cardiac_dysrhythmias",
            variation_type="INDEL",
            p_value=1e-4,
        ),
    ]

    publisher = LegacyAssociationPublisher(output_root=tmp_path / "out")
    publisher.publish(records)

    association_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    overall_path = tmp_path / "out" / "association" / "final" / "overall" / "CVD" / "GENE1.json"

    association_payload = json.loads(association_path.read_text())
    overall_payload = json.loads(overall_path.read_text())

    by_phenotype = {
        tuple(entry["disease"]): {item["name"]: item["value"] for item in entry["vc"]}
        for entry in association_payload
    }

    assert by_phenotype[("cardiomyopathies", "cardiomyopathy")] == {"SNP": 1}
    assert by_phenotype[("cardiac_dysrhythmias", "arrhythmia")] == {"INDEL": 1, "SNP": 1}
    assert overall_payload["data"]["vc"] == {"INDEL": 1, "SNP": 1}


def _aggregate_variant_index_vc(
    payload: list[dict[str, object]],
    *,
    allowed_paths: set[tuple[str, ...]] | None = None,
) -> dict[str, int]:
    by_variant: dict[str, dict[str, object]] = {}
    for entry in payload:
        phenotype_path = tuple(str(item) for item in entry["phenotype_path"])  # type: ignore[index]
        if allowed_paths is not None and phenotype_path not in allowed_paths:
            continue
        variant_id = str(entry["variant_id"])
        existing = by_variant.get(variant_id)
        if existing is None:
            by_variant[variant_id] = entry
            continue
        existing_p = existing.get("p_value")
        candidate_p = entry.get("p_value")
        if existing_p is None or (
            candidate_p is not None and float(candidate_p) < float(existing_p)
        ):
            by_variant[variant_id] = entry

    counts: dict[str, int] = {}
    for entry in by_variant.values():
        variation_type = entry.get("variation_type")
        if variation_type is not None:
            counts[str(variation_type)] = counts.get(str(variation_type), 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[0]))


def test_variant_index_preserves_variant_identity_for_filtered_aggregation(tmp_path: Path) -> None:
    records = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            most_severe_consequence="Missense_Variant",
            clinical_significance="['benign', 'likely benign']",
            p_value=1e-8,
            ancestry={"African": 0.1234567},
            metadata={"source_file": "/tmp/cvd.tsv", "phenotype_key": "cardiomyopathy"},
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            p_value=1e-6,
            ancestry={"European": 0.2},
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="arrhythmia",
            disease_category="cardiac_dysrhythmias",
            variation_type="SNP",
            p_value=1e-5,
        ),
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="arrhythmia",
            disease_category="cardiac_dysrhythmias",
            variation_type="indel",
            p_value=1e-4,
        ),
    ]

    out = tmp_path / "out"
    LegacyAssociationPublisher(output_root=out).publish(records)
    VariantIndexPublisher(
        output_root=out,
        ancestry_value_precision=4,
    ).publish(records)

    overall_path = out / "association" / "final" / "overall" / "CVD" / "GENE1.json"
    index_path = out / "association" / "final" / "variant_index" / "CVD" / "GENE1.json"

    overall_payload = json.loads(overall_path.read_text())
    variant_index_payload = json.loads(index_path.read_text())

    assert len(variant_index_payload) == 3
    assert _aggregate_variant_index_vc(variant_index_payload) == overall_payload["data"]["vc"]
    assert _aggregate_variant_index_vc(
        variant_index_payload,
        allowed_paths={("cardiomyopathies", "cardiomyopathy")},
    ) == {"SNP": 1}
    assert _aggregate_variant_index_vc(
        variant_index_payload,
        allowed_paths={("cardiac_dysrhythmias", "arrhythmia")},
    ) == {"INDEL": 1, "SNP": 1}

    cardiomyopathy = next(
        entry
        for entry in variant_index_payload
        if entry["variant_id"] == "rs1"
        and entry["phenotype_path"] == ["cardiomyopathies", "cardiomyopathy"]
    )
    assert cardiomyopathy["variation_type"] == "SNP"
    assert cardiomyopathy["most_severe_consequence"] == "missense variant"
    assert cardiomyopathy["clinical_significance"] == "likely benign"
    assert cardiomyopathy["ancestry"] == {"African": 0.1235, "European": 0.2}
    assert cardiomyopathy["metadata"]["source_files"] == ["/tmp/cvd.tsv"]


def test_variant_index_supports_gzip_output(tmp_path: Path) -> None:
    publisher = VariantIndexPublisher(
        output_root=tmp_path / "out",
        json_compression="gzip",
        json_indent=None,
    )
    publisher.publish(
        [
            CanonicalRecord(
                dataset_id="d1",
                dataset_type="TRAIT",
                source="legacy",
                gene_id="GENE1",
                variant_id="rs1",
                phenotype="platelet_traits",
                disease_category="blood_cell_traits",
                variation_type="SNP",
            )
        ]
    )

    index_path = (
        tmp_path
        / "out"
        / "association"
        / "final"
        / "variant_index"
        / "TRAIT"
        / "GENE1.json.gz"
    )
    assert index_path.exists()


def test_variant_index_stream_writer_writes_valid_gene_payloads(tmp_path: Path) -> None:
    writer = VariantIndexStreamWriter(
        output_root=tmp_path / "out",
        json_compression="gzip",
        json_indent=None,
    )
    writer.write_record(
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
        )
    )
    writer.write_record(
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="indel",
        )
    )
    writer.write_record(
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="TRAIT",
            source="legacy",
            gene_id="GENE2",
            variant_id="rs3",
            phenotype="platelet_traits",
            disease_category="blood_cell_traits",
            variation_type="SNP",
        )
    )
    writer.close()

    cvd_path = (
        tmp_path
        / "out"
        / "association"
        / "final"
        / "variant_index"
        / "CVD"
        / "GENE1.json.gz"
    )
    trait_path = (
        tmp_path
        / "out"
        / "association"
        / "final"
        / "variant_index"
        / "TRAIT"
        / "GENE2.json.gz"
    )

    with gzip.open(cvd_path, "rt", encoding="utf-8") as stream:
        cvd_payload = json.load(stream)
    with gzip.open(trait_path, "rt", encoding="utf-8") as stream:
        trait_payload = json.load(stream)

    assert [entry["variant_id"] for entry in cvd_payload] == ["rs1", "rs2"]
    assert cvd_payload[1]["variation_type"] == "INDEL"
    assert trait_payload[0]["trait"] == ["blood_cell_traits", "platelet_traits"]


def test_publisher_restores_canonical_path_from_tree_when_category_missing(tmp_path: Path) -> None:
    tree_path = tmp_path / "phenotype_tree.json"
    tree_path.write_text(
        json.dumps({"CVD": {"cardiac_dysrhythmias": ["atrial_fibrillation"]}})
    )

    publisher = LegacyAssociationPublisher(
        output_root=tmp_path / "out",
        tree_json_path=tree_path,
    )
    publisher.publish(
        [
            CanonicalRecord(
                dataset_id="d1",
                dataset_type="CVD",
                source="legacy",
                gene_id="GENE1",
                variant_id="rs1",
                phenotype="atrial_fibrillation",
                disease_category="",
            )
        ]
    )

    cvd_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    association_payload = json.loads(cvd_path.read_text())

    assert association_payload[0]["disease"] == [
        "cardiac_dysrhythmias",
        "atrial_fibrillation",
    ]


def test_publisher_emits_manifest_driven_datahub_metadata(tmp_path: Path) -> None:
    tree_path = tmp_path / "phenotype_tree.json"
    tree_path.write_text(
        json.dumps({"CVD": {"cardiac_dysrhythmias": ["atrial_fibrillation"]}})
    )

    manifest_catalog = AssociationExportManifestCatalog(
        base_manifest_ref="association/base",
        path_resolver=PhenotypePathResolver.from_tree_json(tree_path),
    )

    records = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="legacy_cvd_raw",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="atrial_fibrillation",
            disease_category="",
            clinical_significance="['benign', 'likely benign']",
            ancestry={"African": 0.1},
            metadata={
                "source_file": "/tmp/cvd.csv",
                "provenance": {"source_family": "legacy_raw"},
                "source_ancestry": {
                    "AFR": {
                        "source_ancestry_code": "AFR",
                        "source_ancestry_label": "African",
                        "canonical_ancestry_group": "African",
                        "af": 0.1,
                    }
                },
            },
        ),
        CanonicalRecord(
            dataset_id="d2",
            dataset_type="CVD",
            source="million_veteran_program",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="atrial_fibrillation",
            disease_category="",
            clinical_significance="pathogenic",
            ancestry={"European": 0.2},
            metadata={
                "source_file": "/tmp/mvp.csv.gz",
                "provenance": {"source_family": "mvp"},
                "source_ancestry": {
                    "EUR": {
                        "source_ancestry_code": "EUR",
                        "source_ancestry_label": "European",
                        "canonical_ancestry_group": "European",
                        "af": 0.2,
                    }
                },
            },
        ),
    ]

    publisher = LegacyAssociationPublisher(
        output_root=tmp_path / "out",
        tree_json_path=tree_path,
        export_runtime=manifest_catalog.base_runtime,
    )
    publisher.publish(records)

    association_path = tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    overall_path = tmp_path / "out" / "association" / "final" / "overall" / "CVD" / "GENE1.json"

    association_payload = json.loads(association_path.read_text())
    overall_payload = json.loads(overall_path.read_text())

    entry_meta = association_payload[0]["_datahub"]
    assert entry_meta["manifest_id"] == "association_export_v1"
    assert entry_meta["manifest_version"] == 1
    assert entry_meta["provenance"]["sources"] == [
        "legacy_cvd_raw",
        "million_veteran_program",
    ]
    assert entry_meta["provenance"]["source_counts"] == {
        "legacy_cvd_raw": 1,
        "million_veteran_program": 1,
    }
    assert entry_meta["provenance"]["source_families"] == ["legacy_raw", "mvp"]
    assert entry_meta["ancestry_provenance"]["source_codes"] == ["AFR", "EUR"]
    assert entry_meta["coverage"]["status"] == "not_computed"

    overall_meta = overall_payload["_datahub"]
    assert overall_meta["manifest_id"] == "association_export_v1"
    assert overall_meta["provenance"]["source_file_count"] == 2


def test_incremental_merge_publisher_appends_new_variants_across_batches(tmp_path: Path) -> None:
    publisher = LegacyAssociationPublisher(
        output_root=tmp_path / "out",
        incremental_merge=True,
        deduplicate_ancestry_points=True,
    )

    batch_one = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="s1",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            ancestry={"African": 0.1},
        )
    ]
    batch_two = [
        CanonicalRecord(
            dataset_id="d1",
            dataset_type="CVD",
            source="s1",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="cardiomyopathy",
            disease_category="cardiomyopathies",
            variation_type="SNP",
            ancestry={"African": 0.2},
        )
    ]

    publisher.publish(batch_one)
    publisher.publish(batch_two)

    association_path = (
        tmp_path / "out" / "association" / "final" / "association" / "CVD" / "GENE1.json"
    )
    overall_path = (
        tmp_path / "out" / "association" / "final" / "overall" / "CVD" / "GENE1.json"
    )

    association_payload = json.loads(association_path.read_text())
    overall_payload = json.loads(overall_path.read_text())
    assert len(association_payload) == 1
    assert association_payload[0]["disease"] == ["cardiomyopathies", "cardiomyopathy"]

    vc = {item["name"]: item["value"] for item in association_payload[0]["vc"]}
    assert vc["SNP"] == 2

    ancestry = {
        item["name"]: {point["rsid"]: point["value"] for point in item["data"]}
        for item in association_payload[0]["ancestry"]
    }
    assert ancestry["African"]["rs1"] == 0.1
    assert ancestry["African"]["rs2"] == 0.2
    assert overall_payload["data"]["vc"]["SNP"] == 2
