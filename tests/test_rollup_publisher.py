import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.models import CanonicalRecord  # noqa: E402
from datahub.publishers import PhenotypeRollupPublisher  # noqa: E402


def test_rollup_publisher_deduplicates_rsids_across_subphenotypes(tmp_path: Path) -> None:
    tree_path = tmp_path / "phenotype_tree.json"
    tree_path.write_text(
        json.dumps(
            {
                "CVD": {
                    "cardiac_dysrhythmias": [
                        "atrial_fibrillation",
                        "atrial_flutter",
                    ]
                }
            }
        )
    )

    records = [
        CanonicalRecord(
            dataset_id="mvp",
            dataset_type="CVD",
            source="mvp",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="atrial_fibrillation",
            disease_category="cardiac_dysrhythmias",
            variation_type="SNP",
            p_value=1e-8,
            ancestry={"African": 0.10},
            metadata={"parent_phenotype": "cardiac_dysrhythmias"},
        ),
        CanonicalRecord(
            dataset_id="mvp",
            dataset_type="CVD",
            source="mvp",
            gene_id="GENE1",
            variant_id="rs1",
            phenotype="atrial_flutter",
            disease_category="cardiac_dysrhythmias",
            variation_type="SNP",
            p_value=1e-6,
            ancestry={"African": 0.22},
            metadata={"parent_phenotype": "cardiac_dysrhythmias"},
        ),
        CanonicalRecord(
            dataset_id="mvp",
            dataset_type="CVD",
            source="mvp",
            gene_id="GENE1",
            variant_id="rs2",
            phenotype="atrial_flutter",
            disease_category="cardiac_dysrhythmias",
            variation_type="SNP",
            p_value=1e-5,
            ancestry={"African": 0.33},
            metadata={"parent_phenotype": "cardiac_dysrhythmias"},
        ),
    ]

    publisher = PhenotypeRollupPublisher(
        output_root=tmp_path / "out",
        tree_json_path=tree_path,
        deduplicate_variants=True,
        ancestry_value_precision=3,
    )
    publisher.publish(records)

    out_path = (
        tmp_path
        / "out"
        / "association"
        / "final"
        / "association_rollup"
        / "CVD"
        / "GENE1.json"
    )
    payload = json.loads(out_path.read_text())

    assert len(payload) == 1
    entry = payload[0]
    assert entry["disease"] == ["cardiac_dysrhythmias", "cardiac_dysrhythmias"]

    vc = {item["name"]: item["value"] for item in entry["vc"]}
    assert vc["SNP"] == 2

    ancestry_data = {item["name"]: item["data"] for item in entry["ancestry"]}
    african_points = {item["rsid"]: item["value"] for item in ancestry_data["African"]}
    # Dedup keeps rs1 from lower p-value record.
    assert african_points["rs1"] == 0.1
    assert african_points["rs2"] == 0.33


def test_rollup_publisher_incremental_merge_combines_batches(tmp_path: Path) -> None:
    tree_path = tmp_path / "phenotype_tree.json"
    tree_path.write_text(
        json.dumps(
            {
                "CVD": {
                    "cardiac_dysrhythmias": [
                        "atrial_fibrillation",
                        "atrial_flutter",
                    ]
                }
            }
        )
    )

    publisher = PhenotypeRollupPublisher(
        output_root=tmp_path / "out",
        tree_json_path=tree_path,
        incremental_merge=True,
    )

    publisher.publish(
        [
            CanonicalRecord(
                dataset_id="mvp",
                dataset_type="CVD",
                source="mvp",
                gene_id="GENE1",
                variant_id="rs1",
                phenotype="atrial_fibrillation",
                disease_category="cardiac_dysrhythmias",
                variation_type="SNP",
                ancestry={"African": 0.1},
                metadata={"parent_phenotype": "cardiac_dysrhythmias"},
            )
        ]
    )
    publisher.publish(
        [
            CanonicalRecord(
                dataset_id="mvp",
                dataset_type="CVD",
                source="mvp",
                gene_id="GENE1",
                variant_id="rs2",
                phenotype="atrial_flutter",
                disease_category="cardiac_dysrhythmias",
                variation_type="SNP",
                ancestry={"African": 0.2},
                metadata={"parent_phenotype": "cardiac_dysrhythmias"},
            )
        ]
    )

    out_path = (
        tmp_path
        / "out"
        / "association"
        / "final"
        / "association_rollup"
        / "CVD"
        / "GENE1.json"
    )
    payload = json.loads(out_path.read_text())

    assert len(payload) == 1
    entry = payload[0]
    vc = {item["name"]: item["value"] for item in entry["vc"]}
    assert vc["SNP"] == 2
    ancestry_data = {item["name"]: item["data"] for item in entry["ancestry"]}
    african_points = {item["rsid"]: item["value"] for item in ancestry_data["African"]}
    assert african_points["rs1"] == 0.1
    assert african_points["rs2"] == 0.2
