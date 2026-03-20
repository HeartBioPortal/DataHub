import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.adapters import DbVarStructuralVariantAdapter
from datahub.models import CanonicalRecord
from datahub.publishers import StructuralVariantLegacyPublisher


class _FakeEnsemblClient:
    def overlap_region_genes(self, *, chromosome: str, start: int, end: int, species: str = "human"):
        if chromosome == "2" and start == 202464809 and end == 202464979:
            return [
                {
                    "id": "ENSG00000114739",
                    "external_name": "BMPR2",
                    "start": 202376327,
                    "end": 202567751,
                    "strand": 1,
                    "biotype": "protein_coding",
                }
            ]
        return []

    def gene_lookup(self, gene_id: str):
        if gene_id != "ENSG00000114739":
            return {}
        return {
            "id": gene_id,
            "start": 202376327,
            "end": 202567751,
            "strand": 1,
            "biotype": "protein_coding",
            "Transcript": [
                {
                    "id": "ENST00000263377",
                    "is_canonical": 1,
                    "gencode_primary": 1,
                    "assembly_name": "GRCh38",
                    "biotype": "protein_coding",
                }
            ],
        }

    def close(self) -> None:
        return None


def _write_dbvar_csv(path: Path) -> None:
    headers = [
        "Study ID",
        "Variant ID",
        "Variant Region type",
        "Variant Call type",
        "Sampleset ID",
        "Method",
        "Analysis ID",
        "Validation",
        "Variant Samples",
        "Subject Phenotype",
        "Clinical Interpretation",
        "Assembly Name",
        "Chromosome Accession",
        "Chromosome",
        "Outer Start",
        "Start",
        "Inner Start",
        "Inner End",
        "End",
        "Outer End",
        "Placement Type",
        "Remap Score",
    ]
    row = [
        "nstd102",
        "nssv15119706",
        "",
        "deletion",
        "",
        "",
        "",
        "No",
        "",
        "PPH1;Primary pulmonary hypertension",
        "Pathogenic",
        "GRCh38 (hg38)",
        "NC_000002.12",
        "2",
        "",
        "",
        "202464809",
        "202464979",
        "",
        "",
        "Submitted genomic",
        "",
    ]

    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerow(row)


def test_dbvar_structural_variant_adapter_emits_enriched_records(tmp_path: Path) -> None:
    csv_path = tmp_path / "all_variants_for_nstd102.csv"
    _write_dbvar_csv(csv_path)

    adapter = DbVarStructuralVariantAdapter(
        input_paths=csv_path,
        ensembl_client=_FakeEnsemblClient(),
        count_rows=False,
        progress_every=1,
    )
    records = list(adapter.read())

    assert len(records) == 1
    record = records[0]
    assert record.dataset_type == "STRUCTURAL_VARIANT"
    assert record.gene_id == "BMPR2"
    assert record.variant_id == "nssv15119706"
    assert record.variation_type == "deletion"
    assert record.clinical_significance == "Pathogenic"
    assert record.metadata["variant_region"] == "202464809-202464979"
    assert record.metadata["gene_location"] == "202376327-202567751"
    assert record.metadata["canonical_transcript"][0]["id"] == "ENST00000263377"
    assert adapter.report["records_emitted"] == 1


def test_structural_variant_publisher_writes_legacy_payload(tmp_path: Path) -> None:
    output_path = tmp_path / "structural_variants.json"
    publisher = StructuralVariantLegacyPublisher(output_path=output_path, progress_every=1)

    base_metadata = {
        "study_id": "nstd102",
        "assembly_name": "GRCh38 (hg38)",
        "variant_region": "202464809-202464979",
        "phenotypes": ["PPH1", "Primary pulmonary hypertension"],
        "gene_location": "202376327-202567751",
        "strand": 1,
        "biotype": "protein_coding",
        "canonical_transcript": [{"id": "ENST00000263377"}],
    }
    duplicate = CanonicalRecord(
        dataset_id="hbp_dbvar_structural_variant",
        dataset_type="STRUCTURAL_VARIANT",
        source="dbvar",
        gene_id="BMPR2",
        variant_id="nssv15119706",
        phenotype="PPH1",
        variation_type="deletion",
        clinical_significance="Pathogenic",
        metadata=base_metadata,
    )
    unique = CanonicalRecord(
        dataset_id="hbp_dbvar_structural_variant",
        dataset_type="STRUCTURAL_VARIANT",
        source="dbvar",
        gene_id="BMPR2",
        variant_id="nssv15119707",
        phenotype="PPH1",
        variation_type="duplication",
        clinical_significance=None,
        metadata={
            **base_metadata,
            "study_id": "nstd229",
            "variant_region": "202465000-202465100",
            "phenotypes": [],
        },
    )

    publisher.publish([duplicate, duplicate, unique])
    payload = json.loads(output_path.read_text())

    assert list(payload.keys()) == ["BMPR2"]
    assert payload["BMPR2"]["gene_location"] == "202376327-202567751"
    assert [variant["variant_id"] for variant in payload["BMPR2"]["variants"]] == [
        "nssv15119706",
        "nssv15119707",
    ]
    assert publisher.publish_report["variants_written"] == 2
