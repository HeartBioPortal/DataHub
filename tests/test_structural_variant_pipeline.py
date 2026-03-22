import csv
import gzip
import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.adapters import DbVarStructuralVariantAdapter
from datahub.checkpoints import StructuralVariantCheckpoint
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


class _FailingEnsemblClient:
    def overlap_region_genes(self, *, chromosome: str, start: int, end: int, species: str = "human"):
        raise AssertionError("Ensembl overlap should not be called when local GTF annotations are available")

    def gene_lookup(self, gene_id: str):
        raise AssertionError("Ensembl lookup should not be called when local GTF annotations are available")

    def close(self) -> None:
        return None


def _write_dbvar_csv_rows(path: Path, rows: list[list[str]]) -> None:
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

    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def _write_dbvar_csv(path: Path) -> None:
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
    _write_dbvar_csv_rows(path, [row])


def _write_zip_text(path: Path, member_name: str, payload: str) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, payload)


def _write_gtf(path: Path) -> None:
    payload = "\n".join(
        [
            "##description: test",
            '\t'.join(
                [
                    "2",
                    "HAVANA",
                    "gene",
                    "202376327",
                    "202567751",
                    ".",
                    "+",
                    ".",
                    'gene_id "ENSG00000114739.12"; gene_name "BMPR2"; gene_type "protein_coding";',
                ]
            ),
            '\t'.join(
                [
                    "2",
                    "HAVANA",
                    "transcript",
                    "202376327",
                    "202567751",
                    ".",
                    "+",
                    ".",
                    'gene_id "ENSG00000114739.12"; gene_name "BMPR2"; transcript_id "ENST00000263377.9"; transcript_name "BMPR2-201"; transcript_type "protein_coding"; tag "basic"; tag "Ensembl_canonical";',
                ]
            ),
        ]
    )
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(payload)


def test_dbvar_structural_variant_adapter_emits_enriched_records(tmp_path: Path) -> None:
    csv_path = tmp_path / "all_variants_for_nstd102.csv"
    _write_dbvar_csv(csv_path)
    zip_path = tmp_path / "all_variants_for_nstd102.csv.zip"
    _write_zip_text(zip_path, csv_path.name, csv_path.read_text())

    adapter = DbVarStructuralVariantAdapter(
        input_paths=zip_path,
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


def test_dbvar_structural_variant_adapter_loads_zipped_metadata_seed(tmp_path: Path) -> None:
    csv_path = tmp_path / "all_variants_for_nstd102.csv"
    _write_dbvar_csv(csv_path)
    seed_path = tmp_path / "structural_variants.json.zip"
    _write_zip_text(
        seed_path,
        "structural_variants.json",
        json.dumps(
            {
                "BMPR2": {
                    "gene_location": "202376327-202567751",
                    "strand": 1,
                    "biotype": "protein_coding",
                    "canonical_transcript": [{"id": "ENST00000263377"}],
                    "variants": [],
                }
            }
        ),
    )

    adapter = DbVarStructuralVariantAdapter(
        input_paths=csv_path,
        metadata_seed_path=seed_path,
        ensembl_client=_FakeEnsemblClient(),
        count_rows=False,
        progress_every=1,
    )
    records = list(adapter.read())

    assert len(records) == 1
    assert adapter.metadata_seed["BMPR2"]["canonical_transcript"][0]["id"] == "ENST00000263377"


def test_dbvar_structural_variant_adapter_uses_local_gtf_annotations(tmp_path: Path) -> None:
    csv_path = tmp_path / "all_variants_for_nstd102.csv"
    gtf_path = tmp_path / "gencode.test.annotation.gtf.gz"
    _write_dbvar_csv(csv_path)
    _write_gtf(gtf_path)

    adapter = DbVarStructuralVariantAdapter(
        input_paths=csv_path,
        gene_annotation_gtf_path=gtf_path,
        ensembl_client=_FailingEnsemblClient(),
        count_rows=False,
        progress_every=1,
    )
    records = list(adapter.read())

    assert len(records) == 1
    record = records[0]
    assert record.gene_id == "BMPR2"
    assert record.metadata["gene_location"] == "202376327-202567751"
    assert record.metadata["canonical_transcript"][0]["id"] == "ENST00000263377"


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


def test_structural_variant_publisher_merges_from_zipped_payload(tmp_path: Path) -> None:
    merge_source = tmp_path / "structural_variants.json.zip"
    _write_zip_text(
        merge_source,
        "structural_variants.json",
        json.dumps(
            {
                "BMPR2": {
                    "gene_location": "202376327-202567751",
                    "strand": 1,
                    "biotype": "protein_coding",
                    "canonical_transcript": [{"id": "ENST00000263377"}],
                    "variants": [
                        {
                            "variant_id": "nssv15119705",
                            "study_id": "nstd100",
                            "variant_type": "deletion",
                            "phenotype": ["PPH1"],
                            "clinical_significance": "Pathogenic",
                            "assembly_name": "GRCh38 (hg38)",
                            "variant_region": "202464700-202464800",
                        }
                    ],
                }
            }
        ),
    )

    output_path = tmp_path / "structural_variants.json"
    publisher = StructuralVariantLegacyPublisher(
        output_path=output_path,
        merge_source_json_path=merge_source,
        merge_existing=True,
        progress_every=1,
    )

    publisher.publish(
        [
            CanonicalRecord(
                dataset_id="hbp_dbvar_structural_variant",
                dataset_type="STRUCTURAL_VARIANT",
                source="dbvar",
                gene_id="BMPR2",
                variant_id="nssv15119706",
                phenotype="PPH1",
                variation_type="deletion",
                clinical_significance="Pathogenic",
                metadata={
                    "study_id": "nstd102",
                    "assembly_name": "GRCh38 (hg38)",
                    "variant_region": "202464809-202464979",
                    "phenotypes": ["PPH1"],
                    "gene_location": "202376327-202567751",
                    "strand": 1,
                    "biotype": "protein_coding",
                    "canonical_transcript": [{"id": "ENST00000263377"}],
                },
            )
        ]
    )

    payload = json.loads(output_path.read_text())
    assert [variant["variant_id"] for variant in payload["BMPR2"]["variants"]] == [
        "nssv15119705",
        "nssv15119706",
    ]


def test_structural_variant_resume_skips_checkpointed_rows(tmp_path: Path) -> None:
    full_csv_path = tmp_path / "all_variants_for_nstd102.csv"
    first_row_csv_path = tmp_path / "first_row.csv"
    output_path = tmp_path / "structural_variants.json"
    checkpoint_path = tmp_path / "structural_variants.checkpoint.json"
    gtf_path = tmp_path / "gencode.test.annotation.gtf.gz"

    first_row = [
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
    second_row = [
        "nstd229",
        "nssv15119707",
        "",
        "duplication",
        "",
        "",
        "",
        "No",
        "",
        "",
        "",
        "GRCh38 (hg38)",
        "NC_000002.12",
        "2",
        "",
        "",
        "202465000",
        "202465100",
        "",
        "",
        "Submitted genomic",
        "",
    ]
    _write_dbvar_csv_rows(full_csv_path, [first_row, second_row])
    _write_dbvar_csv_rows(first_row_csv_path, [first_row])
    _write_gtf(gtf_path)

    initial_adapter = DbVarStructuralVariantAdapter(
        input_paths=first_row_csv_path,
        gene_annotation_gtf_path=gtf_path,
        ensembl_client=_FailingEnsemblClient(),
        count_rows=False,
        progress_every=1,
    )
    initial_publisher = StructuralVariantLegacyPublisher(output_path=output_path, progress_every=1)
    initial_publisher.publish(initial_adapter.read())

    checkpoint = StructuralVariantCheckpoint(checkpoint_path)
    checkpoint.load()
    checkpoint.mark_completed_row(
        source_file=str(full_csv_path.resolve()),
        row_index=1,
        output_path=output_path,
    )

    resume_adapter = DbVarStructuralVariantAdapter(
        input_paths=full_csv_path,
        gene_annotation_gtf_path=gtf_path,
        resume_rows_by_file=checkpoint.completed_rows(),
        ensembl_client=_FailingEnsemblClient(),
        count_rows=False,
        progress_every=1,
    )
    resume_publisher = StructuralVariantLegacyPublisher(
        output_path=output_path,
        merge_source_json_path=output_path,
        merge_existing=True,
        checkpoint=checkpoint,
        checkpoint_every_rows=1,
        progress_every=1,
    )
    resume_adapter.row_completion_callback = resume_publisher.mark_source_row_completed
    resume_publisher.publish(resume_adapter.read())

    payload = json.loads(output_path.read_text())
    assert [variant["variant_id"] for variant in payload["BMPR2"]["variants"]] == [
        "nssv15119706",
        "nssv15119707",
    ]
    assert checkpoint.completed_rows()[str(full_csv_path.resolve())] == 2
