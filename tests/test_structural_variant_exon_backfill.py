import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.structural_variant_exons import (
    apply_structural_variant_exon_patch,
    enrich_structural_variant_exons,
    gene_needs_exon_backfill,
    transcript_has_exons,
)


class _FakeTranscriptLookupClient:
    def __init__(self):
        self.calls = []

    def lookup_id(self, identifier: str, *, expand: bool = False):
        self.calls.append((identifier, expand))
        if identifier == "ENSTANK2":
            return {
                "id": "ENSTANK2",
                "display_name": "ANK2-202",
                "start": 100,
                "end": 400,
                "seq_region_name": "4",
                "strand": 1,
                "Exon": [
                    {"id": "ENSEANK201", "start": 100, "end": 150},
                    {"id": "ENSEANK202", "start": 300, "end": 400},
                ],
            }
        return {}


def test_enrich_structural_variant_exons_only_fetches_missing_exons() -> None:
    payload = {
        "ANK2": {
            "gene_location": "100-400",
            "canonical_transcript": [{"id": "ENSTANK2", "display_name": "ANK2-202"}],
            "variants": [],
        },
        "TTN": {
            "gene_location": "1000-2000",
            "canonical_transcript": [
                {
                    "id": "ENSTTTN",
                    "display_name": "TTN-212",
                    "Exon": [{"id": "ENSETTN01", "start": 1000, "end": 1200}],
                }
            ],
            "variants": [],
        },
    }
    client = _FakeTranscriptLookupClient()

    patch, report = enrich_structural_variant_exons(payload, ensembl_client=client)

    assert client.calls == [("ENSTANK2", True)]
    assert report.genes_seen == 2
    assert report.genes_with_existing_exons == 1
    assert report.candidate_genes == 1
    assert report.genes_enriched == 1
    assert patch["ANK2"]["canonical_transcript"][0]["Exon"] == [
        {"id": "ENSEANK201", "start": 100, "end": 150},
        {"id": "ENSEANK202", "start": 300, "end": 400},
    ]
    assert payload["ANK2"]["canonical_transcript"][0]["seq_region_name"] == "4"
    assert payload["ANK2"]["canonical_transcript"][0]["display_name"] == "ANK2-202"
    assert transcript_has_exons(payload["TTN"]["canonical_transcript"][0])
    assert not gene_needs_exon_backfill(payload["TTN"])


def test_enrich_structural_variant_exons_patch_mode_does_not_mutate_payload() -> None:
    payload = {
        "ANK2": {
            "gene_location": "100-400",
            "canonical_transcript": [{"id": "ENSTANK2", "display_name": "ANK2-202"}],
            "variants": [],
        }
    }
    client = _FakeTranscriptLookupClient()

    patch, report = enrich_structural_variant_exons(
        payload,
        ensembl_client=client,
        dry_run=True,
    )

    assert report.genes_enriched == 1
    assert "Exon" not in payload["ANK2"]["canonical_transcript"][0]
    apply_report = apply_structural_variant_exon_patch(payload, {"genes": patch})
    assert apply_report == {
        "patch_genes": 1,
        "applied_genes": 1,
        "missing_genes": 0,
        "skipped_genes": 0,
    }
    assert payload["ANK2"]["canonical_transcript"][0]["Exon"][0]["id"] == "ENSEANK201"


def test_enrich_structural_variant_exons_supports_deterministic_partitions() -> None:
    payload = {
        "A": {"canonical_transcript": [{"id": "ENSTA"}]},
        "B": {"canonical_transcript": [{"id": "ENSTB"}]},
        "C": {"canonical_transcript": [{"id": "ENSTC"}]},
    }
    client = _FakeTranscriptLookupClient()

    _patch, report = enrich_structural_variant_exons(
        payload,
        ensembl_client=client,
        dry_run=True,
        unit_partitions=2,
        unit_partition_index=1,
    )

    assert report.candidate_genes == 3
    assert report.selected_genes == 1
    assert client.calls == [("ENSTB", True)]
