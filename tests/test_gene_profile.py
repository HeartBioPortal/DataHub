import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.secondary_analyses.base import SecondaryAnalysisManifest
from datahub.secondary_analyses.gene_profile import (
    build_gene_profile_payload,
    card_from_gene_profile,
    generate_gene_profile_artifacts,
    read_hgnc_records,
    read_ncbi_gene_summaries,
    read_uniprot_records,
)
from datahub.secondary_analyses.gene_profile_sources import (
    DEFAULT_SOURCES,
    download_source_file,
    write_compact_goa_annotations,
)


def _manifest() -> SecondaryAnalysisManifest:
    return SecondaryAnalysisManifest(
        analysis_id="gene_profile",
        version=1,
        mode="derived",
        description="test",
        artifact_subdir="gene_profile/v1",
    )


def test_gene_profile_parsers_and_payload_are_provenance_aware(tmp_path: Path) -> None:
    hgnc_path = tmp_path / "hgnc_complete_set.txt"
    hgnc_path.write_text(
        "hgnc_id\tsymbol\tname\tstatus\tlocus_group\tlocus_type\talias_symbol\tprev_symbol\tlocation\tentrez_id\tensembl_gene_id\trefseq_accession\tuniprot_ids\tgene_group\n"
        "HGNC:12403\tTTN\ttitin\tApproved\tprotein-coding gene\tgene with protein product\tCMD1G|TMD\t\t2q31.2\t7273\tENSG00000155657\tNM_001267550\tQ8WZ42\tsarcomere\n"
    )
    ncbi_path = tmp_path / "gene_summary.tsv"
    ncbi_path.write_text(
        "#tax_id\tGeneID\tSummary\n"
        "9606\t7273\tThis gene encodes a large abundant protein of striated muscle. The product is divided into regions.\n"
    )
    uniprot_path = tmp_path / "uniprot.json"
    uniprot_path.write_text(
        json.dumps(
            {
                "results": [
                    {
                        "primaryAccession": "Q8WZ42",
                        "entryType": "UniProtKB reviewed (Swiss-Prot)",
                        "proteinDescription": {
                            "recommendedName": {"fullName": {"value": "Titin"}}
                        },
                        "genes": [{"geneName": {"value": "TTN"}}],
                        "sequence": {"length": 34350},
                        "comments": [
                            {
                                "commentType": "FUNCTION",
                                "texts": [{"value": "Key component in the assembly and functioning of vertebrate striated muscles."}],
                            }
                        ],
                        "uniProtKBCrossReferences": [
                            {"database": "GO", "id": "GO:0030018"},
                            {"database": "Reactome", "id": "R-HSA-390522"},
                        ],
                    }
                ]
            }
        )
    )

    hgnc = read_hgnc_records(hgnc_path)[0]
    summary = read_ncbi_gene_summaries(ncbi_path)["7273"]
    uniprot = read_uniprot_records(uniprot_path)["Q8WZ42"]
    payload = build_gene_profile_payload(hgnc=hgnc, ncbi_summary=summary, uniprot=uniprot)
    card = card_from_gene_profile(payload)

    assert payload["identity"]["hgnc_id"] == "HGNC:12403"
    assert payload["summary"]["summary_source"] == "NCBI Gene"
    assert payload["protein"]["recommended_name"] == "Titin"
    assert payload["protein"]["length_aa"] == 34350
    assert payload["quality_flags"]["ncbi_summary_missing"] is False
    assert "HGNC" in card["source_badges"]
    assert "NCBI Gene" in card["source_badges"]
    assert "UniProt reviewed" in card["chips"]


def test_generate_gene_profile_artifacts_writes_hgnc_keyed_payload_and_indexes(tmp_path: Path) -> None:
    hgnc_path = tmp_path / "hgnc_complete_set.txt"
    hgnc_path.write_text(
        "hgnc_id\tsymbol\tname\tstatus\tlocus_group\tlocus_type\talias_symbol\tprev_symbol\tlocation\tentrez_id\tensembl_gene_id\trefseq_accession\tuniprot_ids\tgene_group\n"
        "HGNC:1\tANK2\tankyrin 2\tApproved\tprotein-coding gene\tgene with protein product\tANK-2\tLQT4\t4q25\t287\tENSG00000145362\tNM_001148\tQ01484\tankyrin\n"
    )

    rows = generate_gene_profile_artifacts(
        hgnc_path=hgnc_path,
        output_root=tmp_path / "secondary",
        manifest=_manifest(),
        include_genes={"ANK2"},
    )

    assert len(rows) == 1
    artifact = tmp_path / "secondary" / "final" / "gene_profile" / "v1" / "genes" / "HGNC_1.json.gz"
    with gzip.open(artifact, "rt", encoding="utf-8") as stream:
        payload = json.loads(stream.read())
    assert payload["identity"]["symbol"] == "ANK2"
    assert payload["summary"]["one_sentence"].startswith("ANK2 is a gene with protein product")
    assert (tmp_path / "secondary" / "final" / "gene_profile" / "v1" / "manifest.json").exists()
    assert (
        tmp_path / "secondary" / "final" / "gene_profile" / "v1" / "symbol_to_hgnc.jsonl"
    ).exists() or (
        tmp_path / "secondary" / "final" / "gene_profile" / "v1" / "symbol_to_hgnc.parquet"
    ).exists()


class _FakeResponse:
    def __init__(self, payload: bytes, *, status_code: int = 200):
        self.payload = payload
        self.status_code = status_code
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")

    def iter_content(self, chunk_size: int):
        for index in range(0, len(self.payload), chunk_size):
            yield self.payload[index : index + chunk_size]


class _FakeSession:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.calls = []

    def get(self, url, *, headers, stream, timeout):
        self.calls.append({"url": url, "headers": dict(headers), "stream": stream, "timeout": timeout})
        range_header = headers.get("Range", "")
        if range_header.startswith("bytes="):
            start = int(range_header.split("=", 1)[1].split("-", 1)[0])
            return _FakeResponse(self.payload[start:], status_code=206)
        return _FakeResponse(self.payload)


def test_download_source_file_writes_checksum_manifest_and_skips_valid_existing(tmp_path: Path) -> None:
    session = _FakeSession(b"gene-profile-source")
    output = tmp_path / "raw" / "source.txt"

    manifest = download_source_file(
        source_id="test_source",
        url="https://example.org/source.txt",
        output_path=output,
        session=session,
        progress_interval_seconds=0,
    )

    assert output.read_bytes() == b"gene-profile-source"
    assert manifest["sha256"]
    assert (tmp_path / "raw" / "source.txt.manifest.json").exists()

    skipped = download_source_file(
        source_id="test_source",
        url="https://example.org/source.txt",
        output_path=output,
        session=session,
    )
    assert skipped["status"] == "skipped_existing"
    assert len(session.calls) == 1


def test_download_source_file_resumes_partial_download(tmp_path: Path) -> None:
    session = _FakeSession(b"complete payload")
    output = tmp_path / "source.txt"
    output.with_suffix(".txt.part").write_bytes(b"complete ")

    download_source_file(
        source_id="test_source",
        url="https://example.org/source.txt",
        output_path=output,
        session=session,
        progress_interval_seconds=0,
    )

    assert output.read_bytes() == b"complete payload"
    assert session.calls[0]["headers"]["Range"] == "bytes=9-"


def test_write_compact_goa_annotations_from_gaf(tmp_path: Path) -> None:
    gaf = tmp_path / "goa.gaf"
    gaf.write_text(
        "!gaf-version: 2.2\n"
        "UniProtKB\tQ8WZ42\tTTN\t\tGO:0030018\tPMID:1\tIDA\t\tC\tTitin\t\tprotein\ttaxon:9606\t20260101\tUniProt\t\t\n"
    )
    compact = tmp_path / "goa_compact.tsv.gz"

    manifest = write_compact_goa_annotations(gaf_path=gaf, output_path=compact)

    assert manifest["rows_written"] == 1
    with gzip.open(compact, "rt", encoding="utf-8") as stream:
        text = stream.read()
    assert "gene_symbol\tuniprot_accession\tgo_id" in text
    assert "TTN\tQ8WZ42\tGO:0030018" in text


def test_uniprot_source_uses_current_go_field_name() -> None:
    url = DEFAULT_SOURCES["uniprot_human_reviewed"]["url"]

    assert "go_id" in url
    assert "xref_go" not in url


def test_read_uniprot_tsv_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "uniprot.tsv"
    path.write_text(
        "Entry\tReviewed\tProtein names\tGene Names\tLength\tFunction [CC]\tGene Ontology IDs\tReactome\n"
        "Q8WZ42\treviewed\tTitin\tTTN CMD1G\t34350\tFUNCTION: Muscle protein.\tGO:0030018; GO:0003779\tR-HSA-390522\n"
    )

    records = read_uniprot_records(path)

    assert records["Q8WZ42"].reviewed is True
    assert records["Q8WZ42"].protein_name == "Titin"
    assert records["Q8WZ42"].length_aa == 34350
    assert "TTN" in records["Q8WZ42"].gene_symbols
