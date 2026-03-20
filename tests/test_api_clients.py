import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.apis import EnsemblRestClient, NcbiVariationApiClient


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append(
            {
                "url": url,
                "params": dict(params),
                "headers": dict(headers),
                "timeout": timeout,
            }
        )
        key = (url, tuple(sorted(dict(params).items())))
        return _FakeResponse(self.mapping[key])

    def close(self) -> None:
        return None


def test_ensembl_overlap_region_genes_chunks_and_deduplicates() -> None:
    session = _FakeSession(
        {
            (
                "https://rest.ensembl.org/overlap/region/human/1:1845355-6845354",
                (("feature", "gene"),),
            ): [
                {"id": "ENSG1", "external_name": "GENE1"},
            ],
            (
                "https://rest.ensembl.org/overlap/region/human/1:6845355-9534096",
                (("feature", "gene"),),
            ): [
                {"id": "ENSG1", "external_name": "GENE1"},
                {"id": "ENSG2", "external_name": "GENE2"},
            ],
        }
    )

    client = EnsemblRestClient(session=session, sleep_seconds=0.0, max_overlap_bp=5_000_000)
    genes = client.overlap_region_genes(chromosome="1", start=1845355, end=9534096)

    assert [gene["id"] for gene in genes] == ["ENSG1", "ENSG2"]
    assert [call["url"] for call in session.calls] == [
        "https://rest.ensembl.org/overlap/region/human/1:1845355-6845354",
        "https://rest.ensembl.org/overlap/region/human/1:6845355-9534096",
    ]


def test_ncbi_variation_client_normalizes_rsid_prefix() -> None:
    session = _FakeSession(
        {
            (
                "https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/123/frequency",
                (),
            ): {"refsnp_id": "123"},
        }
    )

    client = NcbiVariationApiClient(session=session, sleep_seconds=0.0)
    payload = client.refsnp_frequency("rs123")

    assert payload == {"refsnp_id": "123"}
    assert session.calls[0]["url"].endswith("/refsnp/123/frequency")
