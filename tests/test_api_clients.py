import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datahub.apis import (
    EbiProteinsApiClient,
    EnsemblRestClient,
    InterProApiClient,
    NcbiVariationApiClient,
)


class _FakeResponse:
    def __init__(self, payload, *, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)
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


class _SequenceSession:
    def __init__(self, responses):
        self.responses = list(responses)
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
        return self.responses.pop(0)

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


def test_ensembl_client_retries_rate_limited_lookup(monkeypatch) -> None:
    sleep_calls = []
    session = _SequenceSession(
        [
            _FakeResponse({"error": "rate limit"}, status_code=429, headers={"Retry-After": "0.25"}),
            _FakeResponse({"id": "ENST00000530963"}),
        ]
    )
    monkeypatch.setattr("datahub.apis.base.time.sleep", sleep_calls.append)

    client = EnsemblRestClient(session=session, max_retries=2, retry_backoff_seconds=0.1)
    payload = client.lookup_id("ENST00000530963", expand=True)

    assert payload == {"id": "ENST00000530963"}
    assert len(session.calls) == 2
    assert sleep_calls == [0.25]


def test_ensembl_translation_overlap_endpoint() -> None:
    session = _FakeSession(
        {
            (
                "https://rest.ensembl.org/overlap/translation/ENSP0001",
                (("feature", "translation_exon"), ("species", "homo_sapiens")),
            ): [{"start": 1, "end": 10}],
        }
    )

    client = EnsemblRestClient(session=session, sleep_seconds=0.0)
    payload = client.overlap_translation("ENSP0001", feature="translation_exon")

    assert payload == [{"start": 1, "end": 10}]
    assert session.calls[0]["url"] == "https://rest.ensembl.org/overlap/translation/ENSP0001"


def test_ebi_proteins_feature_endpoint() -> None:
    session = _FakeSession(
        {
            (
                "https://www.ebi.ac.uk/proteins/api/features/P12345",
                (),
            ): {"features": [{"type": "DOMAIN", "begin": "1", "end": "20"}]},
        }
    )

    client = EbiProteinsApiClient(session=session, sleep_seconds=0.0)
    assert client.features("P12345") == [{"type": "DOMAIN", "begin": "1", "end": "20"}]


def test_interpro_entries_for_uniprot_endpoint() -> None:
    session = _FakeSession(
        {
            (
                "https://www.ebi.ac.uk/interpro/api/entry/interpro/protein/uniprot/P12345",
                (("page_size", 200),),
            ): {"results": [{"metadata": {"accession": "IPR1"}}], "next": None},
        }
    )

    client = InterProApiClient(session=session, sleep_seconds=0.0)
    assert client.entries_for_uniprot("P12345") == [{"metadata": {"accession": "IPR1"}}]


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
