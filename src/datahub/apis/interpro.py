"""InterPro REST API client for protein-domain enrichment."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .base import RestApiClient


class InterProApiClient(RestApiClient):
    """Client for InterPro API protein annotation endpoints."""

    base_url = "https://www.ebi.ac.uk/interpro/api"

    def protein(self, accession: str) -> dict[str, Any]:
        payload = self.get_json(
            f"protein/uniprot/{quote(accession, safe='')}",
            cache_namespace="interpro_protein",
            cache_key=str(accession),
        )
        return dict(payload or {})

    def entries_for_uniprot(
        self,
        accession: str,
        *,
        page_size: int = 200,
        max_pages: int = 25,
    ) -> list[dict[str, Any]]:
        """Return InterPro entries matching a UniProt accession.

        InterPro responses are paginated. The endpoint shape is intentionally
        kept in this client rather than in the protein-context normalizer so the
        rest of DataHub works with a simple list of result payloads.
        """

        endpoint = f"entry/interpro/protein/uniprot/{quote(accession, safe='')}"
        params: dict[str, Any] = {"page_size": int(page_size)}
        cache_key = f"{accession}:page_size={int(page_size)}:max_pages={int(max_pages)}"
        cached = self.cache.get("interpro_entries_for_uniprot", cache_key)
        if cached is not None:
            return list(cached or [])

        results: list[dict[str, Any]] = []
        next_endpoint: str | None = endpoint
        next_params: dict[str, Any] | None = params
        pages = 0
        while next_endpoint and pages < max_pages:
            payload = self.get_json(next_endpoint, params=next_params)
            if isinstance(payload, dict):
                results.extend(list(payload.get("results") or []))
                next_url = payload.get("next")
                next_endpoint = self._endpoint_from_next_url(next_url)
                next_params = None
            else:
                results.extend(list(payload or []))
                next_endpoint = None
            pages += 1

        self.cache.set("interpro_entries_for_uniprot", cache_key, results)
        return results

    def _endpoint_from_next_url(self, next_url: object) -> str | None:
        if not next_url:
            return None
        text = str(next_url)
        prefix = f"{self.base_url}/"
        if text.startswith(prefix):
            return text[len(prefix) :]
        if text.startswith("/interpro/api/"):
            return text[len("/interpro/api/") :]
        if text.startswith("/"):
            return text[1:]
        return text
