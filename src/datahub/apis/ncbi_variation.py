"""NCBI Variation API client for reusable DataHub integrations."""

from __future__ import annotations

from typing import Any

from .base import RestApiClient


class NcbiVariationApiClient(RestApiClient):
    """Client for selected NCBI Variation REST endpoints."""

    base_url = "https://api.ncbi.nlm.nih.gov/variation/v0"

    def refsnp_frequency(self, rsid: str | int) -> dict[str, Any]:
        normalized = str(rsid).strip()
        if normalized.lower().startswith("rs"):
            normalized = normalized[2:]
        payload = self.get_json(
            f"refsnp/{normalized}/frequency",
            cache_namespace="ncbi_refsnp_frequency",
            cache_key=normalized,
        )
        return dict(payload or {})
