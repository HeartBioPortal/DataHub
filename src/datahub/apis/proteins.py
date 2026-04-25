"""EMBL-EBI Proteins API client for protein feature enrichment."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .base import RestApiClient


class EbiProteinsApiClient(RestApiClient):
    """Client for the EMBL-EBI Proteins API."""

    base_url = "https://www.ebi.ac.uk/proteins/api"

    def protein(self, accession: str) -> dict[str, Any]:
        payload = self.get_json(
            f"proteins/{quote(accession, safe='')}",
            cache_namespace="ebi_proteins_entry",
            cache_key=str(accession),
        )
        return dict(payload or {})

    def features(self, accession: str) -> list[dict[str, Any]]:
        payload = self.get_json(
            f"features/{quote(accession, safe='')}",
            cache_namespace="ebi_proteins_features",
            cache_key=str(accession),
        )
        if isinstance(payload, dict):
            features = payload.get("features")
            return list(features or [])
        return list(payload or [])
