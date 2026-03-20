"""Ensembl REST API client for reusable DataHub integrations."""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any
from urllib.parse import quote

from .base import RestApiClient


logger = logging.getLogger(__name__)

ENSEMBL_MAX_OVERLAP_BP = 5_000_000


def _chunk_region_bounds(start: int, end: int, *, max_span_bp: int) -> list[tuple[int, int]]:
    if start > end:
        start, end = end, start
    if max_span_bp <= 0:
        raise ValueError("max_span_bp must be positive")

    chunks: list[tuple[int, int]] = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + max_span_bp - 1, end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + 1
    return chunks


class EnsemblRestClient(RestApiClient):
    """Client for the public Ensembl REST API."""

    base_url = "https://rest.ensembl.org"

    def __init__(
        self,
        *,
        max_overlap_bp: int = ENSEMBL_MAX_OVERLAP_BP,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.max_overlap_bp = max(int(max_overlap_bp), 1)

    def phenotype_term(self, *, species: str, term: str) -> list[dict[str, Any]]:
        cache_key = f"{species}:{term}"
        payload = self.get_json(
            f"phenotype/term/{quote(species, safe='')}/{quote(term, safe='')}",
            cache_namespace="ensembl_phenotype_term",
            cache_key=cache_key,
        )
        return list(payload or [])

    def variation(self, *, species: str, variation_id: str) -> dict[str, Any]:
        cache_key = f"{species}:{variation_id}"
        payload = self.get_json(
            f"variation/{quote(species, safe='')}/{quote(variation_id, safe='')}",
            cache_namespace="ensembl_variation",
            cache_key=cache_key,
        )
        return dict(payload or {})

    def lookup_id(self, identifier: str, *, expand: bool = False) -> dict[str, Any]:
        cache_key = f"{identifier}:expand={int(bool(expand))}"
        payload = self.get_json(
            f"lookup/id/{quote(identifier, safe='')}",
            params={"expand": 1 if expand else 0},
            cache_namespace="ensembl_lookup",
            cache_key=cache_key,
        )
        return dict(payload or {})

    def gene_lookup(self, gene_id: str) -> dict[str, Any]:
        return self.lookup_id(gene_id, expand=True)

    def overlap_region_genes(
        self,
        *,
        chromosome: str,
        start: int,
        end: int,
        species: str = "human",
    ) -> list[dict[str, Any]]:
        regions = _chunk_region_bounds(start, end, max_span_bp=self.max_overlap_bp)
        if len(regions) > 1:
            logger.info(
                "Splitting Ensembl overlap request for %s:%d-%d into %d chunks (max_span=%d)",
                chromosome,
                start,
                end,
                len(regions),
                self.max_overlap_bp,
            )

        deduped: dict[str, dict[str, Any]] = {}
        for chunk_start, chunk_end in regions:
            cache_key = f"{species}:{chromosome}:{chunk_start}-{chunk_end}"
            payload = self.get_json(
                f"overlap/region/{species}/{chromosome}:{chunk_start}-{chunk_end}",
                params={"feature": "gene"},
                cache_namespace="ensembl_overlap_region_genes",
                cache_key=cache_key,
            )
            for gene in payload or []:
                gene_id = str(gene.get("id") or "").strip()
                if gene_id:
                    deduped[gene_id] = deepcopy(gene)
        return list(deduped.values())
