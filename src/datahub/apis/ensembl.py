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

    def lookup_symbol(
        self,
        symbol: str,
        *,
        species: str = "homo_sapiens",
        expand: bool = False,
    ) -> dict[str, Any]:
        cache_key = f"{species}:{symbol}:expand={int(bool(expand))}"
        payload = self.get_json(
            f"lookup/symbol/{quote(species, safe='')}/{quote(symbol, safe='')}",
            params={"expand": 1 if expand else 0},
            cache_namespace="ensembl_lookup_symbol",
            cache_key=cache_key,
        )
        return dict(payload or {})

    def xrefs_id(
        self,
        identifier: str,
        *,
        external_db: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if external_db:
            params["external_db"] = external_db
        cache_key = f"{identifier}:external_db={external_db or ''}"
        payload = self.get_json(
            f"xrefs/id/{quote(identifier, safe='')}",
            params=params,
            cache_namespace="ensembl_xrefs_id",
            cache_key=cache_key,
        )
        return list(payload or [])

    def xrefs_name(
        self,
        name: str,
        *,
        species: str = "homo_sapiens",
        external_db: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if external_db:
            params["external_db"] = external_db
        cache_key = f"{species}:{name}:external_db={external_db or ''}"
        payload = self.get_json(
            f"xrefs/name/{quote(species, safe='')}/{quote(name, safe='')}",
            params=params,
            cache_namespace="ensembl_xrefs_name",
            cache_key=cache_key,
        )
        return list(payload or [])

    def overlap_translation(
        self,
        translation_id: str,
        *,
        feature: str = "protein_feature",
        species: str = "homo_sapiens",
        type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"feature": feature, "species": species}
        if type_filter:
            params["type"] = type_filter
        cache_key = (
            f"{translation_id}:feature={feature}:species={species}:type={type_filter or ''}"
        )
        payload = self.get_json(
            f"overlap/translation/{quote(translation_id, safe='')}",
            params=params,
            cache_namespace="ensembl_overlap_translation",
            cache_key=cache_key,
        )
        return list(payload or [])

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
