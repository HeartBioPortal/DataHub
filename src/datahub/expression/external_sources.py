"""Small URL builders for expression source integrations."""

from __future__ import annotations

from urllib.parse import urlencode


def geo_accession_url(accession: str) -> str:
    return f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}"


def expression_atlas_search_url(query: str) -> str:
    return "https://www.ebi.ac.uk/gxa/search?" + urlencode({"geneQuery": query})


def gtex_gene_url(gene_id_or_symbol: str) -> str:
    return "https://gtexportal.org/home/gene/" + str(gene_id_or_symbol).strip()

