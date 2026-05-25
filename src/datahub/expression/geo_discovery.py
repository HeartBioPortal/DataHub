"""GEO study discovery helpers for CVD expression curation."""

from __future__ import annotations

import sqlite3
import gzip
import shutil
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from .config import DEFAULT_CVD_TERMS

DEFAULT_GEOMETADB_URL = "https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz"


@dataclass(frozen=True)
class GeoStudyCandidate:
    study_accession: str
    title: str | None
    summary: str | None
    pubmed_id: str | None
    organism: str | None
    platform: str | None
    matched_term: str
    overall_design: str | None
    source_url: str

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)


def discover_geo_cvd_candidates(
    sqlite_path: str | Path,
    *,
    terms: Iterable[str] | None = None,
    organism: str = "Homo sapiens",
    limit_per_term: int | None = None,
) -> list[GeoStudyCandidate]:
    """Search a local GEOmetadb.sqlite file for CVD expression candidates."""

    terms = list(terms or DEFAULT_CVD_TERMS)
    path = Path(sqlite_path)
    if not path.exists():
        raise FileNotFoundError(f"GEOmetadb SQLite file not found: {path}")

    candidates: dict[tuple[str, str], GeoStudyCandidate] = {}
    connection = sqlite3.connect(str(path))
    try:
        for term in terms:
            like = f"%{term}%"
            sql = """
SELECT DISTINCT
    gse.gse,
    gse.title,
    gse.summary,
    gse.pubmed_id,
    gpl.organism,
    gpl.gpl,
    gse.overall_design
FROM gse
JOIN gse_gpl ON gse_gpl.gse = gse.gse
JOIN gpl ON gse_gpl.gpl = gpl.gpl
WHERE
    (gse.title LIKE ? OR gse.overall_design LIKE ? OR gse.summary LIKE ?)
    AND gpl.organism = ?
ORDER BY gse.gse
"""
            rows = connection.execute(sql, [like, like, like, organism]).fetchall()
            if limit_per_term is not None:
                rows = rows[:limit_per_term]
            for row in rows:
                accession = str(row[0] or "").strip()
                if not accession:
                    continue
                key = (accession, term)
                candidates[key] = GeoStudyCandidate(
                    study_accession=accession,
                    title=row[1],
                    summary=row[2],
                    pubmed_id=str(row[3]) if row[3] is not None else None,
                    organism=row[4],
                    platform=row[5],
                    matched_term=term,
                    overall_design=row[6],
                    source_url=f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}",
                )
    finally:
        connection.close()

    return sorted(candidates.values(), key=lambda item: (item.matched_term, item.study_accession))


def download_geometadb_sqlite(
    *,
    output_path: str | Path,
    url: str = DEFAULT_GEOMETADB_URL,
    replace: bool = False,
) -> Path:
    """Download and unpack GEOmetadb.sqlite.gz.

    This mirrors the role of Bioconductor GEOmetadb's getSQLiteFile while
    keeping the Python DataHub orchestration self-contained.
    """

    destination = Path(output_path)
    if destination.exists() and not replace:
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    gzip_path = destination.with_suffix(destination.suffix + ".gz")
    urllib.request.urlretrieve(url, gzip_path)  # noqa: S310 - caller controls URL in CLI.
    with gzip.open(gzip_path, "rb") as source, destination.open("wb") as target:
        shutil.copyfileobj(source, target)
    gzip_path.unlink(missing_ok=True)
    return destination
