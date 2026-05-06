"""Source snapshot downloads for gene_profile.

This module uses bulk/source-level downloads, not per-gene API calls. Downloads
write ``*.part`` files first, resume when the server supports HTTP ranges, and
publish checksum manifests after the final file is complete.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_SOURCES = {
    "hgnc": {
        "url": "https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt",
        "filename": "hgnc_complete_set.txt",
    },
    "ncbi_gene_summary": {
        "url": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_summary.gz",
        "filename": "gene_summary.gz",
    },
    "uniprot_human_reviewed": {
        "url": (
            "https://rest.uniprot.org/uniprotkb/stream"
            "?query=%28organism_id%3A9606%29%20AND%20%28reviewed%3Atrue%29"
            "&fields=accession%2Creviewed%2Cprotein_name%2Cgene_names%2Clength%2Ccc_function%2Cxref_go%2Cxref_reactome"
            "&format=tsv"
        ),
        "filename": "human_reviewed_uniprot.tsv",
    },
    "goa_human": {
        "url": "https://current.geneontology.org/annotations/goa_human.gaf.gz",
        "filename": "goa_human.gaf.gz",
    },
}


@dataclass(frozen=True)
class SnapshotPaths:
    root: Path
    hgnc_path: Path
    ncbi_summary_path: Path
    uniprot_path: Path
    goa_gaf_path: Path
    goa_compact_path: Path
    manifest_path: Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def progress_bar(done: int, total: int | None, *, width: int = 24) -> str:
    if not total or total <= 0:
        return "[" + "#" * min(width, max(1, done // (1024 * 1024))) + "]"
    filled = min(width, int(width * min(done, total) / total))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _manifest_for_file(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".manifest.json")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _existing_file_is_valid(path: Path) -> bool:
    manifest = _read_json(_manifest_for_file(path))
    expected = str(manifest.get("sha256") or "")
    if not path.exists() or not expected:
        return False
    return file_sha256(path) == expected


def download_source_file(
    *,
    source_id: str,
    url: str,
    output_path: str | Path,
    session: Any | None = None,
    force: bool = False,
    timeout_seconds: float = 120.0,
    chunk_size: int = 1024 * 1024,
    logger: logging.Logger | None = None,
    progress_interval_seconds: float = 5.0,
) -> dict[str, Any]:
    """Download one source file with resume/checksum/manifest support."""

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = _manifest_for_file(output)
    if output.exists() and not force and _existing_file_is_valid(output):
        manifest = _read_json(manifest_path)
        manifest["status"] = "skipped_existing"
        _write_json(manifest_path, manifest)
        if logger:
            logger.info("Source snapshot exists: source=%s path=%s sha256=%s", source_id, output, manifest.get("sha256"))
        return manifest

    if force and output.exists():
        output.unlink()

    partial = output.with_suffix(output.suffix + ".part")
    resume_from = partial.stat().st_size if partial.exists() and not force else 0
    headers = {"User-Agent": "HeartBioPortal-DataHub/0.1"}
    if resume_from:
        headers["Range"] = f"bytes={resume_from}-"

    client = session or requests.Session()
    started_at = utc_now_iso()
    with client.get(url, headers=headers, stream=True, timeout=timeout_seconds) as response:
        status_code = int(getattr(response, "status_code", 0))
        if resume_from and status_code != 206:
            resume_from = 0
            partial.unlink(missing_ok=True)
            with client.get(url, headers={"User-Agent": headers["User-Agent"]}, stream=True, timeout=timeout_seconds) as fresh_response:
                return _write_download_stream(
                    source_id=source_id,
                    url=url,
                    output=output,
                    partial=partial,
                    response=fresh_response,
                    started_at=started_at,
                    initial_bytes=0,
                    chunk_size=chunk_size,
                    logger=logger,
                    progress_interval_seconds=progress_interval_seconds,
                )
        return _write_download_stream(
            source_id=source_id,
            url=url,
            output=output,
            partial=partial,
            response=response,
            started_at=started_at,
            initial_bytes=resume_from,
            chunk_size=chunk_size,
            logger=logger,
            progress_interval_seconds=progress_interval_seconds,
        )


def _write_download_stream(
    *,
    source_id: str,
    url: str,
    output: Path,
    partial: Path,
    response: Any,
    started_at: str,
    initial_bytes: int,
    chunk_size: int,
    logger: logging.Logger | None,
    progress_interval_seconds: float,
) -> dict[str, Any]:
    status_code = int(getattr(response, "status_code", 0))
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
    content_length = response.headers.get("Content-Length") if hasattr(response, "headers") else None
    try:
        remaining = int(content_length) if content_length else None
    except ValueError:
        remaining = None
    total = initial_bytes + remaining if remaining is not None else None

    mode = "ab" if initial_bytes else "wb"
    downloaded = initial_bytes
    last_log = 0.0
    with partial.open(mode) as stream:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
            stream.write(chunk)
            downloaded += len(chunk)
            now = time.monotonic()
            if logger and now - last_log >= progress_interval_seconds:
                last_log = now
                logger.info(
                    "Source download progress: source=%s bytes=%d total=%s %s",
                    source_id,
                    downloaded,
                    total if total is not None else "unknown",
                    progress_bar(downloaded, total),
                )

    os.replace(partial, output)
    digest = file_sha256(output)
    manifest = {
        "source_id": source_id,
        "url": url,
        "output_path": str(output),
        "status": "downloaded",
        "http_status": status_code,
        "started_at": started_at,
        "completed_at": utc_now_iso(),
        "bytes": output.stat().st_size,
        "sha256": digest,
    }
    _write_json(_manifest_for_file(output), manifest)
    if logger:
        logger.info("Source download complete: source=%s bytes=%d sha256=%s path=%s", source_id, output.stat().st_size, digest, output)
    return manifest


def resolve_snapshot_paths(raw_root: str | Path, *, release: str) -> SnapshotPaths:
    root = Path(raw_root) / "gene_profile" / release
    return SnapshotPaths(
        root=root,
        hgnc_path=root / "hgnc" / DEFAULT_SOURCES["hgnc"]["filename"],
        ncbi_summary_path=root / "ncbi_gene" / DEFAULT_SOURCES["ncbi_gene_summary"]["filename"],
        uniprot_path=root / "uniprotkb" / DEFAULT_SOURCES["uniprot_human_reviewed"]["filename"],
        goa_gaf_path=root / "goa" / DEFAULT_SOURCES["goa_human"]["filename"],
        goa_compact_path=root / "goa" / "goa_human_compact.tsv.gz",
        manifest_path=root / "snapshot_manifest.json",
    )


def _open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def write_compact_goa_annotations(
    *,
    gaf_path: str | Path,
    output_path: str | Path,
    logger: logging.Logger | None = None,
    progress_every_rows: int = 1_000_000,
) -> dict[str, Any]:
    """Normalize GOA GAF into the compact TSV consumed by gene_profile."""

    source = Path(gaf_path)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    written = 0
    open_output = gzip.open if output.suffix == ".gz" else open
    with _open_text(source) as input_stream:
        with open_output(output, "wt", encoding="utf-8", newline="") as output_stream:  # type: ignore[arg-type]
            writer = csv.DictWriter(
                output_stream,
                delimiter="\t",
                fieldnames=[
                    "gene_symbol",
                    "uniprot_accession",
                    "go_id",
                    "go_name",
                    "aspect",
                    "evidence_code",
                    "reference",
                    "assigned_by",
                ],
            )
            writer.writeheader()
            for line in input_stream:
                if not line.strip() or line.startswith("!"):
                    continue
                rows += 1
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 15:
                    continue
                writer.writerow(
                    {
                        "gene_symbol": parts[2],
                        "uniprot_accession": parts[1],
                        "go_id": parts[4],
                        "go_name": "",
                        "aspect": parts[8],
                        "evidence_code": parts[6],
                        "reference": parts[5],
                        "assigned_by": parts[14],
                    }
                )
                written += 1
                if logger and progress_every_rows > 0 and rows % progress_every_rows == 0:
                    logger.info("GOA compact progress: scanned_rows=%d written=%d", rows, written)

    manifest = {
        "source_id": "goa_human_compact",
        "source_path": str(source),
        "output_path": str(output),
        "status": "normalized",
        "completed_at": utc_now_iso(),
        "rows_scanned": rows,
        "rows_written": written,
        "bytes": output.stat().st_size,
        "sha256": file_sha256(output),
    }
    _write_json(_manifest_for_file(output), manifest)
    if logger:
        logger.info("GOA compact complete: scanned_rows=%d written=%d path=%s", rows, written, output)
    return manifest


def download_gene_profile_sources(
    *,
    raw_root: str | Path,
    release: str,
    force: bool = False,
    include_goa: bool = True,
    session: Any | None = None,
    logger: logging.Logger | None = None,
) -> SnapshotPaths:
    """Download all source snapshots needed by the default gene_profile build."""

    paths = resolve_snapshot_paths(raw_root, release=release)
    manifests: dict[str, Any] = {}
    for source_id in ("hgnc", "ncbi_gene_summary", "uniprot_human_reviewed"):
        source = DEFAULT_SOURCES[source_id]
        target = {
            "hgnc": paths.hgnc_path,
            "ncbi_gene_summary": paths.ncbi_summary_path,
            "uniprot_human_reviewed": paths.uniprot_path,
        }[source_id]
        manifests[source_id] = download_source_file(
            source_id=source_id,
            url=source["url"],
            output_path=target,
            force=force,
            session=session,
            logger=logger,
        )

    if include_goa:
        source = DEFAULT_SOURCES["goa_human"]
        manifests["goa_human"] = download_source_file(
            source_id="goa_human",
            url=source["url"],
            output_path=paths.goa_gaf_path,
            force=force,
            session=session,
            logger=logger,
        )
        if force or not paths.goa_compact_path.exists() or not _existing_file_is_valid(paths.goa_compact_path):
            manifests["goa_human_compact"] = write_compact_goa_annotations(
                gaf_path=paths.goa_gaf_path,
                output_path=paths.goa_compact_path,
                logger=logger,
            )
        else:
            manifests["goa_human_compact"] = _read_json(_manifest_for_file(paths.goa_compact_path))

    _write_json(
        paths.manifest_path,
        {
            "module": "gene_profile",
            "release": release,
            "created_at": utc_now_iso(),
            "root": str(paths.root),
            "sources": manifests,
        },
    )
    return paths
