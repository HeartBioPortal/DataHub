"""Build a bounded-memory, per-gene SQLite index from a gene-mapped JSON artifact."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, TextIO

from datahub.artifact_io import open_text_artifact, resolve_readable_artifact_path


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = "hbp_gene_payload_store_v1"
ARTIFACT_KIND = "structural_variants"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path: str | Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def iter_top_level_object(
    stream: TextIO,
    *,
    chunk_size: int = 1024 * 1024,
) -> Iterator[tuple[str, Any]]:
    """Yield a top-level JSON object's entries without retaining prior values."""

    decoder = json.JSONDecoder()
    buffer = ""
    position = 0
    exhausted = False

    def fill() -> bool:
        nonlocal buffer, exhausted
        if exhausted:
            return False
        chunk = stream.read(chunk_size)
        if not chunk:
            exhausted = True
            return False
        buffer += chunk
        return True

    def skip_whitespace() -> bool:
        nonlocal position
        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position < len(buffer):
                return True
            if not fill():
                return False

    def decode_next() -> tuple[Any, int]:
        while True:
            try:
                return decoder.raw_decode(buffer, position)
            except json.JSONDecodeError:
                if not fill():
                    raise

    if not skip_whitespace() or buffer[position] != "{":
        raise ValueError("Structural-variant source must be a top-level JSON object.")
    position += 1

    while True:
        if position > chunk_size:
            buffer = buffer[position:]
            position = 0
        if not skip_whitespace():
            raise ValueError("Unexpected end of structural-variant JSON object.")
        if buffer[position] == "}":
            position += 1
            break

        key, position = decode_next()
        if not isinstance(key, str):
            raise ValueError("Structural-variant JSON object contains a non-string key.")
        if not skip_whitespace() or buffer[position] != ":":
            raise ValueError(f"Missing ':' after structural-variant gene key {key!r}.")
        position += 1
        if not skip_whitespace():
            raise ValueError(f"Missing payload for structural-variant gene key {key!r}.")
        value, position = decode_next()
        yield key, value

        if not skip_whitespace():
            raise ValueError("Unexpected end after structural-variant gene payload.")
        delimiter = buffer[position]
        position += 1
        if delimiter == "}":
            break
        if delimiter != ",":
            raise ValueError(f"Unexpected structural-variant JSON delimiter {delimiter!r}.")

    if skip_whitespace():
        raise ValueError("Unexpected trailing content after structural-variant JSON object.")


class StructuralVariantGeneIndexBuilder:
    """Single-writer, resumable SQLite index builder."""

    def __init__(
        self,
        *,
        source_path: str | Path,
        working_path: str | Path,
        checkpoint_path: str | Path,
        resume: bool = False,
        reset: bool = False,
        commit_interval: int = 100,
        progress_interval: int = 100,
    ) -> None:
        self.source_path = resolve_readable_artifact_path(source_path).resolve()
        self.working_path = Path(working_path)
        self.checkpoint_path = Path(checkpoint_path)
        self.commit_interval = max(1, int(commit_interval))
        self.progress_interval = max(1, int(progress_interval))
        if not self.source_path.is_file():
            raise FileNotFoundError(self.source_path)
        if reset and self.working_path.exists():
            self.working_path.unlink()
        if self.working_path.exists() and not resume:
            raise FileExistsError(
                f"Working index exists; use --resume or --reset: {self.working_path}"
            )

        self.source_sha256 = sha256_file(self.source_path)
        self.working_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.working_path, timeout=60)
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS gene_payloads (
                   gene TEXT PRIMARY KEY,
                   source_gene TEXT NOT NULL,
                   payload_zlib BLOB NOT NULL,
                   payload_sha256 TEXT NOT NULL,
                   uncompressed_bytes INTEGER NOT NULL
               ) WITHOUT ROWID"""
        )
        existing = dict(self.connection.execute("SELECT key, value FROM metadata"))
        if existing:
            if existing.get("schema_version") != SCHEMA_VERSION:
                raise ValueError("Cannot resume an index with a different schema.")
            if existing.get("source_sha256") != self.source_sha256:
                raise ValueError("Cannot resume an index from a different source artifact.")
        started_at = existing.get("started_at") or datetime.now(timezone.utc).isoformat()
        metadata = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "status": "building",
            "source_path": str(self.source_path),
            "source_sha256": self.source_sha256,
            "source_size_bytes": str(self.source_path.stat().st_size),
            "started_at": started_at,
        }
        self.connection.executemany(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
            metadata.items(),
        )
        self.connection.commit()

    def completed_genes(self) -> set[str]:
        return {
            str(row[0])
            for row in self.connection.execute("SELECT gene FROM gene_payloads")
        }

    def _write_checkpoint(
        self,
        *,
        genes_seen: int,
        genes_written: int,
        genes_skipped: int,
        complete: bool,
        started_monotonic: float,
    ) -> None:
        elapsed = max(0.001, time.monotonic() - started_monotonic)
        _atomic_write_json(
            self.checkpoint_path,
            {
                "schema_version": SCHEMA_VERSION,
                "artifact_kind": ARTIFACT_KIND,
                "source_path": str(self.source_path),
                "source_sha256": self.source_sha256,
                "working_path": str(self.working_path.resolve()),
                "genes_seen": genes_seen,
                "genes_written": genes_written,
                "genes_skipped": genes_skipped,
                "complete": complete,
                "elapsed_seconds": round(elapsed, 3),
                "genes_per_second": round(genes_seen / elapsed, 3),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def build(self, *, limit: int | None = None) -> dict[str, Any]:
        completed = self.completed_genes()
        started = time.monotonic()
        genes_seen = 0
        genes_written = 0
        genes_skipped = 0
        limited = False

        with open_text_artifact(self.source_path) as stream:
            for source_gene, payload in iter_top_level_object(stream):
                if limit is not None and genes_seen >= max(0, int(limit)):
                    limited = True
                    break
                genes_seen += 1
                gene = str(source_gene).strip().upper()
                if not gene or gene in completed:
                    genes_skipped += 1
                else:
                    raw = canonical_json(payload).encode("utf-8")
                    self.connection.execute(
                        """INSERT OR REPLACE INTO gene_payloads
                           (gene, source_gene, payload_zlib, payload_sha256, uncompressed_bytes)
                           VALUES (?, ?, ?, ?, ?)""",
                        (
                            gene,
                            source_gene,
                            sqlite3.Binary(zlib.compress(raw, level=6)),
                            hashlib.sha256(raw).hexdigest(),
                            len(raw),
                        ),
                    )
                    genes_written += 1

                if genes_seen % self.commit_interval == 0:
                    self.connection.commit()
                if genes_seen % self.progress_interval == 0:
                    self.connection.commit()
                    self._write_checkpoint(
                        genes_seen=genes_seen,
                        genes_written=genes_written,
                        genes_skipped=genes_skipped,
                        complete=False,
                        started_monotonic=started,
                    )
                    LOGGER.info(
                        "Structural-variant index progress: genes_seen=%d genes_written=%d "
                        "genes_skipped=%d elapsed=%.1fs current_gene=%s",
                        genes_seen,
                        genes_written,
                        genes_skipped,
                        time.monotonic() - started,
                        source_gene,
                    )

        self.connection.commit()
        complete = not limited
        total_genes = int(
            self.connection.execute("SELECT count(*) FROM gene_payloads").fetchone()[0]
        )
        summary = {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": ARTIFACT_KIND,
            "source_path": str(self.source_path),
            "source_sha256": self.source_sha256,
            "working_path": str(self.working_path.resolve()),
            "genes_seen": genes_seen,
            "genes_written": genes_written,
            "genes_skipped": genes_skipped,
            "total_indexed_genes": total_genes,
            "complete": complete,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        if complete:
            completed_at = datetime.now(timezone.utc).isoformat()
            self.connection.executemany(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
                (
                    ("status", "complete"),
                    ("completed_at", completed_at),
                    ("gene_count", str(total_genes)),
                ),
            )
            self.connection.commit()
            self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self._write_checkpoint(
            genes_seen=genes_seen,
            genes_written=genes_written,
            genes_skipped=genes_skipped,
            complete=complete,
            started_monotonic=started,
        )
        return summary

    def close(self) -> None:
        self.connection.close()
