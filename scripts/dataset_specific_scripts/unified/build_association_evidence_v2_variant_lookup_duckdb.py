#!/usr/bin/env python3
"""Build an indexed DuckDB lookup from verified canonical-variant Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


LOGGER = logging.getLogger("datahub.association_evidence_v2.variant_lookup_duckdb")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def monitor(path: Path, interval: int, stop: threading.Event) -> None:
    started = time.perf_counter()
    while not stop.wait(interval):
        size = path.stat().st_size if path.exists() else 0
        LOGGER.info(
            "variant lookup DuckDB progress elapsed_seconds=%.1f bytes=%d",
            time.perf_counter() - started,
            size,
        )


def build(args: argparse.Namespace) -> dict:
    import duckdb

    parquet_root = args.parquet_root.resolve()
    output_db = args.output_db.resolve()
    checkpoint = output_db.with_suffix(output_db.suffix + ".checkpoint.json")
    manifest_path = output_db.with_suffix(output_db.suffix + ".manifest.json")
    checksum_path = output_db.with_suffix(output_db.suffix + ".sha256")
    temporary_db = output_db.with_suffix(output_db.suffix + ".incomplete")
    configuration = {
        "parquet_root": str(parquet_root),
        "parquet_manifest_sha256": args.parquet_manifest_sha256,
        "expected_rows": args.expected_rows,
        "smoke_limit": args.smoke_limit,
    }
    if not parquet_root.is_dir():
        raise FileNotFoundError(f"Canonical variant Parquet root not found: {parquet_root}")
    output_db.parent.mkdir(parents=True, exist_ok=True)
    if args.reset:
        for path in (output_db, checkpoint, manifest_path, checksum_path, temporary_db):
            path.unlink(missing_ok=True)
    if checkpoint.is_file():
        state = json.loads(checkpoint.read_text())
        if state.get("configuration") != configuration:
            raise RuntimeError("Existing checkpoint configuration does not match this run")
        if state.get("complete") and output_db.is_file() and manifest_path.is_file():
            LOGGER.info("indexed variant lookup already complete output=%s", output_db)
            return json.loads(manifest_path.read_text())
    else:
        state = {"configuration": configuration, "started_at": utc_now(), "complete": False}
        write_json(checkpoint, state)
    temporary_db.unlink(missing_ok=True)
    if output_db.exists():
        raise RuntimeError(f"Uncheckpointed lookup database exists: {output_db}")

    glob = str(parquet_root / "**" / "*.parquet").replace("'", "''")
    limit_sql = f" LIMIT {int(args.smoke_limit)}" if args.smoke_limit else ""
    connection = duckdb.connect(
        str(temporary_db),
        config={"threads": str(args.threads), "memory_limit": args.memory_limit},
    )
    connection.execute("SET preserve_insertion_order=false")
    stop = threading.Event()
    progress = threading.Thread(
        target=monitor,
        args=(temporary_db, args.progress_interval, stop),
        daemon=True,
    )
    progress.start()
    started = time.perf_counter()
    try:
        connection.execute(
            f"CREATE TABLE variants AS SELECT * EXCLUDE (variant_bucket) "
            f"FROM read_parquet('{glob}', hive_partitioning=true){limit_sql}"
        )
        connection.execute("CREATE UNIQUE INDEX idx_variants_variant_id ON variants(variant_id)")
        connection.execute("ANALYZE variants")
        rows = int(connection.execute("SELECT count(*) FROM variants").fetchone()[0])
        duplicate_count = int(
            connection.execute(
                "SELECT count(*)-count(DISTINCT variant_id) FROM variants"
            ).fetchone()[0]
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
        stop.set()
        progress.join(timeout=max(1, args.progress_interval))
    if duplicate_count:
        raise RuntimeError(f"Variant lookup contains {duplicate_count} duplicate IDs")
    if args.expected_rows is not None and rows != args.expected_rows:
        raise RuntimeError(
            f"Variant lookup row count mismatch: expected {args.expected_rows}, found {rows}"
        )
    temporary_db.rename(output_db)
    elapsed = time.perf_counter() - started
    digest = sha256_file(output_db)
    checksum_path.write_text(f"{digest}  {output_db.name}\n")
    manifest = {
        "contract": "association_evidence_v2_canonical_variant_lookup",
        "schema_version": "2.8.1-optimized-rc1",
        "built_at": utc_now(),
        "database": str(output_db),
        "database_sha256": digest,
        "source_parquet_root": str(parquet_root),
        "source_parquet_manifest_sha256": args.parquet_manifest_sha256,
        "table": "variants",
        "primary_lookup_key": "variant_id",
        "unique_index": "idx_variants_variant_id",
        "rows": rows,
        "duplicate_variant_ids": duplicate_count,
        "runtime": {
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "threads": args.threads,
            "memory_limit": args.memory_limit,
        },
        "build": {
            "elapsed_seconds": round(elapsed, 3),
            "smoke_limit": args.smoke_limit,
            "checkpoint_granularity": "atomic indexed lookup database",
        },
    }
    write_json(manifest_path, manifest)
    state.update(
        {
            "complete": True,
            "completed_at": utc_now(),
            "rows": rows,
            "database_sha256": digest,
            "elapsed_seconds": round(elapsed, 3),
        }
    )
    write_json(checkpoint, state)
    LOGGER.info(
        "indexed variant lookup complete rows=%d bytes=%d elapsed_seconds=%.3f",
        rows,
        output_db.stat().st_size,
        elapsed,
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-root", type=Path, required=True)
    parser.add_argument("--parquet-manifest-sha256", required=True)
    parser.add_argument("--output-db", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--progress-interval", type=int, default=30)
    parser.add_argument("--smoke-limit", type=int)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    print(json.dumps(build(args), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
