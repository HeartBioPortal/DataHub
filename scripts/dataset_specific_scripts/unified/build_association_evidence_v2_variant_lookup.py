#!/usr/bin/env python3
"""Build a bounded canonical-variant lookup for evidence-model-v2 serving."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


LOGGER = logging.getLogger("datahub.association_evidence_v2.variant_lookup")


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


def monitor_progress(root: Path, interval: int, stop: threading.Event) -> None:
    started = time.perf_counter()
    while not stop.wait(interval):
        files = 0
        size = 0
        if root.exists():
            for path in root.rglob("*.parquet"):
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                files += 1
                size += stat.st_size
        LOGGER.info(
            "variant lookup progress elapsed_seconds=%.1f parquet_files=%d bytes=%d",
            time.perf_counter() - started,
            files,
            size,
        )


def build(args: argparse.Namespace) -> dict:
    import duckdb

    source_db = args.source_db.resolve()
    output_root = args.output_root.resolve()
    checkpoint = output_root / "variant-lookup-checkpoint.json"
    manifest_path = output_root / "serving-manifest.json"
    checksum_path = output_root / "checksums.sha256"
    table_root = output_root / "tables" / "variants"
    incomplete_root = output_root / ".incomplete-variants"

    if not source_db.is_file():
        raise FileNotFoundError(f"Source sidecar not found: {source_db}")
    if args.reset and output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    configuration = {
        "source_db": str(source_db),
        "source_db_sha256": args.source_sha256,
        "bucket_characters": args.bucket_characters,
        "smoke_limit": args.smoke_limit,
    }
    if checkpoint.is_file():
        state = json.loads(checkpoint.read_text())
        if state.get("configuration") != configuration:
            raise RuntimeError("Existing checkpoint configuration does not match this run")
        if state.get("complete") and table_root.is_dir() and manifest_path.is_file():
            LOGGER.info("variant lookup already complete output_root=%s", output_root)
            return json.loads(manifest_path.read_text())
    else:
        state = {
            "configuration": configuration,
            "complete": False,
            "started_at": utc_now(),
        }
        write_json(checkpoint, state)

    if incomplete_root.exists():
        shutil.rmtree(incomplete_root)
    if table_root.exists():
        raise RuntimeError(f"Uncheckpointed final table already exists: {table_root}")
    incomplete_table = incomplete_root / "tables" / "variants"
    incomplete_table.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(
        str(source_db),
        read_only=True,
        config={"threads": str(args.threads), "memory_limit": args.memory_limit},
    )
    connection.execute("SET preserve_insertion_order=false")
    available_rows = int(
        connection.execute(
            "SELECT estimated_size FROM duckdb_tables() WHERE table_name='variants'"
        ).fetchone()[0]
    )
    limit_sql = f" LIMIT {int(args.smoke_limit)}" if args.smoke_limit else ""
    LOGGER.info(
        "variant lookup build start source=%s output=%s available_rows=%d smoke_limit=%s",
        source_db,
        output_root,
        available_rows,
        args.smoke_limit,
    )
    stop = threading.Event()
    monitor = threading.Thread(
        target=monitor_progress,
        args=(incomplete_table, args.progress_interval, stop),
        daemon=True,
    )
    monitor.start()
    started = time.perf_counter()
    try:
        copied = connection.execute(
            f"""
COPY (
    SELECT *, substr(sha256(variant_id), 1, {args.bucket_characters}) AS variant_bucket
    FROM variants
    {limit_sql}
) TO '{str(incomplete_table).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
        ).fetchone()
    finally:
        stop.set()
        monitor.join(timeout=max(1, args.progress_interval))
        connection.close()
    rows = int(copied[0]) if copied else 0
    files = sorted(incomplete_table.rglob("*.parquet"))
    if rows <= 0 or not files:
        raise RuntimeError("Variant lookup build produced no rows")

    table_root.parent.mkdir(parents=True, exist_ok=True)
    incomplete_table.rename(table_root)
    shutil.rmtree(incomplete_root)
    elapsed = time.perf_counter() - started
    manifest = {
        "contract": "association_evidence_v2_partitioned_serving",
        "schema_version": "2.8.1-optimized-rc1",
        "built_at": utc_now(),
        "source_db": str(source_db),
        "source_db_sha256": args.source_sha256,
        "runtime": {
            "python": __import__("platform").python_version(),
            "duckdb": duckdb.__version__,
            "threads": args.threads,
            "memory_limit": args.memory_limit,
            "progress_interval": args.progress_interval,
        },
        "tables": {
            "variants": {
                "path": "tables/variants",
                "partition_key": "variant_id",
                "bucket_function": f"sha256(variant_id)[0:{args.bucket_characters}]",
                "bucket_characters": args.bucket_characters,
                "directory_layout": "flat_variant_bucket",
                "rows": rows,
                "files": len(files),
                "complete": True,
            }
        },
        "build": {
            "elapsed_seconds": round(elapsed, 3),
            "smoke_limit": args.smoke_limit,
            "checkpoint_granularity": "atomic complete variants table",
        },
    }
    write_json(manifest_path, manifest)

    checksum_rows = []
    for path in sorted([manifest_path, *table_root.rglob("*.parquet")]):
        checksum_rows.append(f"{sha256_file(path)}  {path.relative_to(output_root)}")
    checksum_path.write_text("\n".join(checksum_rows) + "\n")
    state.update(
        {
            "complete": True,
            "completed_at": utc_now(),
            "rows": rows,
            "files": len(files),
            "elapsed_seconds": round(elapsed, 3),
            "checksum_entries": len(checksum_rows),
        }
    )
    write_json(checkpoint, state)
    LOGGER.info(
        "variant lookup build complete rows=%d files=%d elapsed_seconds=%.3f",
        rows,
        len(files),
        elapsed,
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--bucket-characters", type=int, choices=(2, 3, 4), default=3)
    parser.add_argument("--threads", type=int, default=4)
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
    result = build(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
