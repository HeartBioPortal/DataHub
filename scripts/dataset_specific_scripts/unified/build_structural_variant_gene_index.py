#!/usr/bin/env python3
"""Build the read-optimized per-gene structural-variant serving index."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path

from datahub.structural_variant_gene_index import (
    StructuralVariantGeneIndexBuilder,
    sha256_file,
)


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-json", required=True)
    parser.add_argument("--output-db", required=True)
    parser.add_argument("--checkpoint")
    parser.add_argument("--manifest")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument(
        "--limit",
        type=int,
        help="Smoke-test gene limit; leaves a non-servable .building artifact.",
    )
    parser.add_argument("--commit-interval", type=int, default=100)
    parser.add_argument("--progress-interval", type=int, default=100)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    output = Path(args.output_db).expanduser()
    working = output.with_name(f"{output.name}.building")
    checkpoint = (
        Path(args.checkpoint).expanduser()
        if args.checkpoint
        else output.with_name(f"{output.name}.checkpoint.json")
    )
    manifest_path = (
        Path(args.manifest).expanduser()
        if args.manifest
        else output.with_name(f"{output.name}.manifest.json")
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not args.reset:
        raise FileExistsError(f"Completed output already exists: {output}")
    if args.reset:
        for path in (
            output,
            working,
            working.with_name(f"{working.name}-wal"),
            working.with_name(f"{working.name}-shm"),
            checkpoint,
        ):
            path.unlink(missing_ok=True)

    builder = StructuralVariantGeneIndexBuilder(
        source_path=args.input_json,
        working_path=working,
        checkpoint_path=checkpoint,
        resume=args.resume,
        reset=args.reset,
        commit_interval=args.commit_interval,
        progress_interval=args.progress_interval,
    )
    try:
        summary = builder.build(limit=args.limit)
    finally:
        builder.close()

    if not summary["complete"]:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 2

    os.replace(working, output)
    output_sha256 = sha256_file(output)
    manifest = {
        "manifest_schema_version": "hbp_gene_payload_store_manifest_v1",
        "status": "complete",
        "built_at": datetime.now(timezone.utc).isoformat(),
        "command": " ".join(shlex.quote(value) for value in sys.argv),
        "output_path": str(output.resolve()),
        "output_size_bytes": output.stat().st_size,
        "output_sha256": output_sha256,
        **summary,
    }
    manifest["working_path"] = str(output.resolve())
    _atomic_write_json(manifest_path, manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
