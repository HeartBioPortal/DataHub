#!/usr/bin/env python3
"""Build dbSNP frequency Parquet handoff artifacts and final DuckDB index."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from datahub.secondary_analyses.dbsnp_frequency import (  # noqa: E402
    build_dbsnp_frequency_index_from_parquet,
    export_archive_to_parquet,
    export_legacy_to_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true", help="Enable INFO logging.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_archive = subparsers.add_parser(
        "export-archive",
        help="Stream one dbSNP frequency tar.gz archive into compressed Parquet part files.",
    )
    export_archive.add_argument("--archive", type=Path, required=True, help="Input dbSNP frequency .tar.gz archive.")
    export_archive.add_argument(
        "--output-root",
        type=Path,
        default=Path("analyzed_data/dbsnp_frequency"),
        help="Parquet handoff root.",
    )
    export_archive.add_argument("--batch-size", type=int, default=100_000, help="Rows per Parquet part.")
    export_archive.add_argument("--limit-members", type=int, default=None, help="Frequency members for smoke tests.")
    export_archive.add_argument("--checkpoint-path", type=Path, default=None, help="Optional checkpoint JSON path.")
    export_archive.add_argument("--reset", action="store_true", help="Rebuild this archive shard from scratch.")
    export_archive.add_argument("--no-progress", dest="progress", action="store_false", default=True)
    export_archive.add_argument("--progress-interval", type=float, default=30.0)
    export_archive.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "uncompressed"])

    export_legacy = subparsers.add_parser(
        "export-legacy",
        help="Export existing HBP legacy dbSNP CSV artifacts into Parquet part files.",
    )
    export_legacy.add_argument(
        "--legacy-dbsnp-root",
        type=Path,
        default=Path("analyzed_data/dbSNP"),
        help="Existing HBP dbSNP artifact root.",
    )
    export_legacy.add_argument(
        "--output-root",
        type=Path,
        default=Path("analyzed_data/dbsnp_frequency"),
        help="Parquet handoff root.",
    )
    export_legacy.add_argument("--batch-size", type=int, default=100_000, help="Rows per Parquet part.")
    export_legacy.add_argument("--checkpoint-path", type=Path, default=None, help="Optional checkpoint JSON path.")
    export_legacy.add_argument("--reset", action="store_true", help="Rebuild the legacy shard from scratch.")
    export_legacy.add_argument("--no-progress", dest="progress", action="store_false", default=True)
    export_legacy.add_argument("--progress-interval", type=float, default=30.0)
    export_legacy.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "uncompressed"])

    build_duckdb = subparsers.add_parser(
        "build-duckdb",
        help="Import normalized Parquet records into the final DuckDB index.",
    )
    build_duckdb.add_argument(
        "--parquet-root",
        type=Path,
        default=Path("analyzed_data/dbsnp_frequency"),
        help="Parquet handoff root containing records/*.parquet.",
    )
    build_duckdb.add_argument(
        "--output-db",
        type=Path,
        default=Path("datamart/dbsnp_frequency.duckdb"),
        help="Output DuckDB path.",
    )
    build_duckdb.add_argument("--replace", action="store_true", help="Replace an existing output DB.")
    build_duckdb.add_argument("--threads", type=int, default=None, help="DuckDB thread count.")
    build_duckdb.add_argument("--memory-limit", default=None, help="DuckDB memory limit, for example 32GB.")
    build_duckdb.add_argument("--temp-directory", type=Path, default=None, help="DuckDB temp/spill directory.")

    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    if args.command == "export-archive":
        summary = export_archive_to_parquet(
            archive_path=args.archive,
            output_root=args.output_root,
            batch_size=args.batch_size,
            limit_members=args.limit_members,
            checkpoint_path=args.checkpoint_path,
            resume=not args.reset,
            progress=args.progress,
            progress_interval=args.progress_interval,
            compression=args.compression,
        )
    elif args.command == "export-legacy":
        summary = export_legacy_to_parquet(
            legacy_dbsnp_root=args.legacy_dbsnp_root,
            output_root=args.output_root,
            batch_size=args.batch_size,
            checkpoint_path=args.checkpoint_path,
            resume=not args.reset,
            progress=args.progress,
            progress_interval=args.progress_interval,
            compression=args.compression,
        )
    elif args.command == "build-duckdb":
        summary = build_dbsnp_frequency_index_from_parquet(
            parquet_root=args.parquet_root,
            output_db=args.output_db,
            replace=args.replace,
            threads=args.threads,
            memory_limit=args.memory_limit,
            temp_directory=args.temp_directory,
        )
    else:  # pragma: no cover - argparse enforces valid choices
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(asdict(summary), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
