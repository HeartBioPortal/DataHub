#!/usr/bin/env python3
"""Build the dbSNP population-frequency DuckDB index."""

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

from datahub.secondary_analyses.dbsnp_frequency import build_dbsnp_frequency_index


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("raw_data/dbsnp"),
        help="Directory containing dbSNP frequency .tar.gz archives.",
    )
    parser.add_argument(
        "--output-db",
        type=Path,
        default=Path("datamart/dbsnp_frequency.duckdb"),
        help="DuckDB file to create.",
    )
    parser.add_argument(
        "--legacy-dbsnp-root",
        type=Path,
        default=Path("analyzed_data/dbSNP"),
        help="Existing HBP dbSNP artifact root used when legacy loading is enabled.",
    )
    legacy_group = parser.add_mutually_exclusive_group()
    legacy_group.add_argument(
        "--include-legacy",
        dest="include_legacy",
        action="store_true",
        default=True,
        help="Also load existing HBP dbSNP CSV artifacts as separately provenanced rows. This is the default.",
    )
    legacy_group.add_argument(
        "--skip-legacy",
        dest="include_legacy",
        action="store_false",
        help="Only load the new dbSNP frequency archives.",
    )
    parser.add_argument(
        "--limit-members",
        type=int,
        default=None,
        help="Limit frequency CSV members per archive for smoke tests.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Number of normalized rows inserted per DuckDB batch.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable progress logging.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    summary = build_dbsnp_frequency_index(
        raw_root=args.raw_root,
        output_db=args.output_db,
        legacy_dbsnp_root=args.legacy_dbsnp_root,
        include_legacy=args.include_legacy,
        limit_members=args.limit_members,
        batch_size=args.batch_size,
    )
    print(json.dumps(asdict(summary), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
