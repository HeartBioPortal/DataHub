#!/usr/bin/env python3
"""Build the resumable unavailable-provider source-summary serving index."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from datahub.association_evidence_v2.source_summary_index import SourceSummaryIndexBuilder


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--association-artifact-root", type=Path, required=True)
    parser.add_argument("--serving-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--progress-interval", type=int, default=1000)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    result = SourceSummaryIndexBuilder(
        source_db=args.source_db,
        artifact_root=args.association_artifact_root,
        serving_root=args.serving_root,
        workers=args.workers,
        progress_interval=args.progress_interval,
    ).build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
