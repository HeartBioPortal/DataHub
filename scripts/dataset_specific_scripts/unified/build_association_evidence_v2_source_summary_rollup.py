#!/usr/bin/env python3
"""Build default-search rollups from a completed evidence-v2 source-summary index."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from datahub.association_evidence_v2.source_summary_rollup import SourceSummaryRollupBuilder


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serving-root", required=True, type=Path)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--progress-interval", type=int, default=1)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    result = SourceSummaryRollupBuilder(
        serving_root=args.serving_root,
        threads=args.threads,
        memory_limit=args.memory_limit,
        progress_interval=args.progress_interval,
        logger=logging.getLogger("datahub.association_evidence_v2.source_summary_rollup"),
    ).build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
