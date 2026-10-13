#!/usr/bin/env python3
"""Publish bounded serving partitions for an association evidence v2 sidecar."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from datahub.association_evidence_v2.serving import AssociationEvidenceV2ServingBuilder


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--intermediate-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--variant-bucket-characters",
        type=int,
        choices=(2, 3, 4),
        default=3,
        help="Hexadecimal SHA-256 prefix width for variant-scoped runtime partitions.",
    )
    parser.add_argument("--progress-interval", type=int, default=60)
    parser.add_argument(
        "--coarse-serving-root",
        type=Path,
        help=(
            "Existing verified lower-width serving package used as the bounded "
            "input when --variant-bucket-characters is greater than 2."
        ),
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    args.output_root.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(args.output_root / "serving-build.log", mode="a"),
        ],
    )
    builder = AssociationEvidenceV2ServingBuilder(
        source_db=args.source_db,
        intermediate_root=args.intermediate_root,
        output_root=args.output_root,
        memory_limit=args.memory_limit,
        threads=args.threads,
        variant_bucket_characters=args.variant_bucket_characters,
        progress_interval=args.progress_interval,
        coarse_serving_root=args.coarse_serving_root,
    )
    print(json.dumps(builder.build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
