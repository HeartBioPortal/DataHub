#!/usr/bin/env python3
"""Prepare raw association CSVs into a standardized schema."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import (  # noqa: E402
    AssociationRawPreparer,
    RawAssociationPreparationProfileLoader,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare non-standard raw association data into stable DataHub input schema."
    )
    parser.add_argument("--input-csv", required=True, help="Raw source CSV path")
    parser.add_argument("--output-csv", required=True, help="Prepared output CSV path")
    parser.add_argument(
        "--profile",
        default="legacy_cvd_raw",
        help="Preparation profile name from config/prep_profiles",
    )
    parser.add_argument(
        "--profile-path",
        default=None,
        help="Optional explicit profile JSON path (overrides --profile)",
    )
    parser.add_argument(
        "--profiles-dir",
        default=None,
        help="Optional custom profile directory",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="CSV chunk size to control memory usage.",
    )
    parser.add_argument(
        "--ancestry-precision",
        type=int,
        default=6,
        help="Round ancestry MAF values to this many decimals. Use -1 to disable rounding.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile_loader = RawAssociationPreparationProfileLoader(args.profiles_dir)
    profile_ref = args.profile_path if args.profile_path else args.profile
    profile = profile_loader.load(profile_ref)

    precision = None if args.ancestry_precision < 0 else args.ancestry_precision
    preparer = AssociationRawPreparer(
        profile=profile,
        chunksize=args.chunksize,
        ancestry_precision=precision,
    )

    report = preparer.prepare_csv(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
    )

    payload = {
        "profile": profile.name,
        "dataset_type": profile.dataset_type,
        "input_rows": report.input_rows,
        "output_rows": report.output_rows,
        "dropped_rows": report.dropped_rows,
        "output_csv": str(report.output_path),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
