#!/usr/bin/env python3
"""Build a JSON QA report for DataHub release artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.artifact_qa import build_artifact_qa_report, write_artifact_qa_report  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize source catalog status, published outputs, and DuckDB artifacts."
    )
    parser.add_argument("--published-root", default=None, help="Published output root.")
    parser.add_argument("--serving-db-path", default=None, help="Serving DuckDB path.")
    parser.add_argument("--working-db-path", default=None, help="Working DuckDB path.")
    parser.add_argument("--manifests-dir", default=None, help="Optional source manifests directory.")
    parser.add_argument("--output-json", default=None, help="Optional report path. Defaults to stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_artifact_qa_report(
        published_root=args.published_root,
        serving_db_path=args.serving_db_path,
        working_db_path=args.working_db_path,
        manifests_dir=args.manifests_dir,
    )
    if args.output_json:
        write_artifact_qa_report(report, args.output_json)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
