#!/usr/bin/env python3
"""Manage the DataHub working DuckDB lifecycle schema."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.prep import RawAssociationPreparationProfileLoader  # noqa: E402
from datahub.working_duckdb import (  # noqa: E402
    ANALYSIS_READY_ASSOCIATION_TABLE,
    ensure_working_schema,
    expected_column_groups_from_prep_profile,
    expected_columns_from_prep_profile,
    load_source_normalized_association_csv,
    materialize_analysis_ready_association_from_points,
    register_raw_release,
)

try:  # pragma: no cover - import error exercised operationally.
    import duckdb
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "duckdb is required for manage_working_duckdb.py. Install DataHub requirements first."
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage raw-release, source-normalized, and analysis-ready tables in a DataHub working DuckDB."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Create lifecycle tables in the working DuckDB.")
    init.add_argument("--db-path", required=True)

    register = subparsers.add_parser(
        "register-raw-release",
        help="Inventory raw source files and write schema drift reports.",
    )
    register.add_argument("--db-path", required=True)
    register.add_argument("--source-id", required=True)
    register.add_argument("--release-id", required=True)
    register.add_argument("--modality", required=True)
    register.add_argument("--input-path", action="append", required=True)
    register.add_argument("--source-version", default="")
    register.add_argument("--release-label", default="")
    register.add_argument("--source-uri", default="")
    register.add_argument("--license", default="")
    register.add_argument("--access-notes", default="")
    register.add_argument("--metadata-json", default="{}")
    register.add_argument("--expected-column", action="append", default=[])
    register.add_argument(
        "--prep-profile",
        default=None,
        help="Optional prep profile whose raw source columns become the expected schema contract.",
    )
    register.add_argument("--profiles-dir", default=None)
    register.add_argument("--delimiter", default=None)
    register.add_argument(
        "--skip-checksum",
        action="store_true",
        help="Skip SHA-256 hashing for very large raw files.",
    )
    register.add_argument(
        "--fail-on-breaking-drift",
        action="store_true",
        help="Exit non-zero if any file is missing expected columns.",
    )

    load = subparsers.add_parser(
        "load-source-normalized-association",
        help="Load prepared association CSV rows into source_normalized_association.",
    )
    load.add_argument("--db-path", required=True)
    load.add_argument("--prepared-csv", required=True)
    load.add_argument("--source-id", required=True)
    load.add_argument("--release-id", required=True)
    load.add_argument("--append", action="store_true", help="Append instead of replacing rows from this file/source/release.")

    attach = subparsers.add_parser(
        "materialize-analysis-ready-association",
        help="Materialize analysis_ready_association from an existing canonical points table.",
    )
    attach.add_argument("--db-path", required=True)
    attach.add_argument("--source-table", default="mvp_association_points")
    attach.add_argument("--target-table", default=ANALYSIS_READY_ASSOCIATION_TABLE)
    attach.add_argument("--replace", action="store_true")

    return parser.parse_args()


def _load_metadata(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid --metadata-json: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("--metadata-json must decode to a JSON object.")
    return payload


def _expected_columns_from_args(args: argparse.Namespace) -> list[str]:
    columns = list(args.expected_column or [])
    if args.prep_profile:
        loader = RawAssociationPreparationProfileLoader(args.profiles_dir)
        profile = loader.load(args.prep_profile)
        columns.extend(expected_columns_from_prep_profile(profile))
    return columns


def _expected_column_groups_from_args(args: argparse.Namespace) -> list[tuple[str, ...]]:
    groups: list[tuple[str, ...]] = [(column,) for column in args.expected_column or []]
    if args.prep_profile:
        loader = RawAssociationPreparationProfileLoader(args.profiles_dir)
        profile = loader.load(args.prep_profile)
        groups.extend(expected_column_groups_from_prep_profile(profile))
    return groups


def main() -> int:
    args = parse_args()
    con = duckdb.connect(args.db_path)
    try:
        if args.command == "init":
            ensure_working_schema(con)
            print(json.dumps({"db_path": args.db_path, "status": "initialized"}, indent=2))
            return 0

        if args.command == "register-raw-release":
            registrations = register_raw_release(
                con,
                source_id=args.source_id,
                release_id=args.release_id,
                modality=args.modality,
                input_paths=args.input_path,
                expected_columns=_expected_columns_from_args(args) if not args.prep_profile else None,
                expected_column_groups=_expected_column_groups_from_args(args)
                if args.prep_profile
                else None,
                source_version=args.source_version,
                release_label=args.release_label,
                source_uri=args.source_uri,
                license=args.license,
                access_notes=args.access_notes,
                metadata=_load_metadata(args.metadata_json),
                compute_checksum=not args.skip_checksum,
                delimiter=args.delimiter,
            )
            status_counts: dict[str, int] = {}
            for registration in registrations:
                status_counts[registration.drift.status] = status_counts.get(registration.drift.status, 0) + 1
            payload = {
                "db_path": args.db_path,
                "source_id": args.source_id,
                "release_id": args.release_id,
                "modality": args.modality,
                "file_count": len(registrations),
                "status_counts": status_counts,
            }
            print(json.dumps(payload, indent=2, sort_keys=True))
            if args.fail_on_breaking_drift and status_counts.get("breaking"):
                return 2
            return 0

        if args.command == "load-source-normalized-association":
            inserted = load_source_normalized_association_csv(
                con,
                prepared_csv=args.prepared_csv,
                source_id=args.source_id,
                release_id=args.release_id,
                replace_file=not args.append,
            )
            print(json.dumps({"db_path": args.db_path, "inserted_rows": inserted}, indent=2))
            return 0

        if args.command == "materialize-analysis-ready-association":
            inserted = materialize_analysis_ready_association_from_points(
                con,
                source_table=args.source_table,
                target_table=args.target_table,
                replace=args.replace,
            )
            print(
                json.dumps(
                    {
                        "db_path": args.db_path,
                        "source_table": args.source_table,
                        "target_table": args.target_table,
                        "inserted_rows": inserted,
                    },
                    indent=2,
                )
            )
            return 0

        raise SystemExit(f"Unhandled command: {args.command}")
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
