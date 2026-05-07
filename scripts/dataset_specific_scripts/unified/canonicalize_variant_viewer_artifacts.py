#!/usr/bin/env python3
"""Canonicalize legacy Variant Viewer artifacts against DataHub phenotype registry."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.variant_viewer_canonicalization import (  # noqa: E402
    backup_tree,
    canonicalize_variant_viewer_artifacts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Clean macOS hidden raw files and canonicalize legacy Variant Viewer "
            "phenotype artifact directories/row labels."
        )
    )
    parser.add_argument(
        "--variant-viewer-root",
        default="analyzed_data/variant_viewer",
        help="Variant Viewer artifact root containing overall/ and phenotypes/.",
    )
    parser.add_argument(
        "--phenotype-tree-json",
        default="config/phenotype_tree.json",
        help="Canonical phenotype tree JSON.",
    )
    parser.add_argument(
        "--raw-root",
        default="raw_data",
        help="Raw data root scanned for macOS hidden resource-fork files.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=None,
        help="Checkpoint JSON path. Defaults to .canonicalize_checkpoint.json under the artifact root.",
    )
    parser.add_argument(
        "--report-path",
        default="analyzed_data/variant_viewer/canonicalization_report.json",
        help="Final JSON report path.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing files.")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Start from a fresh checkpoint.")
    parser.add_argument(
        "--remove-hidden-raw",
        action="store_true",
        help="Remove ._* and .DS_Store files under --raw-root.",
    )
    parser.add_argument(
        "--fail-on-unknown",
        action="store_true",
        help="Exit non-zero if non-canonical phenotypes remain after alias resolution.",
    )
    parser.add_argument(
        "--backup-root",
        default=None,
        help="Optional backup directory. Copies the full Variant Viewer root before in-place writes.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable INFO logging.")
    return parser.parse_args()


def configure_logging(verbose: bool) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("datahub.variant_viewer_canonicalization")


def resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def main() -> int:
    args = parse_args()
    logger = configure_logging(args.verbose)

    variant_viewer_root = resolve_path(args.variant_viewer_root)
    if args.backup_root and not args.dry_run:
        backup_path = backup_tree(variant_viewer_root, resolve_path(args.backup_root))
        logger.info("Variant Viewer artifact backup complete: path=%s", backup_path)

    summary = canonicalize_variant_viewer_artifacts(
        variant_viewer_root=variant_viewer_root,
        phenotype_tree_json=resolve_path(args.phenotype_tree_json),
        raw_root=resolve_path(args.raw_root),
        checkpoint_path=resolve_path(args.checkpoint_path) if args.checkpoint_path else None,
        report_path=resolve_path(args.report_path),
        dry_run=args.dry_run,
        remove_hidden_raw=args.remove_hidden_raw,
        reset_checkpoint=args.reset_checkpoint,
        fail_on_unknown=args.fail_on_unknown,
        logger=logger,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
