#!/usr/bin/env python3
"""Backfill canonical transcript exons for legacy structural-variant artifacts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import (  # noqa: E402
    EnsemblRestClient,
    apply_structural_variant_exon_patch,
    enrich_structural_variant_exons,
    load_json_artifact,
    write_json_atomic,
)


logger = logging.getLogger(__name__)


def _read_gene_file(path: str | Path | None) -> list[str]:
    if not path:
        return []
    gene_path = Path(path)
    genes: list[str] = []
    for line in gene_path.read_text().splitlines():
        gene = line.strip()
        if gene and not gene.startswith("#"):
            genes.append(gene)
    return genes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill missing canonical transcript Exon arrays in a legacy structural_variants JSON "
            "using Ensembl lookup/id?expand=1 responses."
        ),
    )
    parser.add_argument("--input-json", required=True, help="Input structural_variants JSON/JSON.GZ/JSON.ZIP.")
    parser.add_argument(
        "--output-json",
        default="",
        help="Output enriched structural_variants JSON. Required unless --patch-output-json is used.",
    )
    parser.add_argument(
        "--patch-output-json",
        default="",
        help=(
            "Write only a gene-level exon patch JSON instead of the full artifact. "
            "Use this for partitioned sbatch fetch jobs."
        ),
    )
    parser.add_argument(
        "--patch-input-json",
        action="append",
        nargs="+",
        default=[],
        help="Patch JSON to apply to --input-json. Repeat or use a glob expanded by the shell.",
    )
    parser.add_argument("--report-path", default="", help="Optional run report JSON path.")
    parser.add_argument("--cache-path", default="", help="Persistent Ensembl cache JSON path.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="Per-request Ensembl timeout.")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between uncached Ensembl requests.",
    )
    parser.add_argument(
        "--gene",
        action="append",
        default=[],
        help="Optional gene allowlist entry. Repeat to add multiple genes.",
    )
    parser.add_argument("--gene-file", default="", help="Optional newline-delimited gene allowlist.")
    parser.add_argument(
        "--exclude-gene-file",
        default="",
        help="Optional newline-delimited gene denylist. Useful for explicitly excluding a known seed set.",
    )
    parser.add_argument(
        "--force-refresh-exons",
        action="store_true",
        help="Refresh genes even when their canonical transcript already has Exon entries.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch/report without mutating or writing output.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on the first Ensembl lookup failure.")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum genes to process in this job.")
    parser.add_argument("--progress-every", type=int, default=100, help="Log progress every N selected genes.")
    parser.add_argument("--unit-partitions", type=int, default=1, help="Total deterministic gene partitions.")
    parser.add_argument("--unit-partition-index", type=int, default=0, help="Zero-based partition index for this job.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


def _write_report(path: str | Path, report: dict[str, Any]) -> None:
    if not path:
        return
    write_json_atomic(path, report, indent=2, sort_keys=True)


def _apply_patches(args: argparse.Namespace, payload: dict[str, Any]) -> dict[str, Any]:
    if not args.output_json:
        raise ValueError("--output-json is required when applying patches")

    patch_reports: list[dict[str, Any]] = []
    patch_paths = [path for group in args.patch_input_json for path in group]
    for patch_path in patch_paths:
        patch = load_json_artifact(patch_path)
        patch_report = apply_structural_variant_exon_patch(payload, patch)
        patch_report["patch_path"] = str(patch_path)
        patch_reports.append(patch_report)

    if not args.dry_run:
        write_json_atomic(args.output_json, payload, indent=2, sort_keys=True)

    return {
        "mode": "apply",
        "input_json": args.input_json,
        "output_json": args.output_json,
        "dry_run": bool(args.dry_run),
        "patch_reports": patch_reports,
    }


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )

    logger.info("Loading structural variant artifact: %s", args.input_json)
    payload = load_json_artifact(args.input_json)
    if not isinstance(payload, dict):
        raise TypeError(f"Expected mapping JSON payload: {args.input_json}")

    if args.patch_input_json:
        report = _apply_patches(args, payload)
        _write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    if not args.patch_output_json and not args.output_json and not args.dry_run:
        raise ValueError("--output-json is required unless --patch-output-json or --dry-run is used")

    include_genes = [*args.gene, *_read_gene_file(args.gene_file)]
    exclude_genes = _read_gene_file(args.exclude_gene_file)
    client = EnsemblRestClient(
        cache_path=args.cache_path or None,
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
    )
    try:
        patch, backfill_report = enrich_structural_variant_exons(
            payload,
            ensembl_client=client,
            include_genes=include_genes or None,
            exclude_genes=exclude_genes or None,
            force_refresh_exons=args.force_refresh_exons,
            dry_run=args.dry_run or bool(args.patch_output_json),
            unit_partitions=args.unit_partitions,
            unit_partition_index=args.unit_partition_index,
            limit=args.limit,
            progress_every=args.progress_every,
            fail_fast=args.fail_fast,
        )
    finally:
        client.close()

    report = {
        "mode": "patch" if args.patch_output_json else "enrich",
        "input_json": args.input_json,
        "output_json": args.output_json or None,
        "patch_output_json": args.patch_output_json or None,
        "cache_path": args.cache_path or None,
        "report": backfill_report.to_dict(),
    }

    if args.patch_output_json and not args.dry_run:
        patch_payload = {
            "version": 1,
            "input_json": args.input_json,
            "report": backfill_report.to_dict(),
            "genes": patch,
        }
        write_json_atomic(args.patch_output_json, patch_payload, indent=2, sort_keys=True)
    elif args.output_json and not args.dry_run:
        write_json_atomic(args.output_json, payload, indent=2, sort_keys=True)

    _write_report(args.report_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
