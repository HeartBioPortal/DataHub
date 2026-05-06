#!/usr/bin/env python3
"""Download gene-profile source snapshots and build gene_profile artifacts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.secondary_analyses.gene_profile import generate_gene_profile_artifacts
from datahub.secondary_analyses.gene_profile_sources import download_gene_profile_sources
from datahub.secondary_analyses.registry import SecondaryAnalysisRegistry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-command gene_profile pipeline: download resumable source snapshots, "
            "write checksum manifests, normalize GOA, and build gene profile artifacts."
        )
    )
    parser.add_argument("--raw-root", required=True, help="DataHub raw-data root for source snapshots.")
    parser.add_argument("--output-root", required=True, help="Secondary-analysis output root.")
    parser.add_argument(
        "--release",
        default="latest",
        help="Snapshot release label under raw-root/gene_profile/<release>.",
    )
    parser.add_argument(
        "--protein-context-root",
        default=None,
        help="Optional existing protein_context artifact root to fold into gene_profile.",
    )
    parser.add_argument(
        "--include-genes",
        default="",
        help="Optional comma-separated gene symbols/HGNC IDs for smoke builds.",
    )
    parser.add_argument(
        "--include-genes-file",
        default=None,
        help="Optional newline-delimited gene symbols/HGNC IDs for smoke builds.",
    )
    parser.add_argument("--secondary-manifests-dir", default=None)
    parser.add_argument("--force-download", action="store_true", help="Redownload sources even when checksummed files exist.")
    parser.add_argument("--skip-download", action="store_true", help="Build from existing local snapshots only.")
    parser.add_argument("--skip-goa", action="store_true", help="Skip GOA download and compact GO term normalization.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def _setup_logger(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("datahub.gene_profile_pipeline")


def _parse_gene_filter(args: argparse.Namespace) -> set[str] | None:
    genes: set[str] = set()
    if args.include_genes:
        genes.update(item.strip().upper() for item in args.include_genes.split(",") if item.strip())
    if args.include_genes_file:
        genes.update(
            line.strip().upper()
            for line in Path(args.include_genes_file).read_text().splitlines()
            if line.strip()
        )
    return genes or None


def main() -> int:
    args = parse_args()
    logger = _setup_logger(args.log_level)
    if args.skip_download:
        from datahub.secondary_analyses.gene_profile_sources import resolve_snapshot_paths

        paths = resolve_snapshot_paths(args.raw_root, release=args.release)
    else:
        paths = download_gene_profile_sources(
            raw_root=args.raw_root,
            release=args.release,
            force=args.force_download,
            include_goa=not args.skip_goa,
            logger=logger,
        )

    manifest = SecondaryAnalysisRegistry(args.secondary_manifests_dir).load("gene_profile")
    rows = generate_gene_profile_artifacts(
        hgnc_path=paths.hgnc_path,
        ncbi_gene_summary_path=paths.ncbi_summary_path,
        uniprot_path=paths.uniprot_path,
        go_annotation_path=None if args.skip_goa else paths.goa_compact_path,
        protein_context_root=args.protein_context_root,
        output_root=args.output_root,
        manifest=manifest,
        include_genes=_parse_gene_filter(args),
    )
    result = {
        "mode": "gene_profile_pipeline",
        "release": args.release,
        "raw_snapshot_manifest": str(paths.manifest_path),
        "output_root": str(Path(args.output_root)),
        "rows": len(rows),
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
