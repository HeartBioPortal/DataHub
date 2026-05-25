#!/usr/bin/env python3
"""Run HeartBioPortal expression ingestion/export jobs."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.expression.config import ExpressionBuildConfig
from datahub.expression.curation import write_curation_manifest
from datahub.expression.geo_discovery import (
    GeoStudyCandidate,
    discover_geo_cvd_candidates,
    discover_geo_cvd_candidates_from_phenotype_tree,
    download_geometadb_sqlite,
)
from datahub.expression.legacy_cardioquilt import read_cardioquilt_csv
from datahub.expression.pipeline import build_expression_outputs
from datahub.expression.v3_results import read_expression_v3_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    legacy = subparsers.add_parser(
        "legacy-cardioquilt",
        help="Normalize legacy cardioquilt_CREEDS_GEO.csv into expression v2 artifacts.",
    )
    legacy.add_argument("--input-csv", required=True)
    legacy.add_argument("--output-root", required=True)
    legacy.add_argument("--duckdb-path", default=None)
    legacy.add_argument("--adjusted-p-value-threshold", type=float, default=0.05)
    legacy.add_argument("--pipeline-version", default="1")

    discover = subparsers.add_parser(
        "discover-geo",
        help="Discover candidate CVD GEO studies from a local GEOmetadb.sqlite file.",
    )
    discover.add_argument("--geometadb-sqlite", required=True)
    discover.add_argument("--output-csv", required=True)
    discover.add_argument("--limit-per-term", type=int, default=None)
    discover.add_argument(
        "--phenotype-tree-json",
        default=str(REPO_ROOT / "config" / "phenotype_tree.json"),
        help="HBP phenotype tree used to seed CVD search terms and phenotype paths.",
    )
    discover.add_argument(
        "--no-phenotype-tree",
        action="store_true",
        help="Use the fallback built-in CVD term list instead of config/phenotype_tree.json.",
    )
    discover.add_argument(
        "--curation-output-csv",
        default=None,
        help="Optional expression v3 curation manifest seeded from discovered candidates.",
    )

    download = subparsers.add_parser(
        "download-geometadb",
        help="Download and unpack GEOmetadb.sqlite.gz for local GEO discovery.",
    )
    download.add_argument("--output-sqlite", required=True)
    download.add_argument("--url", default=None)
    download.add_argument("--replace", action="store_true")

    manifest = subparsers.add_parser(
        "build-curation-manifest",
        help="Convert discovered GEO candidate CSV into a reviewable expression v3 curation manifest.",
    )
    manifest.add_argument("--candidates-csv", required=True)
    manifest.add_argument("--output-csv", required=True)

    geo_de = subparsers.add_parser(
        "run-approved-geo-de",
        help="Run GEOquery/limma DE for approved curation rows with explicit case/control samples.",
    )
    geo_de.add_argument("--curation-csv", required=True)
    geo_de.add_argument("--output-csv", required=True)
    geo_de.add_argument("--cache-dir", required=True)
    geo_de.add_argument("--adjusted-p-value-threshold", type=float, default=0.05)
    geo_de.add_argument("--rscript-path", default="Rscript")
    geo_de.add_argument("--force-normalize", action="store_true")

    import_v3 = subparsers.add_parser(
        "import-v3-results",
        help="Normalize public expression v3 result rows into DataHub serving artifacts.",
    )
    import_v3.add_argument("--input-csv", required=True)
    import_v3.add_argument("--output-root", required=True)
    import_v3.add_argument("--duckdb-path", default=None)
    import_v3.add_argument("--adjusted-p-value-threshold", type=float, default=0.05)
    import_v3.add_argument("--pipeline-version", default="3")

    return parser.parse_args()


def _run_legacy_cardioquilt(args: argparse.Namespace) -> int:
    config = ExpressionBuildConfig(
        adjusted_p_value_threshold=args.adjusted_p_value_threshold,
        significance_threshold_label=f"adjusted_p_value < {args.adjusted_p_value_threshold}",
        pipeline_version=str(args.pipeline_version),
    )
    rows = read_cardioquilt_csv(args.input_csv, config=config)
    manifest = build_expression_outputs(
        rows=rows,
        output_root=args.output_root,
        config=config,
        duckdb_path=args.duckdb_path,
    )
    print(json.dumps(manifest, indent=2))
    return 0


def _run_discover_geo(args: argparse.Namespace) -> int:
    phenotype_tree_path = Path(args.phenotype_tree_json)
    if not args.no_phenotype_tree and phenotype_tree_path.exists():
        candidates = discover_geo_cvd_candidates_from_phenotype_tree(
            args.geometadb_sqlite,
            phenotype_tree_path,
            limit_per_term=args.limit_per_term,
        )
        discovery_terms_source = str(phenotype_tree_path)
    else:
        candidates = discover_geo_cvd_candidates(
            args.geometadb_sqlite,
            limit_per_term=args.limit_per_term,
        )
        discovery_terms_source = "built_in_default_cvd_terms"
    output = Path(args.output_csv)
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = [candidate.to_dict() for candidate in candidates]
    fieldnames = list(rows[0]) if rows else [
        "study_accession",
        "title",
        "summary",
        "pubmed_id",
        "organism",
        "platform",
        "matched_term",
        "overall_design",
        "source_url",
    ]
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    payload = {
        "output_csv": str(output),
        "candidate_count": len(rows),
        "discovery_terms_source": discovery_terms_source,
    }
    if args.curation_output_csv:
        curation_path = write_curation_manifest(
            candidates=candidates,
            output_csv=args.curation_output_csv,
        )
        payload["curation_output_csv"] = str(curation_path)
    print(json.dumps(payload, indent=2))
    return 0


def _candidate_from_dict(row: dict[str, str]) -> GeoStudyCandidate:
    return GeoStudyCandidate(
        study_accession=row.get("study_accession") or row.get("gse") or "",
        title=row.get("title") or None,
        summary=row.get("summary") or None,
        pubmed_id=row.get("pubmed_id") or None,
        organism=row.get("organism") or None,
        platform=row.get("platform") or None,
        matched_term=row.get("matched_term") or row.get("disease_name") or "",
        phenotype_tree_path=row.get("phenotype_tree_path") or None,
        overall_design=row.get("overall_design") or None,
        source_url=row.get("source_url")
        or f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={row.get('study_accession', '')}",
    )


def _run_download_geometadb(args: argparse.Namespace) -> int:
    kwargs = {
        "output_path": args.output_sqlite,
        "replace": args.replace,
    }
    if args.url:
        kwargs["url"] = args.url
    path = download_geometadb_sqlite(**kwargs)
    print(json.dumps({"geometadb_sqlite": str(path)}, indent=2))
    return 0


def _run_build_curation_manifest(args: argparse.Namespace) -> int:
    with Path(args.candidates_csv).open(newline="") as handle:
        candidates = [_candidate_from_dict(row) for row in csv.DictReader(handle)]
    path = write_curation_manifest(candidates=candidates, output_csv=args.output_csv)
    print(json.dumps({"curation_output_csv": str(path), "row_count": len(candidates)}, indent=2))
    return 0


def _run_approved_geo_de(args: argparse.Namespace) -> int:
    script_path = Path(__file__).with_name("run_geo_limma_de.R")
    command = [
        args.rscript_path,
        str(script_path),
        "--curation-csv",
        args.curation_csv,
        "--output-csv",
        args.output_csv,
        "--cache-dir",
        args.cache_dir,
        "--adjusted-p-value-threshold",
        str(args.adjusted_p_value_threshold),
    ]
    if args.force_normalize:
        command.append("--force-normalize")
    subprocess.run(command, check=True)
    print(json.dumps({"output_csv": args.output_csv, "script": str(script_path)}, indent=2))
    return 0


def _run_import_v3_results(args: argparse.Namespace) -> int:
    config = ExpressionBuildConfig(
        adjusted_p_value_threshold=args.adjusted_p_value_threshold,
        significance_threshold_label=f"adjusted_p_value < {args.adjusted_p_value_threshold}",
        analysis_method="expression_v3_public_geo",
        pipeline_name="datahub_expression_v3",
        pipeline_version=str(args.pipeline_version),
        artifact_subdir="expression_v3",
        source_database="GEO",
    )
    rows = read_expression_v3_results(args.input_csv, config=config)
    manifest = build_expression_outputs(
        rows=rows,
        output_root=args.output_root,
        config=config,
        duckdb_path=args.duckdb_path,
    )
    print(json.dumps(manifest, indent=2))
    return 0


def main() -> int:
    args = parse_args()
    if args.command == "legacy-cardioquilt":
        return _run_legacy_cardioquilt(args)
    if args.command == "discover-geo":
        return _run_discover_geo(args)
    if args.command == "download-geometadb":
        return _run_download_geometadb(args)
    if args.command == "build-curation-manifest":
        return _run_build_curation_manifest(args)
    if args.command == "run-approved-geo-de":
        return _run_approved_geo_de(args)
    if args.command == "import-v3-results":
        return _run_import_v3_results(args)
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
