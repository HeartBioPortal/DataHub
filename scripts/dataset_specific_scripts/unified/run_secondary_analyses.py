#!/usr/bin/env python3
"""Generate and apply secondary-analysis artifacts for serving."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.secondary_analyses.expression import generate_expression_artifacts
from datahub.secondary_analyses.protein_context import generate_protein_context_artifacts
from datahub.secondary_analyses.registry import SecondaryAnalysisRegistry
from datahub.secondary_analyses.serving import apply_secondary_analysis_artifacts
from datahub.secondary_analyses.sga import generate_sga_artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and apply secondary-analysis artifacts."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser(
        "generate",
        help="Generate secondary-analysis artifacts from source inputs.",
    )
    generate.add_argument("--analyses", required=True, help="Comma-separated analysis IDs.")
    generate.add_argument("--output-root", required=True, help="Artifact output root.")
    generate.add_argument(
        "--association-db-path",
        default=None,
        help="Unified association DuckDB path used by derived analyses such as SGA.",
    )
    generate.add_argument(
        "--association-table",
        default="mvp_association_points",
        help="Unified association points table name for derived analyses.",
    )
    generate.add_argument(
        "--expression-json-path",
        default=None,
        help="Path to expression.json for the expression secondary analysis.",
    )
    generate.add_argument(
        "--variant-viewer-root",
        default=None,
        help=(
            "Variant-viewer artifact root used by protein_context to infer genes, "
            "isoform IDs, and protein lengths. Usually analyzed_data/variant_viewer."
        ),
    )
    generate.add_argument(
        "--protein-context-cache-path",
        default=None,
        help="Persistent JSON API cache for protein_context Ensembl/Proteins/InterPro calls.",
    )
    generate.add_argument(
        "--protein-context-species",
        default="homo_sapiens",
        help="Ensembl species for protein_context generation.",
    )
    generate.add_argument(
        "--protein-context-max-isoforms",
        type=int,
        default=12,
        help="Maximum Ensembl protein-coding isoforms to emit per gene for protein_context.",
    )
    generate.add_argument(
        "--protein-context-timeout-seconds",
        type=float,
        default=30.0,
        help="Per-request timeout for protein_context API calls.",
    )
    generate.add_argument(
        "--protein-context-sleep-seconds",
        type=float,
        default=0.1,
        help="Delay after uncached protein_context API requests.",
    )
    generate.add_argument(
        "--protein-context-limit",
        type=int,
        default=None,
        help="Optional maximum genes to process in this protein_context job.",
    )
    generate.add_argument(
        "--protein-context-progress-every",
        type=int,
        default=25,
        help="Log protein_context progress every N selected genes.",
    )
    generate.add_argument(
        "--protein-context-skip-ebi-proteins",
        action="store_true",
        help="Skip EBI Proteins API feature enrichment.",
    )
    generate.add_argument(
        "--protein-context-skip-interpro",
        action="store_true",
        help="Skip InterPro feature enrichment.",
    )
    generate.add_argument(
        "--include-genes",
        default="",
        help="Optional comma-separated gene list filter.",
    )
    generate.add_argument(
        "--include-genes-file",
        default=None,
        help="Optional newline-delimited file containing genes to include.",
    )
    generate.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing artifact directories for the requested analyses before regenerating.",
    )
    generate.add_argument(
        "--unit-partitions",
        type=int,
        default=1,
        help=(
            "Number of deterministic gene partitions for derived analyses. "
            "Use with --unit-partition-index to split large SGA runs across Slurm jobs."
        ),
    )
    generate.add_argument(
        "--unit-partition-index",
        type=int,
        default=0,
        help="Zero-based partition index for this secondary-analysis generation job.",
    )
    generate.add_argument(
        "--duckdb-threads",
        type=int,
        default=0,
        help="DuckDB thread count for derived analyses. 0 keeps DuckDB default.",
    )
    generate.add_argument(
        "--duckdb-memory-limit",
        default=None,
        help="Optional DuckDB memory limit for derived analyses, for example 24GB.",
    )
    generate.add_argument(
        "--duckdb-temp-directory",
        default=None,
        help="Optional DuckDB temp spill directory for derived analyses.",
    )
    generate.add_argument(
        "--secondary-manifests-dir",
        default=None,
        help="Optional custom secondary-analysis manifest directory.",
    )
    generate.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )

    apply = subparsers.add_parser(
        "apply",
        help="Apply generated secondary-analysis artifacts to an existing serving DuckDB.",
    )
    apply.add_argument("--analyses", required=True, help="Comma-separated analysis IDs.")
    apply.add_argument(
        "--secondary-root",
        required=True,
        help="Secondary artifact root containing final/<analysis>/genes.",
    )
    apply.add_argument(
        "--serving-db-path",
        required=True,
        help="Existing serving DuckDB path to update in place.",
    )
    apply.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Insert batch size for serving DB updates.",
    )
    apply.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Log apply progress every N artifact files.",
    )
    apply.add_argument(
        "--secondary-manifests-dir",
        default=None,
        help="Optional custom secondary-analysis manifest directory.",
    )
    apply.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )

    return parser.parse_args()


def _setup_logger(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("datahub.secondary_analyses")


def _parse_analyses(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _parse_gene_filter(args: argparse.Namespace) -> set[str] | None:
    genes: set[str] = set()
    if getattr(args, "include_genes", None):
        genes.update(
            item.strip().upper()
            for item in str(args.include_genes).split(",")
            if item.strip()
        )
    include_file = getattr(args, "include_genes_file", None)
    if include_file:
        path = Path(include_file)
        genes.update(
            line.strip().upper()
            for line in path.read_text().splitlines()
            if line.strip()
        )
    return genes or None


def _row_count(result: object) -> int:
    if isinstance(result, int):
        return result
    return len(result)  # type: ignore[arg-type]


def _remove_existing_artifacts(output_root: Path, artifact_subdir: str) -> None:
    target = output_root / "final" / artifact_subdir
    if target.exists():
        shutil.rmtree(target)


def _run_generate(args: argparse.Namespace) -> int:
    logger = _setup_logger(args.log_level)
    registry = SecondaryAnalysisRegistry(args.secondary_manifests_dir)
    output_root = Path(args.output_root)
    include_genes = _parse_gene_filter(args)

    summaries: dict[str, int] = {}
    for analysis_id in _parse_analyses(args.analyses):
        manifest = registry.load(analysis_id)
        if args.replace:
            _remove_existing_artifacts(output_root, manifest.artifact_subdir)

        if analysis_id == "expression":
            if not args.expression_json_path:
                raise ValueError("expression generation requires --expression-json-path")
            result = generate_expression_artifacts(
                expression_json_path=args.expression_json_path,
                output_root=output_root,
                manifest=manifest,
                include_genes=include_genes,
            )
        elif analysis_id == "sga":
            if not args.association_db_path:
                raise ValueError("sga generation requires --association-db-path")
            result = generate_sga_artifacts(
                db_path=args.association_db_path,
                source_table=args.association_table,
                output_root=output_root,
                manifest=manifest,
                include_genes=include_genes,
                unit_partitions=args.unit_partitions,
                unit_partition_index=args.unit_partition_index,
                duckdb_threads=args.duckdb_threads,
                duckdb_memory_limit=args.duckdb_memory_limit,
                duckdb_temp_directory=args.duckdb_temp_directory,
            )
        elif analysis_id == "protein_context":
            result = generate_protein_context_artifacts(
                output_root=output_root,
                manifest=manifest,
                include_genes=include_genes,
                variant_viewer_root=args.variant_viewer_root,
                species=args.protein_context_species,
                cache_path=args.protein_context_cache_path,
                timeout_seconds=args.protein_context_timeout_seconds,
                sleep_seconds=args.protein_context_sleep_seconds,
                max_isoforms=args.protein_context_max_isoforms,
                use_ebi_proteins=not args.protein_context_skip_ebi_proteins,
                use_interpro=not args.protein_context_skip_interpro,
                unit_partitions=args.unit_partitions,
                unit_partition_index=args.unit_partition_index,
                limit=args.protein_context_limit,
                progress_every=args.protein_context_progress_every,
            )
        else:
            raise ValueError(f"Unsupported secondary analysis: {analysis_id}")

        count = _row_count(result)
        summaries[analysis_id] = count
        logger.info(
            "Secondary analysis generated: analysis=%s rows=%d output_root=%s",
            analysis_id,
            count,
            output_root,
        )

    print(json.dumps({"mode": "generate", "output_root": str(output_root), "rows": summaries}, indent=2))
    return 0


def _run_apply(args: argparse.Namespace) -> int:
    logger = _setup_logger(args.log_level)
    registry = SecondaryAnalysisRegistry(args.secondary_manifests_dir)

    try:
        import duckdb  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("duckdb is required to apply secondary-analysis artifacts") from exc

    connection = duckdb.connect(str(Path(args.serving_db_path)))
    try:
        summaries: dict[str, int] = {}
        for analysis_id in _parse_analyses(args.analyses):
            manifest = registry.load(analysis_id)
            inserted = apply_secondary_analysis_artifacts(
                connection,
                input_root=args.secondary_root,
                manifest=manifest,
                batch_size=args.batch_size,
                logger=logger,
                progress_interval=args.progress_interval,
            )
            summaries[analysis_id] = inserted
            logger.info(
                "Secondary analysis applied: analysis=%s rows=%d db=%s",
                analysis_id,
                inserted,
                args.serving_db_path,
            )
        print(
            json.dumps(
                {"mode": "apply", "serving_db_path": str(args.serving_db_path), "rows": summaries},
                indent=2,
            )
        )
    finally:
        connection.close()
    return 0


def main() -> int:
    args = parse_args()
    if args.command == "generate":
        return _run_generate(args)
    if args.command == "apply":
        return _run_apply(args)
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
