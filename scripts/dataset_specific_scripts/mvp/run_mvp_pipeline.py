#!/usr/bin/env python3
"""Run DataHub pipeline for Million Veteran Program association datasets."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import MissingFieldStrategy, build_association_contract  # noqa: E402
from datahub.adapters import MVPAssociationAdapter, PhenotypeMapper  # noqa: E402
from datahub.pipeline import DataHubPipeline  # noqa: E402
from datahub.publishers import (  # noqa: E402
    LegacyAssociationPublisher,
    LegacyRedisPublisher,
    PhenotypeRollupPublisher,
)
from datahub.storage import DuckDBParquetStorage  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest MVP association files and publish legacy + rollup outputs."
    )
    parser.add_argument(
        "--input-path",
        action="append",
        dest="input_paths",
        required=True,
        help="Input file, directory, or glob. Repeat for multiple paths.",
    )
    parser.add_argument("--output-root", required=True, help="Output root for JSON payloads.")
    parser.add_argument(
        "--dataset-id",
        default="hbp_mvp_association",
        help="Dataset identifier stored in canonical records.",
    )
    parser.add_argument(
        "--required-fields",
        default="gene_id,variant_id,phenotype",
        help="Comma-separated required canonical fields.",
    )
    parser.add_argument(
        "--axis-missing-strategy",
        default="exclude",
        choices=[item.value for item in MissingFieldStrategy],
        help="How missing axis fields are handled.",
    )
    parser.add_argument(
        "--phenotype-map-json",
        default=None,
        help="Optional JSON map for phenotype->dataset/category overrides.",
    )
    parser.add_argument(
        "--fallback-dataset-type",
        default="CVD",
        help="Dataset type used when phenotype is not mapped.",
    )
    parser.add_argument(
        "--dataset-types",
        default=None,
        help="Optional comma-separated dataset filter (for example: CVD,TRAIT).",
    )
    parser.add_argument(
        "--rollup-tree-json",
        default=None,
        help="Optional phenotype tree JSON path for rollup grouping.",
    )
    parser.add_argument(
        "--disable-rollup",
        action="store_true",
        help="Skip rollup output generation.",
    )
    parser.add_argument(
        "--ancestry-precision",
        type=int,
        default=6,
        help="Round ancestry values to this precision in published payloads.",
    )
    parser.add_argument(
        "--duckdb-path",
        default=None,
        help="Optional DuckDB output path for canonical records.",
    )
    parser.add_argument(
        "--parquet-path",
        default=None,
        help="Optional Parquet output path for canonical records.",
    )
    parser.add_argument(
        "--publish-redis",
        action="store_true",
        help="Publish generated legacy payloads into Redis.",
    )
    parser.add_argument(
        "--redis-strict",
        action="store_true",
        help="Fail fast if Redis publishing fails.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="CSV chunk size for MVP ingest.",
    )
    return parser.parse_args()


def _load_mapper(args: argparse.Namespace) -> PhenotypeMapper:
    if args.phenotype_map_json:
        return PhenotypeMapper.from_json(
            args.phenotype_map_json,
            fallback_dataset_type=args.fallback_dataset_type,
        )
    try:
        return PhenotypeMapper.from_hbp_backend(
            fallback_dataset_type=args.fallback_dataset_type
        )
    except Exception:
        return PhenotypeMapper(mapping={}, fallback_dataset_type=args.fallback_dataset_type)


def main() -> int:
    args = parse_args()
    mapper = _load_mapper(args)

    include_dataset_types = (
        {item.strip().upper() for item in args.dataset_types.split(",") if item.strip()}
        if args.dataset_types
        else None
    )

    adapter = MVPAssociationAdapter(
        input_paths=args.input_paths,
        dataset_id=args.dataset_id,
        phenotype_mapper=mapper,
        include_dataset_types=include_dataset_types,
        chunksize=args.chunksize,
    )

    contract = build_association_contract(
        dataset_type="ASSOCIATION",
        required_fields={item.strip() for item in args.required_fields.split(",") if item.strip()},
        axis_missing_strategy=MissingFieldStrategy(args.axis_missing_strategy),
    )

    storage = None
    if bool(args.duckdb_path) ^ bool(args.parquet_path):
        raise ValueError("Provide both --duckdb-path and --parquet-path, or neither.")
    if args.duckdb_path and args.parquet_path:
        storage = DuckDBParquetStorage(
            db_path=args.duckdb_path,
            parquet_path=args.parquet_path,
        )

    publishers = [
        LegacyAssociationPublisher(
            output_root=args.output_root,
            ancestry_value_precision=args.ancestry_precision,
            deduplicate_ancestry_points=True,
        )
    ]
    if not args.disable_rollup:
        publishers.append(
            PhenotypeRollupPublisher(
                output_root=args.output_root,
                tree_json_path=args.rollup_tree_json,
                ancestry_value_precision=args.ancestry_precision,
                deduplicate_variants=True,
            )
        )
    if args.publish_redis:
        publishers.append(
            LegacyRedisPublisher(
                output_root=args.output_root,
                strict=args.redis_strict,
            )
        )

    report = DataHubPipeline(
        contract=contract,
        adapters=[adapter],
        storage=storage,
        publishers=publishers,
    ).run()

    print(
        json.dumps(
            {
                "adapter": "mvp_association",
                "dataset_id": args.dataset_id,
                "ingested_records": report.ingested_records,
                "validated_records": report.validated_records,
                "dropped_records": report.dropped_records,
                "issues": len(report.issues),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
