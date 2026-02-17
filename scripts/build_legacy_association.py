#!/usr/bin/env python3
"""Build HeartBioPortal legacy association outputs via the new DataHub pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import MissingFieldStrategy, build_association_contract
from datahub.adapters import LegacyAssociationCsvAdapter, PhenotypeMapper
from datahub.pipeline import DataHubPipeline
from datahub.publishers import LegacyAssociationPublisher, LegacyRedisPublisher
from datahub.storage import DuckDBParquetStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run DataHub against the legacy HBP aggregated association CSV."
    )
    parser.add_argument("--csv-path", required=True, help="Path to final_aggregated_results.csv")
    parser.add_argument(
        "--output-root",
        required=True,
        help="Root output path that will receive association/final/* JSON files",
    )
    parser.add_argument(
        "--dataset-id",
        default="hbp_legacy_association",
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
        help="How missing axis fields are handled for association views.",
    )
    parser.add_argument(
        "--phenotype-map-json",
        default=None,
        help="Optional JSON mapping to override phenotype->(dataset_type, category).",
    )
    parser.add_argument(
        "--fallback-dataset-type",
        default="CVD",
        help="Dataset type to use when a phenotype is not mapped.",
    )
    parser.add_argument(
        "--dataset-types",
        default=None,
        help="Optional comma-separated filter (for example: CVD,TRAIT).",
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
        "--keep-unknown-axis",
        action="store_true",
        help="Include an 'Unknown' axis bucket instead of excluding missing axis values.",
    )
    parser.add_argument(
        "--publish-redis",
        action="store_true",
        help="Publish generated association JSON into the legacy Redis schema.",
    )
    parser.add_argument(
        "--ancestry-precision",
        type=int,
        default=None,
        help="Optional rounding precision for ancestry numeric values (for smaller JSON output).",
    )
    parser.add_argument(
        "--disable-ancestry-dedup",
        action="store_true",
        help="Keep duplicate ancestry points per rsid if source has repeated rows.",
    )
    parser.add_argument(
        "--redis-strict",
        action="store_true",
        help="Fail fast if Redis publishing fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.phenotype_map_json:
        mapper = PhenotypeMapper.from_json(
            args.phenotype_map_json,
            fallback_dataset_type=args.fallback_dataset_type,
        )
    else:
        mapper = PhenotypeMapper.from_hbp_backend(
            fallback_dataset_type=args.fallback_dataset_type
        )

    dataset_types = (
        {item.strip().upper() for item in args.dataset_types.split(",") if item.strip()}
        if args.dataset_types
        else None
    )

    adapter = LegacyAssociationCsvAdapter(
        csv_path=args.csv_path,
        dataset_id=args.dataset_id,
        phenotype_mapper=mapper,
        include_dataset_types=dataset_types,
    )

    contract = build_association_contract(
        dataset_type="ASSOCIATION",
        required_fields={
            item.strip()
            for item in args.required_fields.split(",")
            if item.strip()
        },
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
            skip_unknown_axis_values=not args.keep_unknown_axis,
            ancestry_value_precision=args.ancestry_precision,
            deduplicate_ancestry_points=not args.disable_ancestry_dedup,
        )
    ]
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

    print("DataHub run complete")
    print(f"Adapters: {report.adapter_count}")
    print(f"Ingested records: {report.ingested_records}")
    print(f"Validated records: {report.validated_records}")
    print(f"Dropped records: {report.dropped_records}")
    print(f"Issues: {len(report.issues)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
