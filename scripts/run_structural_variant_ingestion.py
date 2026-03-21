#!/usr/bin/env python3
"""Run streaming dbVar structural-variant publication through DataHub."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import DatasetProfileLoader, SourceManifestLoader, build_default_adapter_registry, build_default_source_registry  # noqa: E402
from datahub.publishers import StructuralVariantLegacyPublisher  # noqa: E402
from datahub.quality import ContractValidator  # noqa: E402


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream dbVar structural variants through DataHub into the legacy SV JSON contract.",
    )
    parser.add_argument("--input", required=True, help="dbVar CSV/CSV.GZ file, directory, or glob.")
    parser.add_argument("--output-json", required=True, help="Output path for the structural_variants JSON.")
    parser.add_argument(
        "--gene-metadata-seed",
        default="",
        help="Optional gene-metadata seed JSON used to reuse gene-level annotations during ingestion.",
    )
    parser.add_argument("--report-path", default="", help="Optional JSON path for the combined run report.")
    parser.add_argument(
        "--merge-source-json",
        default="",
        help="Optional existing structural_variants JSON to merge into. Defaults to --output-json when --merge-existing is set.",
    )
    parser.add_argument(
        "--contract-path",
        default="",
        help="Optional output-contract JSON path. Defaults to DataHub's structural_variant_legacy contract.",
    )
    parser.add_argument("--profile", default="structural_variant", help="Dataset profile name or path.")
    parser.add_argument("--source-id", default="dbvar", help="Source manifest ID to instantiate.")
    parser.add_argument(
        "--sources-dir",
        default="",
        help="Optional override for the DataHub source manifest directory.",
    )
    parser.add_argument(
        "--cache-path",
        default="",
        help="Optional persistent JSON cache path for Ensembl API responses.",
    )
    parser.add_argument(
        "--phenotype-term",
        action="append",
        default=[],
        help="Optional phenotype allowlist term. Repeat to add multiple terms.",
    )
    parser.add_argument(
        "--merge-existing",
        action="store_true",
        help="Merge emitted variants into the existing structural_variants JSON payload.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="Per-request timeout for Ensembl API calls.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional delay between Ensembl API calls.",
    )
    parser.add_argument(
        "--max-overlap-bp",
        type=int,
        default=5_000_000,
        help="Maximum Ensembl overlap span per request before chunking.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=5_000,
        help="Emit a progress update every N rows/records.",
    )
    parser.add_argument(
        "--skip-row-count",
        action="store_true",
        help="Skip the initial full-file row count and log progress without a total percentage.",
    )
    return parser.parse_args()


def iter_valid_records(
    *,
    records: Iterator[Any],
    validator: ContractValidator,
    contract: Any,
    run_report: dict[str, Any],
) -> Iterator[Any]:
    for record in records:
        run_report["ingested_records"] += 1
        validated_record, issues = validator.validate_record(record, contract)
        run_report["issues"] += len(issues)
        if validated_record is None:
            run_report["dropped_records"] += 1
            continue
        run_report["validated_records"] += 1
        yield validated_record


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    profile_loader = DatasetProfileLoader()
    profile = profile_loader.load(args.profile)

    manifest_loader = SourceManifestLoader(args.sources_dir or None)
    source_registry = build_default_source_registry(manifest_loader)
    adapter_registry = build_default_adapter_registry()
    merge_source_json = args.merge_source_json or (args.output_json if args.merge_existing else "")

    adapter = source_registry.create_adapter(
        args.source_id,
        adapter_registry=adapter_registry,
        params={
            "input_paths": args.input,
            "metadata_seed_path": args.gene_metadata_seed or None,
            "ensembl_cache_path": args.cache_path or None,
            "ensembl_timeout_seconds": args.timeout_seconds,
            "ensembl_sleep_seconds": args.sleep_seconds,
            "max_overlap_bp": args.max_overlap_bp,
            "progress_every": args.progress_every,
            "count_rows": not args.skip_row_count,
            "phenotype_terms": args.phenotype_term,
        },
    )

    publisher = StructuralVariantLegacyPublisher(
        output_path=args.output_json,
        report_path=None,
        merge_source_json_path=merge_source_json or None,
        contract_path=args.contract_path or None,
        merge_existing=args.merge_existing,
        progress_every=args.progress_every,
    )
    validator = ContractValidator()
    run_report = {
        "profile": profile.name,
        "source_id": args.source_id,
        "ingested_records": 0,
        "validated_records": 0,
        "dropped_records": 0,
        "issues": 0,
    }

    publisher.publish(
        iter_valid_records(
            records=adapter.read(),
            validator=validator,
            contract=profile.to_contract(),
            run_report=run_report,
        )
    )

    payload = {
        **run_report,
        "contract_name": publisher.contract.name,
        "adapter_report": adapter.report,
        "publish_report": publisher.publish_report,
    }
    if args.report_path:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
