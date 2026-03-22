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

from datahub import (  # noqa: E402
    DatasetProfileLoader,
    SourceManifestLoader,
    StructuralVariantCheckpoint,
    build_default_adapter_registry,
    build_default_source_registry,
)
from datahub.publishers import StructuralVariantLegacyPublisher  # noqa: E402
from datahub.quality import ContractValidator  # noqa: E402


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream dbVar structural variants through DataHub into the legacy SV JSON contract.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="dbVar CSV/CSV.GZ/CSV.ZIP file, directory, or glob.",
    )
    parser.add_argument("--output-json", required=True, help="Output path for the structural_variants JSON.")
    parser.add_argument(
        "--gene-metadata-seed",
        default="",
        help="Optional gene-metadata seed JSON/JSON.ZIP used to reuse gene-level annotations during ingestion.",
    )
    parser.add_argument("--report-path", default="", help="Optional JSON path for the combined run report.")
    parser.add_argument(
        "--merge-source-json",
        default="",
        help="Optional existing structural_variants JSON/JSON.ZIP to merge into. Defaults to --output-json when --merge-existing is set.",
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
        "--gene-annotation-gtf",
        default="",
        help="Optional local GTF/GTF.GZ path for gene overlap and metadata lookup before Ensembl fallback.",
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
    parser.add_argument(
        "--checkpoint-path",
        default="",
        help="Optional checkpoint JSON path. Defaults to <output-json>.checkpoint.json.",
    )
    parser.add_argument(
        "--checkpoint-every-rows",
        type=int,
        default=50_000,
        help="Write resumable snapshots/checkpoints every N raw rows.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint-based resume and process the raw input from the beginning.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset checkpoint metadata before the run starts.",
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
    requested_merge_source_json = args.merge_source_json or (args.output_json if args.merge_existing else "")

    checkpoint: StructuralVariantCheckpoint | None = None
    resume_rows_by_file: dict[str, int] = {}
    merge_source_json = requested_merge_source_json
    merge_existing = args.merge_existing
    if not args.no_resume:
        checkpoint_path = (
            Path(args.checkpoint_path)
            if args.checkpoint_path
            else Path(f"{args.output_json}.checkpoint.json")
        )
        checkpoint = StructuralVariantCheckpoint(checkpoint_path)
        checkpoint.load()
        if args.reset_checkpoint:
            checkpoint.reset()
            logger.info("Checkpoint reset: %s", checkpoint_path)
        resume_rows_by_file = checkpoint.completed_rows()
        if resume_rows_by_file:
            output_path = Path(args.output_json)
            if not output_path.exists():
                raise FileNotFoundError(
                    f"Checkpoint exists at {checkpoint_path}, but output snapshot is missing: {output_path}. "
                    "Restore the output file or rerun with --reset-checkpoint."
                )
            merge_source_json = str(output_path)
            merge_existing = True
            logger.info(
                "Resume enabled: checkpoint=%s completed_files=%d snapshot=%s",
                checkpoint_path,
                len(resume_rows_by_file),
                output_path,
            )

    adapter = source_registry.create_adapter(
        args.source_id,
        adapter_registry=adapter_registry,
        params={
            "input_paths": args.input,
            "metadata_seed_path": args.gene_metadata_seed or None,
            "gene_annotation_gtf_path": args.gene_annotation_gtf or None,
            "resume_rows_by_file": resume_rows_by_file,
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
        merge_existing=merge_existing,
        progress_every=args.progress_every,
        checkpoint=checkpoint,
        checkpoint_every_rows=args.checkpoint_every_rows,
    )
    if hasattr(adapter, "row_completion_callback"):
        adapter.row_completion_callback = publisher.mark_source_row_completed
    validator = ContractValidator()
    run_report = {
        "profile": profile.name,
        "source_id": args.source_id,
        "ingested_records": 0,
        "validated_records": 0,
        "dropped_records": 0,
        "issues": 0,
        "resume_enabled": not args.no_resume,
        "checkpoint_path": str(checkpoint.path) if checkpoint is not None else None,
        "checkpoint_rows_loaded": sum(resume_rows_by_file.values()) if resume_rows_by_file else 0,
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
