#!/usr/bin/env python3
"""Run a memory-bounded MVP ingestion pipeline for DataHub."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import MissingFieldStrategy, build_association_contract  # noqa: E402
from datahub.adapters import MVPAssociationAdapter, PhenotypeMapper  # noqa: E402
from datahub.adapters.common import expand_input_paths  # noqa: E402
from datahub.models import CanonicalRecord  # noqa: E402
from datahub.publishers import (  # noqa: E402
    LegacyAssociationPublisher,
    LegacyRedisPublisher,
    PhenotypeRollupPublisher,
)
from datahub.quality import ContractValidator  # noqa: E402


_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class StreamingDuckDBSink:
    """Append canonical rows to DuckDB and export Parquet once at the end."""

    def __init__(
        self,
        *,
        db_path: str | Path,
        parquet_path: str | Path,
        table_name: str = "canonical_records",
        logger: logging.Logger,
    ) -> None:
        if not _TABLE_RE.match(table_name):
            raise ValueError(f"Unsafe table name: {table_name}")

        try:
            import duckdb  # type: ignore
        except ImportError as exc:  # pragma: no cover - dependency check
            raise RuntimeError(
                "duckdb is not installed. Install DataHub requirements first."
            ) from exc

        try:
            import pandas as pd  # type: ignore
        except ImportError as exc:  # pragma: no cover - dependency check
            raise RuntimeError(
                "pandas is not installed. Install DataHub requirements first."
            ) from exc

        self.db_path = Path(db_path)
        self.parquet_path = Path(parquet_path)
        self.table_name = table_name
        self.logger = logger
        self._duckdb = duckdb
        self._pd = pd
        self._initialized = False
        self._closed = False
        self._batch_count = 0
        self._record_count = 0

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self.db_path))

    def write(self, records: list[CanonicalRecord]) -> None:
        if not records:
            return

        self._batch_count += 1
        self._record_count += len(records)
        frame = self._pd.DataFrame([record.to_row() for record in records])
        for column in ("ancestry", "metadata"):
            frame[column] = frame[column].map(
                lambda payload: json.dumps(payload or {}, sort_keys=True)
            )

        self._connection.register("canonical_frame", frame)
        try:
            if not self._initialized:
                self._connection.execute(
                    f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM canonical_frame"
                )
                self._initialized = True
            else:
                self._connection.execute(
                    f"INSERT INTO {self.table_name} SELECT * FROM canonical_frame"
                )
        finally:
            self._connection.unregister("canonical_frame")

        if self._batch_count == 1 or self._batch_count % 20 == 0:
            self.logger.info(
                "Storage progress: batches=%d rows=%d",
                self._batch_count,
                self._record_count,
            )

    def finalize(self) -> None:
        if self._closed:
            return

        try:
            if not self._initialized:
                self.logger.info("Storage finalize: no validated rows to persist.")
                return

            if self.parquet_path.exists():
                self.parquet_path.unlink()

            parquet_target = self.parquet_path.as_posix().replace("'", "''")
            self._connection.execute(
                f"COPY {self.table_name} TO '{parquet_target}' (FORMAT PARQUET)"
            )
            self.logger.info(
                "Storage finalize: exported rows=%d to %s",
                self._record_count,
                self.parquet_path,
            )
        finally:
            self._connection.close()
            self._closed = True


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
        default=100_000,
        help="CSV chunk size used while reading MVP files.",
    )
    parser.add_argument(
        "--publish-batch-size",
        type=int,
        default=50_000,
        help="Validated record batch size used for publish/storage writes.",
    )
    parser.add_argument(
        "--merge-mode",
        default="chunk",
        choices=["file", "chunk"],
        help="Record merge scope inside the adapter. Use 'chunk' for bounded memory.",
    )
    parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=200_000,
        help="Emit progress logs every N raw rows per file.",
    )
    parser.add_argument(
        "--max-reported-issues",
        type=int,
        default=20,
        help="Cap warning logs for validation issues (all issues are still counted).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level for pipeline output.",
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


def _batched(records: Iterable[CanonicalRecord], batch_size: int) -> Iterable[list[CanonicalRecord]]:
    if batch_size < 1:
        raise ValueError("publish batch size must be >= 1")

    batch: list[CanonicalRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("datahub.mvp.pipeline")
    started = time.perf_counter()

    input_files = expand_input_paths(args.input_paths)
    if not input_files:
        raise ValueError("No MVP input files matched --input-path values.")

    logger.info("Starting MVP pipeline")
    logger.info(
        (
            "Config: files=%d output_root=%s chunksize=%d publish_batch_size=%d "
            "merge_mode=%s progress_every_rows=%d"
        ),
        len(input_files),
        args.output_root,
        args.chunksize,
        args.publish_batch_size,
        args.merge_mode,
        args.progress_every_rows,
    )

    mapper = _load_mapper(args)
    include_dataset_types = (
        {item.strip().upper() for item in args.dataset_types.split(",") if item.strip()}
        if args.dataset_types
        else None
    )
    contract = build_association_contract(
        dataset_type="ASSOCIATION",
        required_fields={item.strip() for item in args.required_fields.split(",") if item.strip()},
        axis_missing_strategy=MissingFieldStrategy(args.axis_missing_strategy),
    )
    validator = ContractValidator()

    if bool(args.duckdb_path) ^ bool(args.parquet_path):
        raise ValueError("Provide both --duckdb-path and --parquet-path, or neither.")

    storage: StreamingDuckDBSink | None = None
    if args.duckdb_path and args.parquet_path:
        logger.info("Storage enabled: DuckDB=%s Parquet=%s", args.duckdb_path, args.parquet_path)
        storage = StreamingDuckDBSink(
            db_path=args.duckdb_path,
            parquet_path=args.parquet_path,
            logger=logger,
        )
    else:
        logger.info("Storage disabled")

    incremental_publishers = [
        LegacyAssociationPublisher(
            output_root=args.output_root,
            ancestry_value_precision=args.ancestry_precision,
            deduplicate_ancestry_points=True,
            incremental_merge=True,
        )
    ]
    if not args.disable_rollup:
        incremental_publishers.append(
            PhenotypeRollupPublisher(
                output_root=args.output_root,
                tree_json_path=args.rollup_tree_json,
                ancestry_value_precision=args.ancestry_precision,
                deduplicate_variants=True,
                incremental_merge=True,
            )
        )

    redis_publisher = (
        LegacyRedisPublisher(
            output_root=args.output_root,
            strict=args.redis_strict,
        )
        if args.publish_redis
        else None
    )

    logger.info(
        "Publishers (incremental): %s",
        [type(item).__name__ for item in incremental_publishers],
    )

    ingested_records = 0
    validated_records = 0
    dropped_records = 0
    issue_count = 0
    batch_count = 0
    warned_issues = 0

    try:
        for file_index, input_file in enumerate(input_files, start=1):
            logger.info(
                "File %d/%d start: %s",
                file_index,
                len(input_files),
                input_file,
            )

            adapter = MVPAssociationAdapter(
                input_paths=[input_file],
                dataset_id=args.dataset_id,
                phenotype_mapper=mapper,
                include_dataset_types=include_dataset_types,
                chunksize=args.chunksize,
                log_progress=True,
                progress_every_rows=args.progress_every_rows,
                merge_mode=args.merge_mode,
            )

            for batch in _batched(adapter.read(), args.publish_batch_size):
                batch_count += 1
                ingested_records += len(batch)
                validation = validator.validate(batch, contract)
                issue_count += len(validation.issues)
                dropped_records += validation.dropped
                validated = validation.records
                validated_records += len(validated)

                if validation.issues and warned_issues < args.max_reported_issues:
                    for issue in validation.issues:
                        logger.warning(
                            "Validation issue: key=%s field=%s msg=%s",
                            issue.record_key,
                            issue.field_name,
                            issue.message,
                        )
                        warned_issues += 1
                        if warned_issues >= args.max_reported_issues:
                            logger.warning(
                                "Validation warning limit reached; suppressing further issue logs."
                            )
                            break

                if storage is not None:
                    storage.write(validated)

                for publisher in incremental_publishers:
                    publisher.publish(validated)

                logger.info(
                    (
                        "Batch %d complete: file=%s batch_ingested=%d batch_validated=%d "
                        "batch_dropped=%d batch_issues=%d totals(ingested=%d validated=%d dropped=%d)"
                    ),
                    batch_count,
                    input_file.name,
                    len(batch),
                    len(validated),
                    validation.dropped,
                    len(validation.issues),
                    ingested_records,
                    validated_records,
                    dropped_records,
                )

            logger.info(
                "File %d/%d complete: %s",
                file_index,
                len(input_files),
                input_file,
            )
    finally:
        if storage is not None:
            storage.finalize()

    if redis_publisher is not None:
        logger.info("Redis publish start (final output snapshot).")
        redis_publisher.publish([])
        logger.info("Redis publish complete.")

    elapsed = time.perf_counter() - started
    logger.info(
        (
            "Pipeline complete in %.2fs | files=%d batches=%d ingested=%d "
            "validated=%d dropped=%d issues=%d"
        ),
        elapsed,
        len(input_files),
        batch_count,
        ingested_records,
        validated_records,
        dropped_records,
        issue_count,
    )

    print(
        json.dumps(
            {
                "adapter": "mvp_association",
                "dataset_id": args.dataset_id,
                "file_count": len(input_files),
                "batch_count": batch_count,
                "merge_mode": args.merge_mode,
                "ingested_records": ingested_records,
                "validated_records": validated_records,
                "dropped_records": dropped_records,
                "issues": issue_count,
                "elapsed_seconds": round(elapsed, 2),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
