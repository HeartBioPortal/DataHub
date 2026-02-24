#!/usr/bin/env python3
"""Run a memory-bounded MVP ingestion pipeline for DataHub."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

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
_STATE_ROOT_RELATIVE = Path("_datahub_state") / "mvp"
_CHECKPOINT_VERSION = 1


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
        self._bootstrap_existing_table()

    def _bootstrap_existing_table(self) -> None:
        count_query = (
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE lower(table_name) = lower(?)"
        )
        existing = int(
            self._connection.execute(count_query, [self.table_name]).fetchone()[0]
        )
        if existing == 0:
            return
        self._initialized = True
        self._record_count = int(
            self._connection.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]
        )
        self.logger.info(
            "Storage bootstrap: existing table=%s rows=%d",
            self.table_name,
            self._record_count,
        )

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

    def append_parquet(self, parquet_path: str | Path) -> int:
        source = Path(parquet_path)
        if not source.exists():
            return 0

        parquet_target = source.as_posix().replace("'", "''")
        row_count = int(
            self._connection.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_target}')"
            ).fetchone()[0]
        )
        if row_count == 0:
            return 0

        if not self._initialized:
            self._connection.execute(
                f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM read_parquet('{parquet_target}')"
            )
            self._initialized = True
        else:
            self._connection.execute(
                f"INSERT INTO {self.table_name} SELECT * FROM read_parquet('{parquet_target}')"
            )

        self._record_count += row_count
        self.logger.info(
            "Storage merge: source=%s rows=%d total_rows=%d",
            source,
            row_count,
            self._record_count,
        )
        return row_count

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


class MVPCheckpoint:
    """File-level checkpoint for resumable MVP ingestion."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.payload = self._default_payload()

    @staticmethod
    def _default_payload() -> dict[str, Any]:
        return {
            "version": _CHECKPOINT_VERSION,
            "completed_files": {},
            "updated_at": None,
        }

    def load(self) -> None:
        if not self.path.exists():
            self.payload = self._default_payload()
            return

        loaded = json.loads(self.path.read_text())
        if not isinstance(loaded, dict):
            self.payload = self._default_payload()
            return
        if int(loaded.get("version", 0)) != _CHECKPOINT_VERSION:
            self.payload = self._default_payload()
            return
        if not isinstance(loaded.get("completed_files"), dict):
            loaded["completed_files"] = {}
        self.payload = loaded

    def save(self) -> None:
        self.payload["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self.payload, indent=2, sort_keys=True))
        temp.replace(self.path)

    def reset(self) -> None:
        self.payload = self._default_payload()
        if self.path.exists():
            self.path.unlink()

    def completed_files(self) -> set[str]:
        completed = self.payload.get("completed_files", {})
        if not isinstance(completed, dict):
            return set()
        return set(completed.keys())

    def mark_completed(self, file_path: Path, token: str) -> None:
        completed = self.payload.setdefault("completed_files", {})
        completed[_normalized_file_path(file_path)] = {
            "file_token": token,
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self.save()


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
        "--checkpoint-path",
        default=None,
        help="Optional checkpoint file path. Defaults to <output-root>/_datahub_state/mvp/checkpoint.json",
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help="Optional state directory for staged per-file outputs and checkpoint data.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint-based resume and always process all input files.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset checkpoint metadata and remove staged per-file state before run.",
    )
    parser.add_argument(
        "--keep-stage-files",
        action="store_true",
        help="Keep per-file staged artifacts under state dir (debugging).",
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


def _normalized_file_path(path: Path) -> str:
    return str(path.resolve())


def _file_token(path: Path) -> str:
    return hashlib.sha1(_normalized_file_path(path).encode("utf-8")).hexdigest()[:16]


def _build_incremental_publishers(
    *,
    output_root: Path,
    disable_rollup: bool,
    rollup_tree_json: str | None,
    ancestry_precision: int,
) -> list[Any]:
    publishers: list[Any] = [
        LegacyAssociationPublisher(
            output_root=output_root,
            ancestry_value_precision=ancestry_precision,
            deduplicate_ancestry_points=True,
            incremental_merge=True,
        )
    ]
    if not disable_rollup:
        publishers.append(
            PhenotypeRollupPublisher(
                output_root=output_root,
                tree_json_path=rollup_tree_json,
                ancestry_value_precision=ancestry_precision,
                deduplicate_variants=True,
                incremental_merge=True,
            )
        )
    return publishers


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=4))


def _merge_stage_output(
    *,
    stage_root: Path,
    output_root: Path,
    association_merger: LegacyAssociationPublisher,
    rollup_merger: PhenotypeRollupPublisher | None,
    logger: logging.Logger,
) -> None:
    copied = 0
    merged = 0

    for source_file in sorted(stage_root.rglob("*.json")):
        relative = source_file.relative_to(stage_root)
        target_file = output_root / relative
        target_file.parent.mkdir(parents=True, exist_ok=True)

        if not target_file.exists():
            shutil.copy2(source_file, target_file)
            copied += 1
            continue

        source_payload = _read_json(source_file)
        target_payload = _read_json(target_file)

        if (
            len(relative.parts) >= 4
            and relative.parts[0] == "association"
            and relative.parts[1] == "final"
        ):
            section = relative.parts[2]
        else:
            section = ""

        if section == "association":
            if not isinstance(source_payload, list) or not isinstance(target_payload, list):
                raise TypeError(f"Association payload type mismatch for {target_file}")
            merged_payload = association_merger._merge_association_payload(
                target_payload,
                source_payload,
            )
        elif section == "overall":
            if not isinstance(source_payload, dict) or not isinstance(target_payload, dict):
                raise TypeError(f"Overall payload type mismatch for {target_file}")
            merged_payload = association_merger._merge_overall_payload(
                target_payload,
                source_payload,
            )
        elif section == "association_rollup":
            if rollup_merger is None:
                raise RuntimeError("Rollup payload exists but rollup publisher is disabled.")
            if not isinstance(source_payload, list) or not isinstance(target_payload, list):
                raise TypeError(f"Rollup association payload type mismatch for {target_file}")
            merged_payload = rollup_merger._merge_association_payload(
                target_payload,
                source_payload,
            )
        elif section == "overall_rollup":
            if rollup_merger is None:
                raise RuntimeError("Rollup payload exists but rollup publisher is disabled.")
            if not isinstance(source_payload, dict) or not isinstance(target_payload, dict):
                raise TypeError(f"Rollup overall payload type mismatch for {target_file}")
            merged_payload = rollup_merger._merge_overall_payload(
                target_payload,
                source_payload,
            )
        else:
            shutil.copy2(source_file, target_file)
            copied += 1
            continue

        _write_json(target_file, merged_payload)
        merged += 1

    logger.info(
        "Staged merge complete: stage=%s copied=%d merged=%d",
        stage_root,
        copied,
        merged,
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("datahub.mvp.pipeline")
    started = time.perf_counter()

    output_root = Path(args.output_root)
    state_root = Path(args.state_dir) if args.state_dir else output_root / _STATE_ROOT_RELATIVE
    checkpoint_path = (
        Path(args.checkpoint_path)
        if args.checkpoint_path
        else state_root / "checkpoint.json"
    )
    staging_root = state_root / "staging"
    resume_enabled = not args.no_resume

    checkpoint = MVPCheckpoint(checkpoint_path)
    checkpoint.load()

    if args.reset_checkpoint:
        checkpoint.reset()
        if staging_root.exists():
            shutil.rmtree(staging_root)
        logger.info("Checkpoint reset: %s", checkpoint_path)

    input_files = sorted(expand_input_paths(args.input_paths), key=lambda item: str(item))
    if not input_files:
        raise ValueError("No MVP input files matched --input-path values.")

    completed = checkpoint.completed_files() if resume_enabled else set()
    pending_files = (
        [path for path in input_files if _normalized_file_path(path) not in completed]
        if resume_enabled
        else input_files
    )
    skipped_files = len(input_files) - len(pending_files)

    logger.info("Starting MVP pipeline")
    logger.info(
        (
            "Config: files=%d pending=%d skipped=%d output_root=%s state_root=%s "
            "chunksize=%d publish_batch_size=%d merge_mode=%s resume=%s"
        ),
        len(input_files),
        len(pending_files),
        skipped_files,
        output_root,
        state_root,
        args.chunksize,
        args.publish_batch_size,
        args.merge_mode,
        resume_enabled,
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

    final_publishers = _build_incremental_publishers(
        output_root=output_root,
        disable_rollup=args.disable_rollup,
        rollup_tree_json=args.rollup_tree_json,
        ancestry_precision=args.ancestry_precision,
    )
    association_merger = final_publishers[0]
    rollup_merger = final_publishers[1] if len(final_publishers) > 1 else None
    logger.info(
        "Publishers (incremental): %s",
        [type(item).__name__ for item in final_publishers],
    )

    redis_publisher = (
        LegacyRedisPublisher(
            output_root=output_root,
            strict=args.redis_strict,
        )
        if args.publish_redis
        else None
    )

    ingested_records = 0
    validated_records = 0
    dropped_records = 0
    issue_count = 0
    batch_count = 0
    warned_issues = 0
    processed_files = 0

    try:
        for file_index, input_file in enumerate(pending_files, start=1):
            token = _file_token(input_file)
            stage_root = staging_root / token
            if stage_root.exists():
                logger.warning("Removing stale stage directory: %s", stage_root)
                shutil.rmtree(stage_root)
            stage_root.mkdir(parents=True, exist_ok=True)

            logger.info(
                "File %d/%d start: %s token=%s",
                file_index,
                len(pending_files),
                input_file,
                token,
            )

            staged_publishers = _build_incremental_publishers(
                output_root=stage_root,
                disable_rollup=args.disable_rollup,
                rollup_tree_json=args.rollup_tree_json,
                ancestry_precision=args.ancestry_precision,
            )

            staged_storage: StreamingDuckDBSink | None = None
            if storage is not None:
                staged_storage = StreamingDuckDBSink(
                    db_path=stage_root / "canonical.duckdb",
                    parquet_path=stage_root / "canonical.parquet",
                    logger=logger,
                )

            try:
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

                    if staged_storage is not None:
                        staged_storage.write(validated)

                    for publisher in staged_publishers:
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
            except Exception:
                logger.exception("File failed: %s", input_file)
                if staged_storage is not None:
                    staged_storage.finalize()
                if not args.keep_stage_files and stage_root.exists():
                    shutil.rmtree(stage_root, ignore_errors=True)
                raise
            else:
                if staged_storage is not None:
                    staged_storage.finalize()

                _merge_stage_output(
                    stage_root=stage_root,
                    output_root=output_root,
                    association_merger=association_merger,
                    rollup_merger=rollup_merger,
                    logger=logger,
                )

                if storage is not None:
                    merged_rows = storage.append_parquet(stage_root / "canonical.parquet")
                    logger.info(
                        "Storage staged merge complete: file=%s merged_rows=%d",
                        input_file.name,
                        merged_rows,
                    )

                if resume_enabled:
                    checkpoint.mark_completed(input_file, token)
                processed_files += 1

                if not args.keep_stage_files and stage_root.exists():
                    shutil.rmtree(stage_root, ignore_errors=True)

                logger.info(
                    "File %d/%d complete: %s token=%s",
                    file_index,
                    len(pending_files),
                    input_file,
                    token,
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
            "Pipeline complete in %.2fs | files=%d processed=%d skipped=%d batches=%d "
            "ingested=%d validated=%d dropped=%d issues=%d"
        ),
        elapsed,
        len(input_files),
        processed_files,
        skipped_files,
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
                "processed_files": processed_files,
                "skipped_files": skipped_files,
                "batch_count": batch_count,
                "merge_mode": args.merge_mode,
                "resume_enabled": resume_enabled,
                "checkpoint_path": str(checkpoint_path),
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
