"""Bounded serving sidecars for the normalized association evidence v2 release."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import platform
import shutil
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

REUSED_TABLES = {
    "provider_relations": "provider_relation_partitions",
    "provider_variant_projection": "provider_variant_partitions",
    "summary_base": "summary_base_partitions",
    "consequence_annotations": "published_variant_partitions/consequence_annotations",
    "clinical_assertions": "published_variant_partitions/clinical_assertions",
    "population_observations": "published_variant_partitions/population_observations",
}

FINE_VARIANT_TABLES = {
    "consequence_annotations": ("consequence_annotations", "variant_id"),
    "clinical_assertions": ("clinical_assertions", "variant_id"),
    "population_observations": ("population_observations", "variant_id"),
}

COARSE_PASSTHROUGH_TABLES = (
    "source_summary_associations_by_gene",
    "source_summary_association_phenotype_counts_by_gene",
    "source_summary_association_base_by_gene",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hardlink_tree(source: Path, destination: Path) -> int:
    """Create a self-contained tree without duplicating file data on one volume."""

    files = 0
    for source_path in sorted(source.rglob("*")):
        relative = source_path.relative_to(source)
        target = destination / relative
        if source_path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            source_stat = source_path.stat()
            target_stat = target.stat()
            if (source_stat.st_ino, source_stat.st_size) != (
                target_stat.st_ino,
                target_stat.st_size,
            ):
                raise RuntimeError(f"Serving hardlink target conflicts: {target}")
        else:
            os.link(source_path, target)
        files += 1
    return files


@contextmanager
def _progress_monitor(label: str, root: Path, interval_seconds: int):
    """Report bounded filesystem progress while DuckDB executes one atomic COPY."""

    stopped = threading.Event()
    started = time.perf_counter()

    def report() -> None:
        while not stopped.wait(interval_seconds):
            files = 0
            bytes_written = 0
            if root.exists():
                for path in root.rglob("*.parquet"):
                    try:
                        stat = path.stat()
                    except FileNotFoundError:
                        continue
                    files += 1
                    bytes_written += stat.st_size
            LOGGER.info(
                "%s progress elapsed_seconds=%.1f parquet_files=%d bytes=%d root=%s",
                label,
                time.perf_counter() - started,
                files,
                bytes_written,
                root,
            )

    thread = threading.Thread(target=report, name=f"{label}-progress", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=max(1, interval_seconds))


class AssociationEvidenceV2ServingBuilder:
    """Publish immutable hash partitions used by the backend query service."""

    def __init__(
        self,
        *,
        source_db: Path,
        intermediate_root: Path,
        output_root: Path,
        memory_limit: str = "8GB",
        threads: int = 1,
        variant_bucket_characters: int = 3,
        progress_interval: int = 60,
        coarse_serving_root: Path | None = None,
    ) -> None:
        if variant_bucket_characters not in {2, 3, 4}:
            raise ValueError("variant_bucket_characters must be 2, 3, or 4")
        if progress_interval < 1:
            raise ValueError("progress_interval must be at least 1 second")
        self.source_db = source_db.resolve()
        self.intermediate_root = intermediate_root.resolve()
        self.output_root = output_root.resolve()
        self.memory_limit = memory_limit
        self.threads = threads
        self.variant_bucket_characters = variant_bucket_characters
        self.progress_interval = progress_interval
        self.coarse_serving_root = (
            coarse_serving_root.resolve() if coarse_serving_root else None
        )
        self.tables_root = self.output_root / "tables"
        self.checkpoint_path = self.output_root / "serving-checkpoint.json"
        self.manifest_path = self.output_root / "serving-manifest.json"
        self.log_path = self.output_root / "serving-build.log"

    def _state(self) -> dict[str, Any]:
        if not self.checkpoint_path.exists():
            return {"completed": {}}
        return json.loads(self.checkpoint_path.read_text())

    def _save_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = _utc_now()
        temporary = self.checkpoint_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        temporary.replace(self.checkpoint_path)

    def _partition_source(self, logical_name: str) -> tuple[Path, int, str]:
        if self.coarse_serving_root is None:
            raise RuntimeError(
                "A lower-width --coarse-serving-root is required when publishing "
                "more than 256 runtime buckets."
            )
        manifest_path = self.coarse_serving_root / "serving-manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"Coarse serving manifest is missing: {manifest_path}"
            )
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("contract") != "association_evidence_v2_partitioned_serving":
            raise RuntimeError(f"Invalid coarse serving contract: {manifest_path}")
        table = (manifest.get("tables") or {}).get(logical_name)
        if not isinstance(table, dict) or not table.get("path"):
            raise RuntimeError(
                f"Coarse serving manifest does not register {logical_name}"
            )
        characters = int(table.get("bucket_characters") or 2)
        layout = str(table.get("directory_layout") or "flat_variant_bucket")
        expected_layout = (
            "flat_variant_bucket" if characters == 2 else "nested_coarse_fine"
        )
        if (
            characters < 2
            or characters >= self.variant_bucket_characters
            or layout != expected_layout
        ):
            raise RuntimeError(
                f"Expected a compatible lower-width partition table for {logical_name}; "
                f"target_characters={self.variant_bucket_characters} "
                f"found characters={characters} layout={layout}"
            )
        root = (self.coarse_serving_root / str(table["path"])).resolve()
        if self.coarse_serving_root not in root.parents:
            raise RuntimeError(f"Coarse serving table escapes its root: {root}")
        if not root.is_dir():
            raise FileNotFoundError(f"Coarse serving table is missing: {root}")
        return root, characters, layout

    def _reuse_coarse_generated_table(
        self,
        logical_name: str,
        state: dict[str, Any],
    ) -> None:
        """Hardlink an unchanged gene projection from the verified coarse package."""

        if self.coarse_serving_root is None:
            return
        destination = self.tables_root / logical_name
        if destination.exists():
            return
        manifest_path = self.coarse_serving_root / "serving-manifest.json"
        manifest = json.loads(manifest_path.read_text())
        table = (manifest.get("tables") or {}).get(logical_name)
        if not isinstance(table, dict) or not table.get("path"):
            raise RuntimeError(
                f"Coarse serving manifest does not register {logical_name}"
            )
        source = (self.coarse_serving_root / str(table["path"])).resolve()
        if not source.is_dir():
            raise FileNotFoundError(f"Reusable coarse serving table is missing: {source}")
        started = time.perf_counter()
        files = _hardlink_tree(source, destination)
        state.setdefault("completed", {})[logical_name] = {
            "rows": table.get("rows"),
            "files": files,
            "complete": True,
            "reused_from": str(source),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        self._save_state(state)

    def _publish_variant_table(
        self,
        *,
        connection: Any,
        state: dict[str, Any],
        logical_name: str,
        source_table: str,
        key_column: str,
        where_sql: str = "",
    ) -> tuple[Path, int]:
        """Publish one variant table with table-atomic or coarse-unit checkpoints."""

        completed = state.setdefault("completed", {})
        target = self.tables_root / logical_name
        count = int(
            connection.execute(
                f"SELECT count(*) FROM {source_table} {where_sql}"
            ).fetchone()[0]
        )
        previous = completed.get(logical_name) or {}
        partition_source = None
        if self.variant_bucket_characters > 2:
            partition_source = self._partition_source(logical_name)
        source_token = (
            "|".join(str(value) for value in partition_source)
            if partition_source
            else str(self.source_db)
        )
        configuration_matches = (
            previous.get("rows") == count
            and previous.get("variant_bucket_characters")
            == self.variant_bucket_characters
            and previous.get("source_token") == source_token
        )
        if not configuration_matches:
            if target.exists():
                stale = target.with_name(
                    target.name
                    + ".stale-"
                    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                )
                target.rename(stale)
            previous = {
                "rows": count,
                "variant_bucket_characters": self.variant_bucket_characters,
                "source_token": source_token,
                "completed_coarse_buckets": [],
                "complete": False,
            }
            completed[logical_name] = previous
            self._save_state(state)
        if (
            previous.get("complete")
            and target.is_dir()
            and any(target.rglob("*.parquet"))
        ):
            return target, count

        if self.variant_bucket_characters == 2:
            target.parent.mkdir(parents=True, exist_ok=True)
            LOGGER.info("%s partitioning start rows=%d", logical_name, count)
            where_clause = f" {where_sql}" if where_sql else ""
            with _progress_monitor(logical_name, target, self.progress_interval):
                connection.execute(
                    f"""
COPY (
    SELECT *, substr(sha256({key_column}), 1, 2) AS variant_bucket
    FROM {source_table}{where_clause}
) TO '{str(target).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
                )
            previous.update(
                {
                    "files": len(list(target.rglob("*.parquet"))),
                    "complete": True,
                }
            )
            self._save_state(state)
            return target, count

        target.mkdir(parents=True, exist_ok=True)
        work_root = self.output_root / ".serving-work" / logical_name
        work_root.mkdir(parents=True, exist_ok=True)
        done = set(previous.get("completed_coarse_buckets") or [])
        source_root, source_characters, source_layout = partition_source
        source_directory_prefix = (
            "variant_bucket="
            if source_layout == "flat_variant_bucket"
            else "coarse_bucket="
        )
        coarse_directories = sorted(
            path
            for path in source_root.iterdir()
            if path.is_dir()
            and path.name.startswith(source_directory_prefix)
            and len(path.name.rsplit("=", 1)[-1]) == 2
        )
        if not coarse_directories:
            raise RuntimeError(
                f"No two-character coarse units found in partition source: {source_root}"
            )
        for index, source_directory in enumerate(coarse_directories, start=1):
            coarse_bucket = source_directory.name.rsplit("=", 1)[-1]
            final_unit = target / f"coarse_bucket={coarse_bucket}"
            if coarse_bucket in done and final_unit.is_dir():
                continue
            if final_unit.exists():
                raise RuntimeError(
                    f"Uncheckpointed fine partition unit already exists: {final_unit}"
                )
            temporary_unit = work_root / f"coarse_bucket={coarse_bucket}.incomplete"
            if temporary_unit.exists():
                shutil.rmtree(temporary_unit)
            if source_characters == 2:
                source_glob_path = source_directory / "*.parquet"
                excluded_partition_columns = "variant_bucket"
            else:
                source_glob_path = source_directory / "variant_bucket=*" / "*.parquet"
                excluded_partition_columns = "variant_bucket, coarse_bucket"
            source_glob = str(source_glob_path).replace("'", "''")
            LOGGER.info(
                "%s fine partition start coarse_bucket=%s unit=%d/%d",
                logical_name,
                coarse_bucket,
                index,
                len(coarse_directories),
            )
            with _progress_monitor(
                f"{logical_name} coarse_bucket={coarse_bucket}",
                temporary_unit,
                self.progress_interval,
            ):
                connection.execute(
                    f"""
COPY (
    SELECT * EXCLUDE ({excluded_partition_columns}),
           substr(sha256({key_column}), 1, {self.variant_bucket_characters}) AS variant_bucket
    FROM read_parquet('{source_glob}', hive_partitioning=true)
) TO '{str(temporary_unit).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
                )
            temporary_unit.rename(final_unit)
            done.add(coarse_bucket)
            previous["completed_coarse_buckets"] = sorted(done)
            previous["files"] = len(list(target.rglob("*.parquet")))
            self._save_state(state)
            LOGGER.info(
                "%s fine partition complete coarse_buckets=%d/%d files=%d",
                logical_name,
                len(done),
                len(coarse_directories),
                previous["files"],
            )
        previous["complete"] = True
        previous["files"] = len(list(target.rglob("*.parquet")))
        self._save_state(state)
        return target, count

    def build(self) -> dict[str, Any]:
        import duckdb

        self.output_root.mkdir(parents=True, exist_ok=True)
        self.tables_root.mkdir(parents=True, exist_ok=True)
        state = self._state()
        completed = state.setdefault("completed", {})
        reused: dict[str, Any] = {}
        for logical_name, relative in REUSED_TABLES.items():
            if (
                self.variant_bucket_characters > 2
                and logical_name in FINE_VARIANT_TABLES
            ):
                continue
            source = self.intermediate_root / relative
            if not source.is_dir():
                raise FileNotFoundError(f"Required build partition is missing: {source}")
            destination = self.tables_root / logical_name
            started = time.perf_counter()
            file_count = _hardlink_tree(source, destination)
            reused[logical_name] = {
                "path": str(destination.relative_to(self.output_root)),
                "partition_key": (
                    "association_record_id" if logical_name == "provider_relations" else "variant_id"
                ),
                "bucket_function": (
                    "association_record_id[12:14]"
                    if logical_name == "provider_relations"
                    else "sha256(variant_id)[0:2]"
                ),
                "files": file_count,
                "hardlinked_from": str(source),
            }
            completed[logical_name] = {
                "files": file_count,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            }
            self._save_state(state)

        connection = duckdb.connect(
            str(self.source_db),
            read_only=True,
            config={"threads": str(self.threads), "memory_limit": self.memory_limit},
        )
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("PRAGMA disable_progress_bar")
        provider_started = time.perf_counter()
        provider_root, provider_count = self._publish_variant_table(
            connection=connection,
            state=state,
            logical_name="provider_records",
            source_table="provider_records",
            key_column="variant_id_raw",
            where_sql="WHERE coalesce(trim(variant_id_raw), '')<>''",
        )
        provider_total_count = int(
            connection.execute("SELECT count(*) FROM provider_records").fetchone()[0]
        )
        provider_unkeyed_count = provider_total_count - provider_count
        completed["provider_records"]["elapsed_seconds"] = round(
            time.perf_counter() - provider_started, 3
        )
        self._save_state(state)
        association_started = time.perf_counter()
        association_root, association_count = self._publish_variant_table(
            connection=connection,
            state=state,
            logical_name="association_records",
            source_table="association_records",
            key_column="variant_id",
        )
        completed["association_records"]["elapsed_seconds"] = round(
            time.perf_counter() - association_started, 3
        )
        self._save_state(state)
        summary_started = time.perf_counter()
        summary_root, summary_count = self._publish_variant_table(
            connection=connection,
            state=state,
            logical_name="variant_phenotype_summaries",
            source_table="variant_phenotype_summaries",
            key_column="variant_id",
        )
        completed["variant_phenotype_summaries"]["elapsed_seconds"] = round(
            time.perf_counter() - summary_started, 3
        )
        self._save_state(state)
        fine_variant_tables: dict[str, Any] = {}
        if self.variant_bucket_characters > 2:
            for logical_name, (source_table, key_column) in FINE_VARIANT_TABLES.items():
                started = time.perf_counter()
                table_root, table_count = self._publish_variant_table(
                    connection=connection,
                    state=state,
                    logical_name=logical_name,
                    source_table=source_table,
                    key_column=key_column,
                )
                completed[logical_name]["elapsed_seconds"] = round(
                    time.perf_counter() - started, 3
                )
                self._save_state(state)
                fine_variant_tables[logical_name] = {
                    "path": str(table_root.relative_to(self.output_root)),
                    "partition_key": key_column,
                    "bucket_function": (
                        f"sha256({key_column})[0:{self.variant_bucket_characters}]"
                    ),
                    "bucket_characters": self.variant_bucket_characters,
                    "coarse_bucket_characters": 2,
                    "directory_layout": "nested_coarse_fine",
                    **completed[logical_name],
                }
        passthrough_tables: dict[str, Any] = {}
        if self.coarse_serving_root is not None:
            coarse_manifest_path = self.coarse_serving_root / "serving-manifest.json"
            coarse_manifest = json.loads(coarse_manifest_path.read_text())
            for logical_name in COARSE_PASSTHROUGH_TABLES:
                source_metadata = (coarse_manifest.get("tables") or {}).get(logical_name)
                if not isinstance(source_metadata, dict):
                    continue
                self._reuse_coarse_generated_table(logical_name, state)
                source_table = dict(source_metadata)
                source_table["path"] = str(
                    (self.tables_root / logical_name).relative_to(self.output_root)
                )
                source_table["reused_from_manifest"] = str(coarse_manifest_path)
                source_table.update(completed[logical_name])
                passthrough_tables[logical_name] = source_table
        self._reuse_coarse_generated_table("summary_base_by_gene", state)
        summary_gene_root = self.tables_root / "summary_base_by_gene"
        summary_gene_state = completed.get("summary_base_by_gene") or {}
        if not (
            summary_gene_state.get("complete")
            and summary_gene_root.is_dir()
            and any(summary_gene_root.rglob("*.parquet"))
        ):
            if summary_gene_root.exists():
                stale = summary_gene_root.with_name(
                    summary_gene_root.name + ".stale-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                )
                summary_gene_root.rename(stale)
            started = time.perf_counter()
            source = str((self.tables_root / "summary_base" / "**" / "*.parquet")).replace("'", "''")
            LOGGER.info("Gene summary partitioning start")
            copied = connection.execute(
                f"""
COPY (
    SELECT * EXCLUDE (variant_bucket), substr(sha256(gene_id), 1, 2) AS gene_bucket
    FROM read_parquet('{source}', hive_partitioning=true)
) TO '{str(summary_gene_root).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (gene_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
            ).fetchone()
            completed["summary_base_by_gene"] = {
                "rows": int(copied[0]) if copied else None,
                "files": len(list(summary_gene_root.rglob("*.parquet"))),
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "complete": True,
            }
            self._save_state(state)
        self._reuse_coarse_generated_table("consequence_annotations_by_gene", state)
        consequence_gene_root = self.tables_root / "consequence_annotations_by_gene"
        consequence_gene_count = int(
            connection.execute(
                "SELECT count(*) FROM consequence_annotations "
                "WHERE coalesce(trim(gene_id), '')<>''"
            ).fetchone()[0]
        )
        consequence_gene_state = completed.get("consequence_annotations_by_gene") or {}
        if not (
            consequence_gene_state.get("complete")
            and consequence_gene_state.get("rows") == consequence_gene_count
            and consequence_gene_root.is_dir()
            and any(consequence_gene_root.rglob("*.parquet"))
        ):
            if consequence_gene_root.exists():
                stale = consequence_gene_root.with_name(
                    consequence_gene_root.name
                    + ".stale-"
                    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                )
                consequence_gene_root.rename(stale)
            started = time.perf_counter()
            LOGGER.info(
                "Gene consequence partitioning start rows=%d", consequence_gene_count
            )
            connection.execute(
                f"""
COPY (
    SELECT *, substr(sha256(upper(gene_id)), 1, 2) AS gene_bucket
    FROM consequence_annotations
    WHERE coalesce(trim(gene_id), '')<>''
) TO '{str(consequence_gene_root).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (gene_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
            )
            completed["consequence_annotations_by_gene"] = {
                "rows": consequence_gene_count,
                "files": len(list(consequence_gene_root.rglob("*.parquet"))),
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "complete": True,
            }
            self._save_state(state)
        connection.close()

        db_sha_path = self.source_db.with_suffix(self.source_db.suffix + ".sha256")
        source_db_sha = None
        if db_sha_path.is_file():
            source_db_sha = db_sha_path.read_text().split()[0]
        manifest = {
            "contract": "association_evidence_v2_partitioned_serving",
            "schema_version": "2.6.0-rc1",
            "built_at": _utc_now(),
            "source_db": str(self.source_db),
            "source_db_sha256": source_db_sha,
            "coarse_serving_manifest": (
                str(self.coarse_serving_root / "serving-manifest.json")
                if self.coarse_serving_root
                else None
            ),
            "coarse_serving_manifest_sha256": (
                _sha256(self.coarse_serving_root / "serving-manifest.json")
                if self.coarse_serving_root
                else None
            ),
            "variant_bucket_count": 16 ** self.variant_bucket_characters,
            "runtime": {
                "python": platform.python_version(),
                "duckdb": duckdb.__version__,
                "platform": platform.platform(),
                "threads": self.threads,
                "memory_limit": self.memory_limit,
                "progress_interval": self.progress_interval,
            },
            "tables": {
                **reused,
                **passthrough_tables,
                **fine_variant_tables,
                "provider_records": {
                    "path": str(provider_root.relative_to(self.output_root)),
                    "partition_key": "variant_id_raw",
                    "bucket_function": (
                        f"sha256(variant_id_raw)[0:{self.variant_bucket_characters}]"
                    ),
                    "bucket_characters": self.variant_bucket_characters,
                    "normalized_source_rows": provider_total_count,
                    "excluded_unkeyed_rows": provider_unkeyed_count,
                    "exclusion_contract": (
                        "Rows without variant_id_raw remain in the normalized sidecar "
                        "but cannot enter variant-keyed serving or drill-down exports."
                    ),
                    "coarse_bucket_characters": (
                        2 if self.variant_bucket_characters > 2 else None
                    ),
                    "directory_layout": (
                        "nested_coarse_fine"
                        if self.variant_bucket_characters > 2
                        else "flat_variant_bucket"
                    ),
                    **completed["provider_records"],
                },
                "association_records": {
                    "path": str(association_root.relative_to(self.output_root)),
                    "partition_key": "variant_id",
                    "bucket_function": (
                        f"sha256(variant_id)[0:{self.variant_bucket_characters}]"
                    ),
                    "bucket_characters": self.variant_bucket_characters,
                    "coarse_bucket_characters": (
                        2 if self.variant_bucket_characters > 2 else None
                    ),
                    "directory_layout": (
                        "nested_coarse_fine"
                        if self.variant_bucket_characters > 2
                        else "flat_variant_bucket"
                    ),
                    **completed["association_records"],
                },
                "variant_phenotype_summaries": {
                    "path": str(summary_root.relative_to(self.output_root)),
                    "partition_key": "variant_id",
                    "bucket_function": (
                        f"sha256(variant_id)[0:{self.variant_bucket_characters}]"
                    ),
                    "bucket_characters": self.variant_bucket_characters,
                    "coarse_bucket_characters": (
                        2 if self.variant_bucket_characters > 2 else None
                    ),
                    "directory_layout": (
                        "nested_coarse_fine"
                        if self.variant_bucket_characters > 2
                        else "flat_variant_bucket"
                    ),
                    **completed["variant_phenotype_summaries"],
                },
                "summary_base_by_gene": {
                    "path": str(summary_gene_root.relative_to(self.output_root)),
                    "partition_key": "gene_id",
                    "bucket_function": "sha256(gene_id)[0:2]",
                    "bucket_characters": 2,
                    **completed["summary_base_by_gene"],
                },
                "consequence_annotations_by_gene": {
                    "path": str(consequence_gene_root.relative_to(self.output_root)),
                    "partition_key": "gene_id",
                    "bucket_function": "sha256(upper(gene_id))[0:2]",
                    "bucket_characters": 2,
                    **completed["consequence_annotations_by_gene"],
                },
            },
            "limitations": [
                "MVP source summaries are first-class associations; provider rows are not fabricated.",
                "Provider provenance partitions contain only recoverable AWS source rows.",
                "Global ART indexes are intentionally not built for hundred-million/billion-row provenance links.",
            ],
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        checksum = _sha256(self.manifest_path)
        checksum_path = self.manifest_path.with_suffix(".json.sha256")
        checksum_path.write_text(f"{checksum}  {self.manifest_path.name}\n")
        return manifest
