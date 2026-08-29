"""Build bounded default-summary rollups from the immutable source-summary index."""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

SOURCE_TABLE = "unavailable_provider_summaries_by_gene"
BASE_TABLE = "unavailable_provider_summary_base_by_gene"
PHENOTYPE_TABLE = "unavailable_provider_phenotype_counts_by_gene"
CONTRACT = "retained_compact_source_summary_rollup_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _save_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


class SourceSummaryRollupBuilder:
    """Create resumable gene-keyed summary projections without altering raw evidence."""

    def __init__(
        self,
        *,
        serving_root: Path,
        threads: int = 2,
        memory_limit: str = "4GB",
        progress_interval: int = 1,
        logger: logging.Logger | None = None,
    ) -> None:
        self.serving_root = serving_root.resolve()
        self.threads = max(1, int(threads))
        self.memory_limit = memory_limit
        self.progress_interval = max(1, int(progress_interval))
        self.logger = logger or logging.getLogger(__name__)
        self.tables_root = self.serving_root / "tables"
        self.source_root = self.tables_root / SOURCE_TABLE
        self.base_root = self.tables_root / BASE_TABLE
        self.phenotype_root = self.tables_root / PHENOTYPE_TABLE
        self.work_root = self.serving_root / ".source-summary-rollup-work"
        self.checkpoint_path = self.serving_root / "source-summary-rollup-checkpoint.json"
        self.file_manifest = self.serving_root / "source-summary-rollup-files.jsonl"

    def _validate_source(self) -> dict[str, Any]:
        manifest_path = self.serving_root / "serving-manifest.json"
        checksum_path = self.serving_root / "serving-manifest.json.sha256"
        manifest = json.loads(manifest_path.read_text())
        expected = checksum_path.read_text().split()[0]
        if _sha256(manifest_path) != expected:
            raise RuntimeError("Serving manifest checksum failed before source-summary rollup.")
        source = (manifest.get("tables") or {}).get(SOURCE_TABLE) or {}
        if manifest.get("schema_version") != "2.7.0-rc1":
            raise RuntimeError("Source-summary rollup requires completed schema 2.7.0-rc1.")
        if source.get("contract") != "retained_compact_source_summary_index_v1":
            raise RuntimeError("Unexpected source-summary input contract.")
        if int(source.get("files") or 0) != int(source.get("source_artifacts") or -1):
            raise RuntimeError("Source-summary files do not reconcile with registered artifacts.")
        return manifest

    @staticmethod
    def _uncheckpointed(path: Path) -> Path:
        return path.with_name(path.name + ".uncheckpointed-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

    def _rollup_bucket(self, bucket: str) -> dict[str, Any]:
        source_bucket = self.source_root / f"gene_bucket={bucket}"
        final_base = self.base_root / f"gene_bucket={bucket}"
        final_phenotype = self.phenotype_root / f"gene_bucket={bucket}"
        if final_base.exists() or final_phenotype.exists():
            raise RuntimeError(f"Uncheckpointed rollup output exists for bucket {bucket}.")
        work = self.work_root / f"bucket={bucket}.incomplete"
        if work.exists():
            work.rename(self._uncheckpointed(work))
        base_work = work / "base"
        phenotype_work = work / "phenotype"
        base_work.mkdir(parents=True)
        phenotype_work.mkdir(parents=True)
        source_glob = str(source_bucket / "gene_key=*" / "*.parquet").replace("'", "''")
        base_target = str(base_work).replace("'", "''")
        phenotype_target = str(phenotype_work).replace("'", "''")
        connection = duckdb.connect(config={
            "threads": str(self.threads),
            "memory_limit": self.memory_limit,
        })
        try:
            connection.execute("SET preserve_insertion_order=false")
            connection.execute(f"SET temp_directory='{str(work / 'duckdb-tmp').replace(chr(39), chr(39)*2)}'")
            connection.execute(f"""
COPY (
  SELECT gene_id, dataset_type, variant_id,
         min(try_cast(reported_p_value AS DOUBLE)) AS p_value,
         to_json(list_sort(list(DISTINCT phenotype_path_json))) AS phenotype_paths_json,
         count(*)::UBIGINT AS retained_source_summary_count,
         first(source ORDER BY source) AS source,
         first(provider_detail_status ORDER BY source) AS provider_detail_status,
         first(provider_detail_reason ORDER BY source) AS provider_detail_reason,
         first(missing_fields_json ORDER BY source) AS missing_fields_json,
         first(retained_source_summary_artifact ORDER BY source) AS retained_source_summary_artifact,
         first(publication_mode ORDER BY source) AS publication_mode,
         substr(sha256(gene_id), 1, 16) AS gene_key
  FROM read_parquet('{source_glob}', hive_partitioning=true, union_by_name=true)
  GROUP BY gene_id, dataset_type, variant_id
) TO '{base_target}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (gene_key), ROW_GROUP_SIZE 50000)
""")
            connection.execute(f"""
COPY (
  SELECT gene_id, dataset_type, phenotype_path_json, phenotype_path_key,
         count(DISTINCT variant_id)::UBIGINT AS distinct_variant_count,
         to_json(list_sort(list(DISTINCT variant_id))) AS variant_ids_json,
         min(try_cast(reported_p_value AS DOUBLE)) AS minimum_reported_p_value,
         substr(sha256(gene_id), 1, 16) AS gene_key
  FROM read_parquet('{source_glob}', hive_partitioning=true, union_by_name=true)
  GROUP BY gene_id, dataset_type, phenotype_path_json, phenotype_path_key
) TO '{phenotype_target}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (gene_key), ROW_GROUP_SIZE 50000)
""")
        finally:
            connection.close()
        base_files = sorted(base_work.rglob("*.parquet"))
        phenotype_files = sorted(phenotype_work.rglob("*.parquet"))
        base_rows = phenotype_rows = 0
        check = duckdb.connect()
        try:
            if base_files:
                base_rows = int(check.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=true)",
                    [str(base_work / "gene_key=*" / "*.parquet")],
                ).fetchone()[0])
            if phenotype_files:
                phenotype_rows = int(check.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=true)",
                    [str(phenotype_work / "gene_key=*" / "*.parquet")],
                ).fetchone()[0])
        finally:
            check.close()
        final_base.parent.mkdir(parents=True, exist_ok=True)
        final_phenotype.parent.mkdir(parents=True, exist_ok=True)
        base_work.rename(final_base)
        phenotype_work.rename(final_phenotype)
        shutil.rmtree(work, ignore_errors=True)
        return {
            "bucket": bucket,
            "base_rows": base_rows,
            "phenotype_rows": phenotype_rows,
            "base_files": len(base_files),
            "phenotype_files": len(phenotype_files),
        }

    def build(self) -> dict[str, Any]:
        started = time.perf_counter()
        manifest = self._validate_source()
        checkpoint = json.loads(self.checkpoint_path.read_text()) if self.checkpoint_path.exists() else {
            "contract": CONTRACT,
            "completed_buckets": {},
            "started_at": _utc_now(),
        }
        completed = checkpoint.setdefault("completed_buckets", {})
        buckets = [f"{value:02x}" for value in range(256)]
        self.work_root.mkdir(parents=True, exist_ok=True)
        for index, bucket in enumerate(buckets, start=1):
            if bucket in completed:
                continue
            result = self._rollup_bucket(bucket)
            completed[bucket] = result
            checkpoint["updated_at"] = _utc_now()
            _save_json(self.checkpoint_path, checkpoint)
            if index % self.progress_interval == 0:
                self.logger.info(
                    "source-summary rollup progress buckets=%d/256 base_rows=%d phenotype_rows=%d elapsed=%.1fs",
                    len(completed),
                    sum(int(row["base_rows"]) for row in completed.values()),
                    sum(int(row["phenotype_rows"]) for row in completed.values()),
                    time.perf_counter() - started,
                )
        file_rows = []
        for table_name, table_root in ((BASE_TABLE, self.base_root), (PHENOTYPE_TABLE, self.phenotype_root)):
            for path in sorted(table_root.rglob("*.parquet")):
                file_rows.append({
                    "table": table_name,
                    "path": str(path.relative_to(self.serving_root)),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                })
        with self.file_manifest.open("w", encoding="utf-8") as stream:
            for row in file_rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
        base_rows = sum(int(row["base_rows"]) for row in completed.values())
        phenotype_rows = sum(int(row["phenotype_rows"]) for row in completed.values())
        tables = manifest.setdefault("tables", {})
        common = {
            "contract": CONTRACT,
            "partition_key": "gene_id",
            "bucket_function": "sha256(gene_id)[0:2]",
            "bucket_characters": 2,
            "gene_key_function": "sha256(gene_id)[0:16]",
            "directory_layout": "nested_gene_key",
        }
        tables[BASE_TABLE] = {
            **common,
            "path": str(self.base_root.relative_to(self.serving_root)),
            "rows": base_rows,
            "files": sum(int(row["base_files"]) for row in completed.values()),
            "unit": "gene_dataset_variant",
        }
        tables[PHENOTYPE_TABLE] = {
            **common,
            "path": str(self.phenotype_root.relative_to(self.serving_root)),
            "rows": phenotype_rows,
            "files": sum(int(row["phenotype_files"]) for row in completed.values()),
            "unit": "gene_dataset_exact_phenotype_path",
        }
        manifest["schema_version"] = "2.8.0-rc1"
        manifest["source_summary_rollup_built_at"] = _utc_now()
        manifest["source_summary_rollup_runtime"] = {
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "threads": self.threads,
            "memory_limit": self.memory_limit,
        }
        manifest_path = self.serving_root / "serving-manifest.json"
        _save_json(manifest_path, manifest)
        checksum = _sha256(manifest_path)
        (self.serving_root / "serving-manifest.json.sha256").write_text(
            f"{checksum}  serving-manifest.json\n"
        )
        result = {
            "contract": CONTRACT,
            "built_at": manifest["source_summary_rollup_built_at"],
            "base_rows": base_rows,
            "phenotype_rows": phenotype_rows,
            "files": len(file_rows),
            "file_manifest": str(self.file_manifest),
            "checkpoint": str(self.checkpoint_path),
            "serving_manifest": str(manifest_path),
            "serving_manifest_sha256": checksum,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
        return result
