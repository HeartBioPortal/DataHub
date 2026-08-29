"""Build a resumable serving index for first-class MVP summary associations.

The retained compact variant-index artifacts are valid source-summary association
records. They are indexed without fabricating provider, study, allele, effect,
sample-size, ancestry, or fine-mapping detail that the compact artifact omits.
"""

from __future__ import annotations

import concurrent.futures
import csv
import gzip
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import duckdb

LOGGER = logging.getLogger(__name__)
TABLE_NAME = "source_summary_associations_by_gene"
CONTRACT = "source_summary_association_index_v2"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iter_json_array(path: Path, *, chunk_size: int = 1024 * 1024) -> Iterator[dict[str, Any]]:
    opener = gzip.open if path.name.endswith(".gz") else open
    decoder = json.JSONDecoder()
    with opener(path, "rt", encoding="utf-8") as stream:
        buffer = ""
        position = 0
        started = False
        eof = False
        while True:
            if not eof:
                chunk = stream.read(chunk_size)
                if chunk:
                    buffer += chunk
                else:
                    eof = True
            while True:
                while position < len(buffer) and buffer[position].isspace():
                    position += 1
                if not started:
                    if position >= len(buffer):
                        break
                    if buffer[position] != "[":
                        raise ValueError(f"Expected JSON array in {path}")
                    started = True
                    position += 1
                    continue
                while position < len(buffer) and (
                    buffer[position].isspace() or buffer[position] == ","
                ):
                    position += 1
                if position < len(buffer) and buffer[position] == "]":
                    return
                if position >= len(buffer):
                    break
                try:
                    value, end = decoder.raw_decode(buffer, position)
                except json.JSONDecodeError:
                    if eof:
                        raise
                    break
                if not isinstance(value, dict):
                    raise ValueError(f"Expected object entries in {path}")
                yield value
                position = end
            if position:
                buffer = buffer[position:]
                position = 0
            if eof:
                if buffer.strip():
                    raise ValueError(f"Truncated JSON array in {path}")
                return


def _source_summary_id(
    source: str,
    dataset_type: str,
    gene: str,
    variant_id: str,
    phenotype_path: list[str],
    logical_artifact: str,
) -> str:
    identity = json.dumps(
        [source, dataset_type, gene, variant_id, phenotype_path, logical_artifact],
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return "source-summary:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _process_artifact(task: tuple[Any, ...]) -> dict[str, Any]:
    (
        source,
        gene,
        dataset_type,
        logical_artifact,
        size_bytes,
        modified_at_ns,
        provider_detail_reason,
        missing_fields_json,
        content_contract,
        publication_mode,
        path_text,
        output_text,
        work_text,
    ) = task
    path = Path(path_text)
    output = Path(output_text)
    work_root = Path(work_text)
    stat = path.stat()
    if stat.st_size != int(size_bytes) or stat.st_mtime_ns != int(modified_at_ns):
        raise RuntimeError(f"Registered source-summary artifact changed: {path}")

    token = hashlib.sha256(str(logical_artifact).encode("utf-8")).hexdigest()
    csv_path = work_root / f"{token}.csv"
    parquet_path = work_root / f"{token}.parquet.incomplete"
    work_root.mkdir(parents=True, exist_ok=True)
    csv_path.unlink(missing_ok=True)
    parquet_path.unlink(missing_ok=True)
    selected_rows = 0
    seen_summary_ids: set[str] = set()
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            (
                "association_record_id",
                "source_summary_id",
                "record_kind",
                "evidence_granularity",
                "source",
                "source_display_name",
                "dataset_type",
                "gene_id",
                "variant_id",
                "phenotype_raw",
                "phenotype_path_json",
                "phenotype_path_key",
                "label_key",
                "reported_p_value",
                "variation_type",
                "ancestry_json",
                "metadata_json",
                "representative_source",
                "provider_detail_status",
                "provider_detail_reason",
                "missing_fields_json",
                "retained_source_summary_artifact",
                "retained_source_summary_json",
                "content_contract",
                "publication_mode",
                "source_row_ordinal",
            )
        )
        for ordinal, entry in enumerate(_iter_json_array(path)):
            sources = entry.get("sources")
            if not isinstance(sources, list):
                sources = [entry.get("source")]
            if str(source).casefold() not in {
                str(value).strip().casefold() for value in sources if value
            }:
                continue
            variant_id = str(entry.get("variant_id") or "").strip()
            if not variant_id:
                continue
            phenotype_path = entry.get("phenotype_path")
            if not isinstance(phenotype_path, list) or not phenotype_path:
                phenotype = str(entry.get("phenotype") or "").strip()
                phenotype_path = [phenotype] if phenotype else []
            phenotype_path = [
                str(value).strip() for value in phenotype_path if str(value).strip()
            ]
            if not phenotype_path:
                continue
            representative_source = str(entry.get("source") or "").strip()
            p_value = entry.get("p_value") if representative_source.casefold() == str(source).casefold() else None
            try:
                p_value = float(p_value) if p_value is not None else None
            except (TypeError, ValueError):
                p_value = None
            source_summary_id = _source_summary_id(
                str(source), str(dataset_type), str(gene), variant_id,
                phenotype_path, str(logical_artifact),
            )
            selected_rows += 1
            if source_summary_id in seen_summary_ids:
                continue
            seen_summary_ids.add(source_summary_id)
            writer.writerow(
                (
                    "association-source-summary:"
                    + source_summary_id.partition(":")[2],
                    source_summary_id,
                    "source_summary_association",
                    "source_summary",
                    "million_veteran_program",
                    "MVP",
                    dataset_type,
                    gene,
                    variant_id,
                    entry.get("phenotype") or phenotype_path[-1],
                    json.dumps(phenotype_path, separators=(",", ":"), ensure_ascii=True),
                    " > ".join(phenotype_path),
                    entry.get("label_key"),
                    p_value,
                    entry.get("variation_type"),
                    json.dumps(entry.get("ancestry"), separators=(",", ":"), ensure_ascii=True)
                    if entry.get("ancestry") is not None else None,
                    json.dumps(entry.get("metadata"), separators=(",", ":"), ensure_ascii=True)
                    if entry.get("metadata") is not None else None,
                    representative_source or None,
                    "not_applicable",
                    None,
                    missing_fields_json,
                    logical_artifact,
                    json.dumps(
                        {key: value for key, value in entry.items() if value is not None},
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ),
                    content_contract,
                    publication_mode,
                    ordinal,
                )
            )

    connection = duckdb.connect(config={"threads": "1", "memory_limit": "1GB"})
    try:
        connection.execute("SET preserve_insertion_order=false")
        escaped_csv = str(csv_path).replace("'", "''")
        escaped_parquet = str(parquet_path).replace("'", "''")
        connection.execute(
            f"""
COPY (
    SELECT *
    FROM read_csv('{escaped_csv}', header=true, all_varchar=true)
) TO '{escaped_parquet}'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
"""
        )
        unique_rows = len(seen_summary_ids)
    finally:
        connection.close()
        csv_path.unlink(missing_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise RuntimeError(f"Uncheckpointed source-summary target exists: {output}")
    parquet_path.replace(output)
    return {
        "logical_artifact": logical_artifact,
        "source": source,
        "gene_id": gene,
        "dataset_type": dataset_type,
        "input_size_bytes": int(size_bytes),
        "input_modified_at_ns": int(modified_at_ns),
        "selected_source_rows": selected_rows,
        "unique_summary_rows": unique_rows,
        "output_path": str(output),
        "output_size_bytes": output.stat().st_size,
        "output_sha256": _sha256(output),
    }


class SourceSummaryIndexBuilder:
    def __init__(
        self,
        *,
        source_db: Path,
        artifact_root: Path,
        serving_root: Path,
        workers: int = 3,
        progress_interval: int = 1000,
    ) -> None:
        self.source_db = source_db.resolve()
        self.artifact_root = artifact_root.resolve()
        self.serving_root = serving_root.resolve()
        self.workers = max(1, int(workers))
        self.progress_interval = max(1, int(progress_interval))
        self.table_root = self.serving_root / "tables" / TABLE_NAME
        self.work_root = self.serving_root / ".source-summary-work"
        self.checkpoint_db = self.serving_root / "source-summary-index-checkpoint.duckdb"
        self.file_manifest = self.serving_root / "source-summary-index-files.jsonl"
        self.log_path = self.serving_root / "source-summary-index.log"

    def build(self) -> dict[str, Any]:
        self.serving_root.mkdir(parents=True, exist_ok=True)
        self.table_root.mkdir(parents=True, exist_ok=True)
        checkpoint = duckdb.connect(str(self.checkpoint_db))
        checkpoint.execute(
            """CREATE TABLE IF NOT EXISTS processed_artifacts (
              logical_artifact VARCHAR PRIMARY KEY,
              source VARCHAR NOT NULL,
              gene_id VARCHAR NOT NULL,
              dataset_type VARCHAR NOT NULL,
              input_size_bytes UBIGINT NOT NULL,
              input_modified_at_ns UBIGINT NOT NULL,
              selected_source_rows UBIGINT NOT NULL,
              unique_summary_rows UBIGINT NOT NULL,
              output_path VARCHAR NOT NULL,
              output_size_bytes UBIGINT NOT NULL,
              output_sha256 VARCHAR NOT NULL,
              processed_at TIMESTAMP NOT NULL
            )"""
        )
        completed = {
            row[0]
            for row in checkpoint.execute(
                "SELECT logical_artifact FROM processed_artifacts"
            ).fetchall()
        }
        source = duckdb.connect(str(self.source_db), read_only=True)
        artifacts = source.execute(
            """SELECT source, gene_id, dataset_type, logical_artifact, size_bytes,
                      modified_at_ns, provider_detail_reason, missing_fields_json,
                      content_contract, publication_mode
            FROM source_summary_artifacts
            WHERE provider_detail_status IN ('not_applicable', 'unavailable')
              AND publication_mode='gene_scoped_on_demand'
            ORDER BY dataset_type, gene_id, logical_artifact"""
        ).fetchall()
        source.close()
        tasks: list[tuple[Any, ...]] = []
        for row in artifacts:
            source_name, gene, dataset_type, logical = map(str, row[:4])
            if logical in completed:
                continue
            source_path = (self.artifact_root / logical).resolve()
            source_path.relative_to(self.artifact_root)
            gene_bucket = hashlib.sha256(gene.encode("utf-8")).hexdigest()[:2]
            gene_key = hashlib.sha256(gene.encode("utf-8")).hexdigest()[:16]
            file_token = hashlib.sha256(logical.encode("utf-8")).hexdigest()[:24]
            output = (
                self.table_root / f"gene_bucket={gene_bucket}"
                / f"gene_key={gene_key}" / f"part-{file_token}.parquet"
            )
            if output.exists():
                orphaned = output.with_name(
                    output.name
                    + ".uncheckpointed-"
                    + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                )
                output.rename(orphaned)
            tasks.append(
                (
                    source_name, gene, dataset_type, logical, int(row[4]), int(row[5]),
                    row[6], str(row[7]), row[8], row[9], str(source_path),
                    str(output), str(self.work_root),
                )
            )
        started = time.perf_counter()
        processed_count = len(completed)
        selected_total = int(
            checkpoint.execute(
                "SELECT coalesce(sum(selected_source_rows),0) FROM processed_artifacts"
            ).fetchone()[0]
        )
        unique_total = int(
            checkpoint.execute(
                "SELECT coalesce(sum(unique_summary_rows),0) FROM processed_artifacts"
            ).fetchone()[0]
        )

        def save(result: dict[str, Any]) -> None:
            nonlocal processed_count, selected_total, unique_total
            checkpoint.execute(
                """INSERT INTO processed_artifacts VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
                [
                    result["logical_artifact"], result["source"], result["gene_id"],
                    result["dataset_type"], result["input_size_bytes"],
                    result["input_modified_at_ns"], result["selected_source_rows"],
                    result["unique_summary_rows"], result["output_path"],
                    result["output_size_bytes"], result["output_sha256"],
                ],
            )
            processed_count += 1
            selected_total += int(result["selected_source_rows"])
            unique_total += int(result["unique_summary_rows"])
            if processed_count % self.progress_interval == 0:
                LOGGER.info(
                    "source-summary index progress artifacts=%d/%d selected_rows=%d "
                    "unique_rows=%d elapsed_seconds=%.1f workers=%d",
                    processed_count, len(artifacts), selected_total, unique_total,
                    time.perf_counter() - started, self.workers,
                )

        if self.workers == 1:
            for task in tasks:
                save(_process_artifact(task))
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.workers) as executor:
                for result in executor.map(_process_artifact, tasks, chunksize=1):
                    save(result)

        rows = checkpoint.execute(
            """SELECT logical_artifact, source, gene_id, dataset_type,
                      selected_source_rows, unique_summary_rows, output_path,
                      output_size_bytes, output_sha256
            FROM processed_artifacts ORDER BY logical_artifact"""
        ).fetchall()
        with self.file_manifest.open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(dict(zip(
                    ("logical_artifact", "source", "gene_id", "dataset_type",
                     "selected_source_rows", "unique_summary_rows", "output_path",
                     "output_size_bytes", "output_sha256"), row
                )), sort_keys=True) + "\n")
        checkpoint.close()
        manifest_path = self.serving_root / "serving-manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest.setdefault("tables", {})[TABLE_NAME] = {
            "path": str(self.table_root.relative_to(self.serving_root)),
            "rows": unique_total,
            "selected_source_rows": selected_total,
            "files": len(rows),
            "source_artifacts": len(artifacts),
            "partition_key": "gene_id",
            "bucket_function": "sha256(gene_id)[0:2]",
            "bucket_characters": 2,
            "gene_key_function": "sha256(gene_id)[0:16]",
            "directory_layout": "nested_gene_key",
            "contract": CONTRACT,
            "provider_detail_status": "not_applicable",
            "record_kind": "source_summary_association",
            "evidence_granularity": "source_summary",
        }
        manifest["schema_version"] = "2.9.0-rc2"
        manifest["source_summary_index_built_at"] = _utc_now()
        temporary = manifest_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        temporary.replace(manifest_path)
        checksum = _sha256(manifest_path)
        manifest_path.with_suffix(".json.sha256").write_text(
            f"{checksum}  {manifest_path.name}\n"
        )
        return {
            "contract": CONTRACT,
            "built_at": manifest["source_summary_index_built_at"],
            "source_artifacts": len(artifacts),
            "selected_source_rows": selected_total,
            "unique_summary_rows": unique_total,
            "files": len(rows),
            "table_root": str(self.table_root),
            "file_manifest": str(self.file_manifest),
            "checkpoint_db": str(self.checkpoint_db),
            "serving_manifest": str(manifest_path),
            "serving_manifest_sha256": checksum,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
