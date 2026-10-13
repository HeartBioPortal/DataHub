#!/usr/bin/env python3
"""Compare association evidence v2 category counts with published v1 artifacts."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


EXAMPLES = (
    ("TTN", "TRAIT", "qrs_interval_europeans", "rs202094100"),
    ("PCSK9", "TRAIT", "qrs_interval_europeans", "rs141502002"),
    ("ANK2", "CVD", "mitral_annular_calcification", "rs10013743"),
    ("HMGCR", "TRAIT", "rr_interval", "rs2303151"),
    ("BMPR2", "CVD", "mitral_annular_calcification", "rs1048829"),
)
DIMENSIONS = ("vc", "msc", "cs")
REASONS = {
    "vc": (
        "v2 derives a canonical class only from parseable unordered source alleles; "
        "provider-unavailable variants remain unresolved"
    ),
    "msc": (
        "v2 preserves every source/transcript consequence and counts distinct variants "
        "per term; v1 selected one compact representative term"
    ),
    "cs": (
        "v2 preserves every source clinical assertion and counts distinct variants per "
        "term; v1 selected one compact representative term"
    ),
}


def category_difference_reason(
    *,
    gene: str,
    dataset_type: str,
    dimension: str,
    v1_count: int,
    v2_count: int,
    v2_totals: dict[tuple[str, str], dict[str, Any]],
) -> str:
    if v1_count == v2_count:
        return ""
    if (gene, dataset_type) not in v2_totals:
        return (
            "No v2 gene/dataset summary was recoverable. The v1 identifier may be invalid, "
            "composite, unsupported by the archived provider gene field, or absent from the "
            "retained source-summary artifacts; review this gene row explicitly."
        )
    if v1_count == 0 and v2_count > 0:
        return (
            f"{REASONS[dimension]}; this term was not exposed by the v1 representative "
            "entry but is present in separated v2 evidence."
        )
    if v2_count == 0 and v1_count > 0:
        return (
            f"{REASONS[dimension]}; this v1 representative category is not supported by "
            "the v2 derivation or retained source-linked annotation set."
        )
    return REASONS[dimension]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v2-db", required=True, type=Path)
    parser.add_argument("--v1-overall-root", required=True, type=Path)
    parser.add_argument("--v1-variant-index-root", required=True, type=Path)
    parser.add_argument("--artifact-root", type=Path, help="Root used to resolve logical source-summary artifacts; defaults to the variant-index parent.")
    parser.add_argument(
        "--source-summary-serving-root",
        type=Path,
        help=(
            "Completed association-evidence-v2 serving root containing the "
            "gene-keyed unavailable-provider source-summary index. When supplied, "
            "the audit reuses its verified Parquet partitions instead of rereading "
            "the registered compact JSON artifacts."
        ),
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--v2-sha256", help="Verified manifest checksum; avoids a second full database read.")
    parser.add_argument("--checkpoint-path", type=Path)
    parser.add_argument("--log-path", type=Path)
    parser.add_argument("--progress-interval", type=int, default=1000)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel immutable artifact readers; DuckDB writes remain single-process.",
    )
    parser.add_argument("--max-v1-files", type=int, help="Smoke-test limit per dataset type.")
    parser.add_argument("--reset", action="store_true", help="Reset only this audit's checkpoint and intermediate files.")
    return parser.parse_args()


def configure_logging(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("datahub.association_evidence_v2.audit")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def configuration_hash(args: argparse.Namespace) -> str:
    value = {
        "v2_db": str(args.v2_db.resolve()),
        "v1_overall_root": str(args.v1_overall_root.resolve()),
        "v1_variant_index_root": str(args.v1_variant_index_root.resolve()),
        "artifact_root": str((args.artifact_root or args.v1_variant_index_root.parent).resolve()),
        "source_summary_serving_root": (
            str(args.source_summary_serving_root.resolve())
            if args.source_summary_serving_root else None
        ),
        "runtime_compact_membership": str(
            (args.output_dir / "runtime-compact-membership.duckdb").resolve()
        ),
        "max_v1_files": args.max_v1_files,
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def save_checkpoint(path: Path, configuration: str, phase: str, details: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps({
        "version": 1,
        "configuration_hash": configuration,
        "phase": phase,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "details": details,
    }, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def load_checkpoint(path: Path, configuration: str) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text())
    if value.get("configuration_hash") != configuration:
        raise RuntimeError("Audit checkpoint configuration does not match this run; use --reset.")
    return value


def encode_count_bundle(category: dict, totals: dict) -> dict[str, Any]:
    return {
        "category": [
            {"gene": key[0], "dataset_type": key[1], "dimension": key[2], "label": key[3], "count": count}
            for key, count in sorted(category.items())
        ],
        "totals": [
            {"gene": key[0], "dataset_type": key[1], **value}
            if isinstance(value, dict)
            else {"gene": key[0], "dataset_type": key[1], "count": value}
            for key, value in sorted(totals.items())
        ],
    }


def decode_count_bundle(value: dict[str, Any], *, rich_totals: bool) -> tuple[dict, dict]:
    category = {
        (row["gene"], row["dataset_type"], row["dimension"], row["label"]): int(row["count"])
        for row in value.get("category") or []
    }
    if rich_totals:
        totals = {
            (row["gene"], row["dataset_type"]): {
                key: int(item) for key, item in row.items()
                if key not in {"gene", "dataset_type"}
            }
            for row in value.get("totals") or []
        }
    else:
        totals = {
            (row["gene"], row["dataset_type"]): int(row["count"])
            for row in value.get("totals") or []
        }
    return category, totals


def normalized_label(value: Any) -> str:
    text = str(value or "").strip().replace("_", " ").casefold()
    text = " ".join(text.split())
    if text in {"snp", "single nucleotide variant"}:
        return "snv"
    return text or "unannotated"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def iter_json_array(path: Path, *, chunk_size: int = 1024 * 1024):
    """Stream objects from a JSON array without loading a gene artifact in memory."""

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


def _flush_membership_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, str, str]],
    batch_csv: Path,
) -> None:
    if not rows:
        return
    with batch_csv.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("gene_id", "dataset_type", "variant_id"))
        writer.writerows(rows)
    connection.execute(
        """CREATE OR REPLACE TEMP TABLE __membership_batch AS
        SELECT * FROM read_csv(?, header=true, columns={
          'gene_id':'VARCHAR','dataset_type':'VARCHAR','variant_id':'VARCHAR'
        })""",
        [str(batch_csv)],
    )
    connection.execute(
        """INSERT OR IGNORE INTO compact_variant_membership
        SELECT gene_id, dataset_type, variant_id FROM __membership_batch
        WHERE variant_id IS NOT NULL AND trim(variant_id)<>''"""
    )
    rows.clear()


def _read_membership_artifact(
    task: tuple[str, str, str, str, int, int, str],
) -> dict[str, Any]:
    """Read one immutable artifact without writing shared audit state."""

    source, gene, dataset_type, logical, size_bytes, modified_at_ns, path_text = task
    path = Path(path_text)
    stat = path.stat()
    if int(stat.st_size) != int(size_bytes) or int(stat.st_mtime_ns) != int(modified_at_ns):
        raise RuntimeError(f"Registered source-summary artifact changed: {path}")
    selected_rows = 0
    variant_ids: set[str] = set()
    for entry in iter_json_array(path):
        entry_sources = entry.get("sources")
        if not isinstance(entry_sources, list):
            entry_sources = [entry.get("source")]
        normalized_sources = {
            str(value).strip().casefold() for value in entry_sources if value
        }
        if str(source).casefold() not in normalized_sources:
            continue
        variant_id = str(entry.get("variant_id") or "").strip()
        if not variant_id:
            continue
        selected_rows += 1
        variant_ids.add(variant_id)
    return {
        "source": str(source),
        "gene": str(gene),
        "dataset_type": str(dataset_type),
        "logical": str(logical),
        "size_bytes": int(size_bytes),
        "modified_at_ns": int(modified_at_ns),
        "selected_rows": selected_rows,
        "membership_rows": [
            (str(gene), str(dataset_type), variant_id)
            for variant_id in sorted(variant_ids)
        ],
    }


def materialize_compact_membership(
    registry_connection: duckdb.DuckDBPyConnection,
    *,
    artifact_root: Path,
    runtime_db_path: Path,
    logger: logging.Logger,
    progress_interval: int,
    batch_rows: int = 100_000,
    workers: int = 1,
) -> dict[str, int]:
    """Build a resumable narrow index of unavailable-source variant membership."""

    runtime_db_path.parent.mkdir(parents=True, exist_ok=True)
    batch_csv = runtime_db_path.with_suffix(".membership-batch.csv")
    runtime = duckdb.connect(str(runtime_db_path))
    runtime.execute(
        """CREATE TABLE IF NOT EXISTS compact_variant_membership (
          gene_id VARCHAR NOT NULL,
          dataset_type VARCHAR NOT NULL,
          variant_id VARCHAR NOT NULL,
          PRIMARY KEY (gene_id, dataset_type, variant_id)
        );
        CREATE TABLE IF NOT EXISTS processed_source_summary_artifacts (
          logical_artifact VARCHAR PRIMARY KEY,
          source VARCHAR NOT NULL,
          gene_id VARCHAR NOT NULL,
          dataset_type VARCHAR NOT NULL,
          size_bytes UBIGINT NOT NULL,
          modified_at_ns UBIGINT NOT NULL,
          selected_source_rows UBIGINT NOT NULL,
          processed_at TIMESTAMP NOT NULL
        );"""
    )
    processed = {
        row[0]
        for row in runtime.execute(
            "SELECT logical_artifact FROM processed_source_summary_artifacts"
        ).fetchall()
    }
    artifacts = registry_connection.execute(
        """SELECT source, gene_id, dataset_type, logical_artifact, size_bytes,
                  modified_at_ns
        FROM source_summary_artifacts
        WHERE provider_detail_status='unavailable'
          AND publication_mode='gene_scoped_on_demand'
        ORDER BY dataset_type, gene_id, logical_artifact"""
    ).fetchall()
    started = time.perf_counter()
    completed = len(processed)
    selected_rows_total = int(
        runtime.execute(
            "SELECT coalesce(sum(selected_source_rows), 0) FROM processed_source_summary_artifacts"
        ).fetchone()[0]
    )
    tasks = []
    for source, gene, dataset_type, logical, size_bytes, modified_at_ns in artifacts:
        if logical in processed:
            continue
        path = (artifact_root / str(logical)).resolve()
        try:
            path.relative_to(artifact_root.resolve())
        except ValueError as exc:
            raise RuntimeError(f"Artifact escapes configured root: {logical}") from exc
        tasks.append((
            str(source), str(gene), str(dataset_type), str(logical),
            int(size_bytes), int(modified_at_ns), str(path),
        ))

    def commit_result(result: dict[str, Any]) -> None:
        nonlocal completed, selected_rows_total
        runtime.execute("BEGIN TRANSACTION")
        try:
            rows = result["membership_rows"]
            for offset in range(0, len(rows), max(1, batch_rows)):
                batch = rows[offset : offset + max(1, batch_rows)]
                _flush_membership_rows(runtime, batch, batch_csv)
            runtime.execute(
                """INSERT INTO processed_source_summary_artifacts VALUES
                (?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
                [
                    result["logical"], result["source"], result["gene"],
                    result["dataset_type"], result["size_bytes"],
                    result["modified_at_ns"], result["selected_rows"],
                ],
            )
            runtime.execute("COMMIT")
        except Exception:
            runtime.execute("ROLLBACK")
            raise
        finally:
            batch_csv.unlink(missing_ok=True)
        completed += 1
        selected_rows_total += int(result["selected_rows"])
        processed.add(str(result["logical"]))
        if completed % max(1, progress_interval) == 0:
            logger.info(
                "runtime membership progress artifacts=%d/%d selected_rows=%d elapsed=%.1fs workers=%d",
                completed, len(artifacts), selected_rows_total,
                time.perf_counter() - started, max(1, workers),
            )

    worker_count = max(1, int(workers))
    if worker_count == 1:
        for task in tasks:
            commit_result(_read_membership_artifact(task))
    else:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=worker_count)
        task_iterator = iter(tasks)
        futures: dict[concurrent.futures.Future, tuple] = {}

        def submit_next() -> bool:
            try:
                task = next(task_iterator)
            except StopIteration:
                return False
            futures[executor.submit(_read_membership_artifact, task)] = task
            return True

        try:
            for _ in range(worker_count * 2):
                if not submit_next():
                    break
            while futures:
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    futures.pop(future)
                    commit_result(future.result())
                    submit_next()
        except BaseException:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)
    distinct_membership = int(
        runtime.execute("SELECT count(*) FROM compact_variant_membership").fetchone()[0]
    )
    runtime.close()
    logger.info(
        "runtime membership complete artifacts=%d distinct_gene_dataset_variants=%d selected_rows=%d elapsed=%.1fs",
        completed, distinct_membership, selected_rows_total,
        time.perf_counter() - started,
    )
    return {
        "artifacts": completed,
        "distinct_gene_dataset_variants": distinct_membership,
        "selected_source_rows": selected_rows_total,
    }



def materialize_indexed_compact_membership(
    *,
    serving_root: Path,
    runtime_db_path: Path,
    logger: logging.Logger,
) -> dict[str, int]:
    """Build resumable membership from a completed gene-keyed serving index."""

    serving_root = serving_root.resolve()
    manifest_path = serving_root / "serving-manifest.json"
    manifest_checksum_path = serving_root / "serving-manifest.json.sha256"
    file_manifest_path = serving_root / "source-summary-index-files.jsonl"
    manifest = json.loads(manifest_path.read_text())
    table = (manifest.get("tables") or {}).get(
        "source_summary_associations_by_gene"
    ) or {}
    compatible_schemas = {"2.9.0-rc2", "2.10.0-rc2"}
    if manifest.get("schema_version") not in compatible_schemas:
        raise RuntimeError(
            "Source-summary serving index is not a completed compatible release "
            f"({', '.join(sorted(compatible_schemas))})."
        )
    if table.get("contract") != "source_summary_association_index_v2":
        raise RuntimeError("Unexpected source-summary serving-index contract.")
    if int(table.get("files") or 0) != int(table.get("source_artifacts") or -1):
        raise RuntimeError("Source-summary index file/artifact counts do not reconcile.")
    expected_manifest_checksum = manifest_checksum_path.read_text().split()[0]
    if sha256_file(manifest_path) != expected_manifest_checksum:
        raise RuntimeError("Source-summary serving manifest checksum failed.")
    if not file_manifest_path.exists():
        raise RuntimeError("Source-summary index file manifest is missing.")

    rollup_table = (manifest.get("tables") or {}).get(
        "source_summary_association_base_by_gene"
    ) or {}
    use_rollup = (
        manifest.get("schema_version") == "2.10.0-rc2"
        and rollup_table.get("contract")
        == "source_summary_association_rollup_v2"
    )
    if use_rollup and not (
        serving_root / "source-summary-rollup-files.jsonl"
    ).is_file():
        raise RuntimeError("Source-summary rollup file manifest is missing.")
    membership_table = rollup_table if use_rollup else table
    logger.info(
        "indexed membership source=%s",
        "exact_default_rollup" if use_rollup else "full_source_summary_index",
    )

    runtime_db_path.parent.mkdir(parents=True, exist_ok=True)
    runtime = duckdb.connect(str(runtime_db_path))
    runtime.execute("SET preserve_insertion_order=false")
    runtime.execute("SET memory_limit='4GB'")
    runtime.execute(
        f"SET temp_directory='{runtime_db_path.parent.as_posix()}/duckdb-audit-tmp'"
    )
    runtime.execute(
        """CREATE TABLE IF NOT EXISTS compact_variant_membership (
          gene_id VARCHAR NOT NULL,
          dataset_type VARCHAR NOT NULL,
          variant_id VARCHAR NOT NULL,
          PRIMARY KEY (gene_id, dataset_type, variant_id)
        );
        CREATE TABLE IF NOT EXISTS processed_source_summary_artifacts (
          logical_artifact VARCHAR PRIMARY KEY,
          source VARCHAR NOT NULL,
          gene_id VARCHAR NOT NULL,
          dataset_type VARCHAR NOT NULL,
          size_bytes UBIGINT NOT NULL,
          modified_at_ns UBIGINT NOT NULL,
          selected_source_rows UBIGINT NOT NULL,
          processed_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS indexed_source_summary_buckets (
          gene_bucket VARCHAR PRIMARY KEY,
          membership_rows_after UBIGINT NOT NULL,
          processed_at TIMESTAMP NOT NULL
        );"""
    )
    runtime.execute(
        """INSERT OR IGNORE INTO processed_source_summary_artifacts
        SELECT logical_artifact, source, gene_id, dataset_type,
               0, 0, selected_source_rows, current_timestamp
        FROM read_json_auto(?, format='newline_delimited')""",
        [str(file_manifest_path)],
    )
    processed_buckets = {
        row[0]
        for row in runtime.execute(
            "SELECT gene_bucket FROM indexed_source_summary_buckets"
        ).fetchall()
    }
    table_root = serving_root / str(membership_table["path"])
    started = time.perf_counter()
    for bucket_dir in sorted(table_root.glob("gene_bucket=*")):
        bucket = bucket_dir.name.split("=", 1)[1]
        if bucket in processed_buckets:
            continue
        parquet_glob = str(bucket_dir / "gene_key=*" / "*.parquet")
        runtime.execute("BEGIN TRANSACTION")
        try:
            runtime.execute(
                """INSERT OR IGNORE INTO compact_variant_membership
                SELECT gene_id, dataset_type, variant_id
                FROM read_parquet(?, union_by_name=true)
                WHERE variant_id IS NOT NULL AND trim(variant_id)<>''""",
                [parquet_glob],
            )
            membership_rows = int(
                runtime.execute(
                    "SELECT count(*) FROM compact_variant_membership"
                ).fetchone()[0]
            )
            runtime.execute(
                "INSERT INTO indexed_source_summary_buckets VALUES (?, ?, current_timestamp)",
                [bucket, membership_rows],
            )
            runtime.execute("COMMIT")
        except Exception:
            runtime.execute("ROLLBACK")
            raise
        processed_buckets.add(bucket)
        logger.info(
            "indexed membership progress buckets=%d/256 membership_rows=%d elapsed=%.1fs",
            len(processed_buckets), membership_rows, time.perf_counter() - started,
        )
    artifacts = int(
        runtime.execute(
            "SELECT count(*) FROM processed_source_summary_artifacts"
        ).fetchone()[0]
    )
    selected_rows = int(
        runtime.execute(
            "SELECT coalesce(sum(selected_source_rows),0) FROM processed_source_summary_artifacts"
        ).fetchone()[0]
    )
    distinct_membership = int(
        runtime.execute("SELECT count(*) FROM compact_variant_membership").fetchone()[0]
    )
    bucket_count = int(
        runtime.execute("SELECT count(*) FROM indexed_source_summary_buckets").fetchone()[0]
    )
    runtime.close()
    if artifacts != int(table["source_artifacts"]):
        raise RuntimeError("Indexed artifact count does not match the serving manifest.")
    if selected_rows != int(table["selected_source_rows"]):
        raise RuntimeError("Indexed selected-row count does not match the serving manifest.")
    if bucket_count != 256:
        raise RuntimeError(f"Expected 256 completed gene buckets, observed {bucket_count}.")
    return {
        "artifacts": artifacts,
        "distinct_gene_dataset_variants": distinct_membership,
        "selected_source_rows": selected_rows,
        "indexed_gene_buckets": bucket_count,
    }

def v2_counts(
    connection: duckdb.DuckDBPyConnection,
    *,
    runtime_db_path: Path | None = None,
    v2_db_path: Path | None = None,
) -> tuple[
    dict[tuple[str, str, str, str], int],
    dict[tuple[str, str], dict[str, int]],
]:
    query_connection = connection
    prefix = ""
    membership_cte = """
        SELECT DISTINCT gene_id, dataset_type, variant_id
        FROM variant_phenotype_summaries
    """
    runtime = None
    if runtime_db_path is not None:
        if v2_db_path is None:
            raise ValueError("v2_db_path is required with runtime_db_path")
        runtime = duckdb.connect(str(runtime_db_path))
        escaped_v2_path = str(v2_db_path).replace("'", "''")
        runtime.execute(f"ATTACH '{escaped_v2_path}' AS v2 (READ_ONLY)")
        query_connection = runtime
        prefix = "v2."
        membership_cte = """
            SELECT gene_id, dataset_type, variant_id
            FROM v2.variant_phenotype_summaries
            UNION
            SELECT gene_id, dataset_type, variant_id
            FROM compact_variant_membership
        """
    category: dict[tuple[str, str, str, str], int] = {}
    queries = {
        "vc": f"""
            WITH membership AS ({membership_cte})
            SELECT m.gene_id, m.dataset_type,
                   coalesce(v.variation_type, 'Unresolved'),
                   count(DISTINCT m.variant_id)
            FROM membership m
            LEFT JOIN {prefix}variants v USING (variant_id)
            GROUP BY 1, 2, 3
        """,
        "msc": f"""
            WITH membership AS ({membership_cte})
            SELECT m.gene_id, m.dataset_type, a.consequence,
                   count(DISTINCT m.variant_id)
            FROM membership m
            JOIN {prefix}consequence_annotations a
              ON a.variant_id=m.variant_id AND a.gene_id=m.gene_id
            GROUP BY 1, 2, 3
        """,
        "cs": f"""
            WITH membership AS ({membership_cte})
            SELECT m.gene_id, m.dataset_type, a.clinical_significance,
                   count(DISTINCT m.variant_id)
            FROM membership m
            JOIN {prefix}clinical_assertions a USING (variant_id)
            GROUP BY 1, 2, 3
        """,
    }
    try:
        for dimension, query in queries.items():
            for gene, dataset_type, label, count in query_connection.execute(query).fetchall():
                category[(gene, dataset_type, dimension, normalized_label(label))] = int(count)
        for dimension, annotation_table, join_clause in (
            ("msc", "consequence_annotations", "a.variant_id=m.variant_id AND a.gene_id=m.gene_id"),
            ("cs", "clinical_assertions", "a.variant_id=m.variant_id"),
        ):
            for gene, dataset_type, count in query_connection.execute(
                f"""
                WITH membership AS ({membership_cte})
                SELECT m.gene_id, m.dataset_type, count(DISTINCT m.variant_id)
                FROM membership m
                WHERE NOT EXISTS (
                  SELECT 1 FROM {prefix}{annotation_table} a WHERE {join_clause}
                )
                GROUP BY 1, 2
                """
            ).fetchall():
                category[(gene, dataset_type, dimension, "unannotated")] = int(count)

        provider_totals = {
            (row[0], row[1]): {
                "source_observation_count": int(row[2] or 0),
                "association_record_count": int(row[3] or 0),
                "provider_association_record_count": int(row[4] or 0),
            }
            for row in query_connection.execute(
                f"""
                SELECT gene_id, dataset_type, sum(source_observation_count),
                       sum(association_record_count),
                       sum(provider_association_record_count)
                FROM {prefix}variant_phenotype_summaries
                GROUP BY 1, 2
                """
            ).fetchall()
        }
        compact_totals = {}
        if runtime is not None:
            compact_totals = {
                (row[0], row[1]): {
                    "retained_source_summary_count": int(row[2] or 0),
                    "retained_source_summary_artifacts": int(row[3] or 0),
                }
                for row in query_connection.execute(
                    """SELECT gene_id, dataset_type, sum(selected_source_rows), count(*)
                    FROM processed_source_summary_artifacts GROUP BY 1, 2"""
                ).fetchall()
            }
        totals = {}
        for gene, dataset_type, count in query_connection.execute(
            f"""WITH membership AS ({membership_cte})
            SELECT gene_id, dataset_type, count(DISTINCT variant_id)
            FROM membership GROUP BY 1, 2"""
        ).fetchall():
            key = (gene, dataset_type)
            totals[key] = {
                "v2_distinct_variants": int(count),
                **provider_totals.get(key, {
                    "source_observation_count": 0,
                    "association_record_count": 0,
                    "provider_association_record_count": 0,
                }),
                **compact_totals.get(key, {
                    "retained_source_summary_count": 0,
                    "retained_source_summary_artifacts": 0,
                }),
            }
        return category, totals
    finally:
        if runtime is not None:
            runtime.close()

def v1_counts(
    root: Path,
    *,
    logger: logging.Logger,
    progress_interval: int,
    max_files: int | None,
) -> tuple[
    dict[tuple[str, str, str, str], int],
    dict[tuple[str, str], int],
]:
    category: dict[tuple[str, str, str, str], int] = {}
    totals: dict[tuple[str, str], int] = {}
    files_seen = 0
    started = time.perf_counter()
    for dataset_type in ("CVD", "TRAIT"):
        directory = root / dataset_type
        paths = sorted(directory.glob("*.json*"))
        if max_files is not None:
            paths = paths[:max_files]
        for path in paths:
            files_seen += 1
            opener = gzip.open if path.name.endswith(".gz") else open
            with opener(path, "rt", encoding="utf-8") as stream:
                payload = json.load(stream)
            gene = path.name.removesuffix(".gz").removesuffix(".json")
            data = payload.get("data") if isinstance(payload, dict) else {}
            data = data if isinstance(data, dict) else {}
            for dimension in DIMENSIONS:
                values = data.get(dimension) if isinstance(data.get(dimension), dict) else {}
                for label, count in values.items():
                    key = (gene, dataset_type, dimension, normalized_label(label))
                    category[key] = category.get(key, 0) + int(count or 0)
            totals[(gene, dataset_type)] = sum(
                int(value or 0)
                for value in (data.get("vc") or {}).values()
            )
            if files_seen % max(1, progress_interval) == 0:
                logger.info(
                    "v1 count progress files=%d current=%s elapsed=%.1fs",
                    files_seen,
                    path,
                    time.perf_counter() - started,
                )
    logger.info("v1 count complete files=%d elapsed=%.1fs", files_seen, time.perf_counter() - started)
    return category, totals


def compact_variant_index_entries(
    root: Path,
    gene: str,
    dataset_type: str,
    phenotype_slug: str,
    variant_id: str,
) -> list[dict[str, Any]]:
    directory = root / dataset_type
    candidates = (directory / f"{gene}.json.gz", directory / f"{gene}.json")
    path = next((candidate for candidate in candidates if candidate.exists()), None)
    if path is None:
        return []
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    matches = []
    for row in payload if isinstance(payload, list) else []:
        row_path = row.get("phenotype_path") or row.get("disease") or row.get("trait") or []
        row_slug = str(row.get("phenotype") or (row_path[-1] if row_path else "")).strip().lower()
        if str(row.get("variant_id") or "") == variant_id and row_slug == phenotype_slug:
            matches.append(row)
    return matches


def example_audit(
    connection: duckdb.DuckDBPyConnection,
    variant_index_root: Path,
) -> list[dict[str, Any]]:
    results = []
    for gene, dataset_type, phenotype_slug, variant_id in EXAMPLES:
        summary = connection.execute(
            """
            SELECT * FROM variant_phenotype_summaries
            WHERE gene_id=? AND dataset_type=? AND phenotype_slug=? AND variant_id=?
            """,
            [gene, dataset_type, phenotype_slug, variant_id],
        ).fetchone()
        if summary is None:
            results.append(
                {
                    "gene": gene,
                    "dataset_type": dataset_type,
                    "phenotype_slug": phenotype_slug,
                    "variant_id": variant_id,
                    "status": "missing",
                }
            )
            continue
        columns = [
            row[0]
            for row in connection.execute(
                "DESCRIBE variant_phenotype_summaries"
            ).fetchall()
        ]
        value = dict(zip(columns, summary))
        summary_id = value["variant_phenotype_summary_id"]
        source_rows = connection.execute(
            """
            SELECT ar.source, ar.record_kind, count(*), sum(ar.provider_record_count),
                   min(ar.reported_p_value)
            FROM summary_association_records l
            JOIN association_records ar USING (association_record_id)
            WHERE l.variant_phenotype_summary_id=?
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
            [summary_id],
        ).fetchall()
        provider_ids = connection.execute(
            """
            SELECT p.provider_record_id
            FROM summary_association_records s
            JOIN association_record_provider_records l USING (association_record_id)
            JOIN provider_records p USING (provider_record_id)
            WHERE s.variant_phenotype_summary_id=?
            ORDER BY p.provider_record_id
            """,
            [summary_id],
        ).fetchall()
        compact_entries = compact_variant_index_entries(
            variant_index_root, gene, dataset_type, phenotype_slug, variant_id
        )
        results.append(
            {
                "gene": gene,
                "dataset_type": dataset_type,
                "phenotype_slug": phenotype_slug,
                "variant_id": variant_id,
                "status": "verified",
                "summary_id": summary_id,
                "source_observation_count": int(value["source_observation_count"]),
                "association_record_count": int(value["association_record_count"]),
                "provider_record_count": len(provider_ids),
                "minimum_reported_p_value": value["minimum_reported_p_value"],
                "minimum_p_supplier": value[
                    "minimum_reported_p_value_association_record_id"
                ],
                "consequence_annotation_count": int(
                    value["consequence_annotation_count"]
                ),
                "clinical_assertion_count": int(value["clinical_assertion_count"]),
                "source_breakdown": [
                    {
                        "source": row[0],
                        "record_kind": row[1],
                        "association_records": int(row[2]),
                        "provider_rows": int(row[3]),
                        "minimum_reported_p_value": row[4],
                    }
                    for row in source_rows
                ],
                "provider_record_ids": [row[0] for row in provider_ids],
                "published_v1_compact_entries": compact_entries,
                "published_v1_representative_sources": sorted(
                    {str(row.get("source") or "") for row in compact_entries if row.get("source")}
                ),
                "published_v1_all_sources": sorted(
                    {
                        str(source)
                        for row in compact_entries
                        for source in (row.get("sources") or [row.get("source")])
                        if source
                    }
                ),
                "source_priority_interpretation": (
                    "The v1 compact entry exposes one representative source/annotation. "
                    "The v2 result preserves every recoverable provider row and versioned "
                    "annotation; minimum_reported_p_value is aggregate-only and records its supplier."
                ),
            }
        )
    return results


SUPPLEMENTAL_QA_QUERIES = {
    "association_gene_links_resolve": """
        SELECT count(*) FROM association_record_genes g
        LEFT JOIN association_records a USING (association_record_id)
        WHERE a.association_record_id IS NULL
    """,
    "consequence_link_associations_resolve": """
        SELECT count(*) FROM consequence_annotation_provider_records l
        LEFT JOIN association_records a USING (association_record_id)
        WHERE a.association_record_id IS NULL
    """,
    "clinical_link_associations_resolve": """
        SELECT count(*) FROM clinical_assertion_provider_records l
        LEFT JOIN association_records a USING (association_record_id)
        WHERE a.association_record_id IS NULL
    """,
    "population_link_associations_resolve": """
        SELECT count(*) FROM population_observation_provider_records l
        LEFT JOIN association_records a USING (association_record_id)
        WHERE a.association_record_id IS NULL
    """,
    "summary_consequence_links_resolve": """
        SELECT count(*) FROM summary_consequence_annotations l
        LEFT JOIN variant_phenotype_summaries s USING (variant_phenotype_summary_id)
        LEFT JOIN consequence_annotations a USING (consequence_annotation_id)
        WHERE s.variant_phenotype_summary_id IS NULL OR a.consequence_annotation_id IS NULL
    """,
    "summary_clinical_links_resolve": """
        SELECT count(*) FROM summary_clinical_assertions l
        LEFT JOIN variant_phenotype_summaries s USING (variant_phenotype_summary_id)
        LEFT JOIN clinical_assertions a USING (clinical_assertion_id)
        WHERE s.variant_phenotype_summary_id IS NULL OR a.clinical_assertion_id IS NULL
    """,
    "summary_variants_resolve": """
        SELECT count(*) FROM variant_phenotype_summaries s
        LEFT JOIN variants v USING (variant_id)
        WHERE v.variant_id IS NULL
    """,
    "no_compact_rows_promoted_to_associations": """
        SELECT count(*) FROM association_records
        WHERE record_kind='source_summary' OR provider_detail_status='unavailable'
    """,
    "no_compact_rows_promoted_to_provider_records": """
        SELECT count(*) FROM provider_records
        WHERE source='million_veteran_program'
    """,
}


def supplemental_qa(connection: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = []
    for name, query in SUPPLEMENTAL_QA_QUERIES.items():
        violations = int(connection.execute(query).fetchone()[0])
        rows.append({
            "check_name": name,
            "status": "passed" if violations == 0 else "failed",
            "violations": violations,
            "expected": 0,
        })
    return rows


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = args.checkpoint_path or args.output_dir / "audit-checkpoint.json"
    log_path = args.log_path or args.output_dir / "audit.log"
    v2_intermediate = args.output_dir / "intermediate-v2-counts.json"
    v1_intermediate = args.output_dir / "intermediate-v1-counts.json"
    runtime_db_path = args.output_dir / "runtime-compact-membership.duckdb"
    artifact_root = (args.artifact_root or args.v1_variant_index_root.parent).resolve()
    if args.reset:
        for path in (checkpoint_path, v2_intermediate, v1_intermediate, runtime_db_path):
            path.unlink(missing_ok=True)
    logger = configure_logging(log_path)
    configuration = configuration_hash(args)
    checkpoint = load_checkpoint(checkpoint_path, configuration)
    logger.info("audit start v2=%s v1=%s output=%s", args.v2_db, args.v1_overall_root, args.output_dir)

    connection = duckdb.connect(str(args.v2_db), read_only=True)
    if checkpoint.get("phase") in {"v2_counts_complete", "v1_counts_complete", "complete"} and v2_intermediate.exists():
        logger.info("resume: loading v2 count intermediate %s", v2_intermediate)
        v2_category, v2_totals = decode_count_bundle(json.loads(v2_intermediate.read_text()), rich_totals=True)
    else:
        started = time.perf_counter()
        logger.info("v2 runtime membership phase start artifact_root=%s", artifact_root)
        if args.source_summary_serving_root:
            logger.info(
                "reusing completed source-summary serving index root=%s",
                args.source_summary_serving_root,
            )
            membership_stats = materialize_indexed_compact_membership(
                serving_root=args.source_summary_serving_root,
                runtime_db_path=runtime_db_path,
                logger=logger,
            )
        else:
            membership_stats = materialize_compact_membership(
                connection,
                artifact_root=artifact_root,
                runtime_db_path=runtime_db_path,
                logger=logger,
                progress_interval=args.progress_interval,
                workers=args.workers,
            )
        logger.info("v2 count phase start")
        v2_category, v2_totals = v2_counts(
            connection,
            runtime_db_path=runtime_db_path,
            v2_db_path=args.v2_db,
        )
        v2_intermediate.write_text(json.dumps(encode_count_bundle(v2_category, v2_totals), sort_keys=True) + "\n")
        save_checkpoint(checkpoint_path, configuration, "v2_counts_complete", {
            "category_rows": len(v2_category), "gene_dataset_rows": len(v2_totals),
            "runtime_membership": membership_stats,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        })
        checkpoint = load_checkpoint(checkpoint_path, configuration)
        logger.info("v2 count phase complete categories=%d elapsed=%.1fs", len(v2_category), time.perf_counter() - started)
    examples = example_audit(connection, args.v1_variant_index_root)
    qa = [
        dict(zip(("check_name", "status", "observed", "expected"), row))
        for row in connection.execute(
            "SELECT check_name, status, observed_value, expected_value "
            "FROM qa_results ORDER BY check_name"
        ).fetchall()
    ]
    supplemental_qa_rows: list[dict[str, Any]] = []
    supplemental_qa_execution = {
        "status": "not_reexecuted",
        "reason": (
            "The release audit reuses the persisted full-build QA results. "
            "A second set of referential joins over the normalized link tables was "
            "intentionally not rerun because it duplicates build-time checks and "
            "requires an unbounded rescan of up to one billion link rows."
        ),
        "persisted_build_qa_checks": len(qa),
    }
    metadata = connection.execute("SELECT * FROM build_metadata").fetchone()
    metadata_columns = [
        row[0] for row in connection.execute("DESCRIBE build_metadata").fetchall()
    ]
    connection.close()

    if checkpoint.get("phase") in {"v1_counts_complete", "complete"} and v1_intermediate.exists():
        logger.info("resume: loading v1 count intermediate %s", v1_intermediate)
        v1_category, v1_totals = decode_count_bundle(json.loads(v1_intermediate.read_text()), rich_totals=False)
    else:
        v1_category, v1_totals = v1_counts(
            args.v1_overall_root,
            logger=logger,
            progress_interval=args.progress_interval,
            max_files=args.max_v1_files,
        )
        v1_intermediate.write_text(json.dumps(encode_count_bundle(v1_category, v1_totals), sort_keys=True) + "\n")
        save_checkpoint(checkpoint_path, configuration, "v1_counts_complete", {
            "category_rows": len(v1_category), "gene_dataset_rows": len(v1_totals),
        })
    comparison = []
    exact = defaultdict(int)
    changed = defaultdict(int)
    aggregate = defaultdict(lambda: {"v1": 0, "v2": 0})
    for key in sorted(set(v1_category) | set(v2_category)):
        gene, dataset_type, dimension, label = key
        v1 = v1_category.get(key, 0)
        v2 = v2_category.get(key, 0)
        matches = v1 == v2
        exact[dimension] += int(matches)
        changed[dimension] += int(not matches)
        aggregate[dimension]["v1"] += v1
        aggregate[dimension]["v2"] += v2
        comparison.append(
            {
                "gene": gene,
                "dataset_type": dataset_type,
                "dimension": dimension,
                "normalized_label": label,
                "v1_count": v1,
                "v2_count": v2,
                "delta": v2 - v1,
                "status": "unchanged" if matches else "changed_expected_review",
                "difference_contract": "" if matches else REASONS[dimension],
                "difference_reason": category_difference_reason(
                    gene=gene,
                    dataset_type=dataset_type,
                    dimension=dimension,
                    v1_count=v1,
                    v2_count=v2,
                    v2_totals=v2_totals,
                ),
            }
        )

    gene_rows = []
    for key in sorted(set(v1_totals) | set(v2_totals)):
        details = v2_totals.get(key, {})
        v1 = v1_totals.get(key, 0)
        v2 = details.get("v2_distinct_variants", 0)
        gene_rows.append(
            {
                "gene": key[0],
                "dataset_type": key[1],
                "v1_variation_axis_total": v1,
                "v2_distinct_variants": v2,
                "delta": v2 - v1,
                **{k: value for k, value in details.items() if k != "v2_distinct_variants"},
            }
        )

    write_csv(args.output_dir / "category-count-comparison.csv", comparison)
    write_csv(args.output_dir / "gene-count-comparison.csv", gene_rows)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "v2_database": str(args.v2_db.resolve()),
        "v2_database_sha256": args.v2_sha256 or sha256_file(args.v2_db),
        "v1_overall_root": str(args.v1_overall_root.resolve()),
        "v1_variant_index_root": str(args.v1_variant_index_root.resolve()),
        "artifact_root": str(artifact_root),
        "source_summary_serving_root": (
            str(args.source_summary_serving_root.resolve())
            if args.source_summary_serving_root else None
        ),
        "runtime_compact_membership": str(runtime_db_path.resolve()),
        "build_metadata": dict(zip(metadata_columns, metadata)),
        "category_rows": len(comparison),
        "gene_dataset_rows": len(gene_rows),
        "exact_category_rows": dict(exact),
        "changed_category_rows": dict(changed),
        "aggregate_counts": dict(aggregate),
        "five_example_validation": examples,
        "qa_results": qa,
        "supplemental_qa_results": supplemental_qa_rows,
        "supplemental_qa_execution": supplemental_qa_execution,
        "difference_contracts": REASONS,
    }
    report_path = args.output_dir / "release-audit.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
    checksum_lines = []
    for path in sorted(args.output_dir.iterdir()):
        if path.is_file() and path.name != "checksums.sha256":
            checksum_lines.append(f"{sha256_file(path)}  {path.name}")
    (args.output_dir / "checksums.sha256").write_text(
        "\n".join(checksum_lines) + "\n"
    )
    save_checkpoint(checkpoint_path, configuration, "complete", {
        "report": str(report_path),
        "category_rows": len(comparison),
        "gene_dataset_rows": len(gene_rows),
    })
    logger.info("audit complete report=%s categories=%d genes=%d", report_path, len(comparison), len(gene_rows))
    print(json.dumps({
        "output_dir": str(args.output_dir),
        "category_rows": len(comparison),
        "gene_dataset_rows": len(gene_rows),
        "five_examples_verified": sum(row["status"] == "verified" for row in examples),
        "qa_passed": (
            all(row["status"] == "passed" for row in qa)
        ),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
