#!/usr/bin/env python3
"""Ingest legacy CVD/TRAIT raw association files into DuckDB points table.

This script normalizes legacy DataManager raw files into the same long-form
association-point schema used by the MVP fast ingest table.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.adapters import PhenotypeMapper  # noqa: E402
from datahub.adapters.common import expand_input_paths  # noqa: E402


ANCESTRY_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("gnomad_genome.af.af", "af_total", "Total"),
    ("gnomad_genome.af.af_afr", "af_afr", "African"),
    ("gnomad_genome.af.af_amr", "af_amr", "Admixed American"),
    ("gnomad_genome.af.af_asj", "af_asj", "Ashkenazi Jewish"),
    ("gnomad_genome.af.af_eas", "af_eas", "East Asian"),
    ("gnomad_genome.af.af_fin", "af_fin", "Finnish"),
    ("gnomad_genome.af.af_nfe", "af_nfe", "European"),
    ("gnomad_genome.af.af_oth", "af_oth", "Other"),
)


@dataclass(frozen=True)
class LegacyRawIngestJob:
    """One file-level ingest unit for checkpointed processing."""

    file_path: Path
    dataset_type: str
    dataset_id: str
    source: str


class LegacyRawCheckpoint:
    """File-level checkpoint for resumable legacy raw ingestion."""

    VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.payload = self._default_payload()

    @classmethod
    def _default_payload(cls) -> dict[str, Any]:
        return {
            "version": cls.VERSION,
            "completed_jobs": {},
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

        if int(loaded.get("version", 0)) != self.VERSION:
            self.payload = self._default_payload()
            return

        if not isinstance(loaded.get("completed_jobs"), dict):
            loaded["completed_jobs"] = {}

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

    def completed(self) -> set[str]:
        completed = self.payload.get("completed_jobs", {})
        if not isinstance(completed, dict):
            return set()
        return set(completed.keys())

    def mark_completed(self, job: LegacyRawIngestJob, rows_inserted: int) -> None:
        key = _job_key(job)
        token = _job_token(key)
        completed = self.payload.setdefault("completed_jobs", {})
        completed[key] = {
            "job_token": token,
            "dataset_type": job.dataset_type,
            "dataset_id": job.dataset_id,
            "source": job.source,
            "rows_inserted": int(rows_inserted),
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self.save()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest legacy CVD/TRAIT raw files into DuckDB association points "
            "with resumable, idempotent file-level writes."
        )
    )
    parser.add_argument(
        "--cvd-input-path",
        action="append",
        dest="cvd_input_paths",
        default=[],
        help="Legacy CVD raw file, directory, or glob. Repeat for multiple paths.",
    )
    parser.add_argument(
        "--trait-input-path",
        action="append",
        dest="trait_input_paths",
        default=[],
        help="Legacy trait raw file, directory, or glob. Repeat for multiple paths.",
    )
    parser.add_argument("--db-path", required=True, help="DuckDB database path.")
    parser.add_argument(
        "--table-name",
        default="mvp_association_points",
        help="Target DuckDB points table name.",
    )
    parser.add_argument(
        "--dataset-id-cvd",
        default="hbp_legacy_cvd_association",
        help="Dataset ID for CVD raw files.",
    )
    parser.add_argument(
        "--dataset-id-trait",
        default="hbp_legacy_trait_association",
        help="Dataset ID for trait raw files.",
    )
    parser.add_argument(
        "--source-cvd",
        default="legacy_cvd_raw",
        help="Source label for CVD raw files.",
    )
    parser.add_argument(
        "--source-trait",
        default="legacy_trait_raw",
        help="Source label for trait raw files.",
    )
    parser.add_argument(
        "--phenotype-map-json",
        default=None,
        help="Optional JSON map for phenotype->(dataset_type, category) overrides.",
    )
    parser.add_argument(
        "--fallback-dataset-type",
        default="CVD",
        help="Fallback dataset type used by phenotype mapper.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=None,
        help=(
            "Optional checkpoint path. "
            "Defaults to <db-path>.legacy_raw_checkpoint.json."
        ),
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint resume and process all files.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Reset checkpoint before ingestion starts.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=0,
        help="DuckDB thread count (0 keeps DuckDB default).",
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="DuckDB memory limit. Use empty string to keep DuckDB default.",
    )
    parser.add_argument(
        "--temp-directory",
        default="/data/hbp/datamart/duckdb_tmp",
        help="DuckDB temp spill directory.",
    )
    parser.add_argument(
        "--preserve-insertion-order",
        action="store_true",
        help="Keep DuckDB insertion-order preservation enabled (uses more memory).",
    )
    parser.add_argument(
        "--csv-strict-mode",
        action="store_true",
        help="Enable strict CSV parsing (default is tolerant parsing).",
    )
    parser.add_argument(
        "--csv-ignore-errors",
        dest="csv_ignore_errors",
        action="store_true",
        help="Skip malformed CSV rows during ingest.",
    )
    parser.add_argument(
        "--no-csv-ignore-errors",
        dest="csv_ignore_errors",
        action="store_false",
        help="Fail on malformed CSV rows.",
    )
    parser.set_defaults(csv_ignore_errors=True)
    parser.add_argument(
        "--csv-null-padding",
        dest="csv_null_padding",
        action="store_true",
        help="Pad missing trailing columns with NULL.",
    )
    parser.add_argument(
        "--no-csv-null-padding",
        dest="csv_null_padding",
        action="store_false",
        help="Disable CSV null padding.",
    )
    parser.set_defaults(csv_null_padding=True)
    parser.add_argument(
        "--duckdb-progress-bar",
        dest="duckdb_progress_bar",
        action="store_true",
        help="Enable DuckDB internal progress bar output.",
    )
    parser.add_argument(
        "--no-duckdb-progress-bar",
        dest="duckdb_progress_bar",
        action="store_false",
        help="Disable DuckDB internal progress bar output.",
    )
    parser.set_defaults(duckdb_progress_bar=True)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
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
            fallback_dataset_type=args.fallback_dataset_type,
        )
    except Exception:
        return PhenotypeMapper(mapping={}, fallback_dataset_type=args.fallback_dataset_type)


def _safe_table_name(table_name: str) -> str:
    if not table_name:
        raise ValueError("table name cannot be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if table_name[0].isdigit() or any(char not in allowed for char in table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    return table_name


def _normalized_file_path(path: Path) -> str:
    return str(path.resolve())


def _job_key(job: LegacyRawIngestJob) -> str:
    return f"{job.dataset_type}::{_normalized_file_path(job.file_path)}"


def _job_token(job_key: str) -> str:
    return hashlib.sha1(job_key.encode("utf-8")).hexdigest()[:16]


def _progress_bar(progress: float, width: int = 24) -> str:
    clamped = max(0.0, min(1.0, progress))
    filled = int(round(clamped * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + f"] {int(clamped * 100):3d}%"


def _log_file_progress(
    logger: logging.Logger,
    *,
    file_index: int,
    total_files: int,
    file_name: str,
    progress: float,
    phase: str,
    extra: str = "",
) -> None:
    suffix = f" | {extra}" if extra else ""
    logger.info(
        "File %d/%d %s %s %s%s",
        file_index,
        total_files,
        file_name,
        _progress_bar(progress),
        phase,
        suffix,
    )


def _prepare_mapping_rows(mapper: PhenotypeMapper) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for phenotype_slug, details in mapper.mapping.items():
        dataset_type, category = details
        rows.append(
            (
                phenotype_slug,
                str(dataset_type).upper(),
                PhenotypeMapper.normalize(category),
            )
        )
    return rows


def _create_mapping_table(connection: Any, mapper: PhenotypeMapper) -> None:
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE phenotype_map("
        "phenotype_slug VARCHAR, dataset_type VARCHAR, category VARCHAR)"
    )
    rows = _prepare_mapping_rows(mapper)
    if rows:
        connection.executemany("INSERT INTO phenotype_map VALUES (?, ?, ?)", rows)


def _build_stage_sql(
    *,
    file_path: Path,
    dataset_id: str,
    source: str,
    dataset_type: str,
    csv_strict_mode: bool,
    csv_ignore_errors: bool,
    csv_null_padding: bool,
) -> tuple[str, list[str]]:
    strict_mode = "true" if csv_strict_mode else "false"
    ignore_errors = "true" if csv_ignore_errors else "false"
    null_padding = "true" if csv_null_padding else "false"

    ancestry_selects: list[str] = []
    missing_checks: list[str] = []
    for _source_col, alias_name, label in ANCESTRY_COLUMNS:
        ancestry_selects.append(
            """
    SELECT
        dataset_id,
        source,
        dataset_type,
        gene_id,
        variant_id,
        phenotype,
        disease_category,
        variation_type,
        clinical_significance,
        most_severe_consequence,
        p_value,
        '{label}' AS ancestry,
        {alias_name} AS ancestry_af
    FROM resolved
""".strip().format(label=label.replace("'", "''"), alias_name=alias_name)
        )
        missing_checks.append(f"{alias_name} IS NULL")

    ancestry_selects.append(
        """
    SELECT
        dataset_id,
        source,
        dataset_type,
        gene_id,
        variant_id,
        phenotype,
        disease_category,
        variation_type,
        clinical_significance,
        most_severe_consequence,
        p_value,
        NULL::VARCHAR AS ancestry,
        NULL::DOUBLE AS ancestry_af
    FROM resolved
    WHERE {missing_condition}
""".strip().format(missing_condition=" AND ".join(missing_checks))
    )

    ancestry_union = "\n    UNION ALL\n".join(ancestry_selects)

    ancestry_columns_sql = "\n".join(
        f"        TRY_CAST(\"{source_col}\" AS DOUBLE) AS {alias_name},"
        for source_col, alias_name, _label in ANCESTRY_COLUMNS
    )

    sql = f"""
CREATE OR REPLACE TEMP TABLE __legacy_file_rows AS
WITH raw AS (
    SELECT
        trim(coalesce("dbsnp.rsid", "MarkerID")) AS variant_id,
        trim("snpeff.ann.gene_id") AS gene_id,
        _slug("Phenotype") AS phenotype,
        CASE
            WHEN upper(trim(coalesce("dbsnp.vartype", ''))) = 'SNP' THEN 'SNP'
            WHEN trim(coalesce("dbsnp.vartype", '')) = '' THEN NULL
            ELSE trim("dbsnp.vartype")
        END AS variation_type,
        trim("clinvar.rcv.clinical_significance") AS clinical_significance,
        trim("snpeff.ann.effect") AS most_severe_consequence,
        TRY_CAST("pval" AS DOUBLE) AS p_value,
{ancestry_columns_sql}
        trim(coalesce("dbsnp.alleles.allele", '')) AS allele_string
    FROM read_csv_auto(
        ?,
        header=true,
        compression='auto',
        all_varchar=true,
        strict_mode={strict_mode},
        ignore_errors={ignore_errors},
        null_padding={null_padding}
    )
),
resolved AS (
    SELECT
        ? AS dataset_id,
        ? AS source,
        ? AS dataset_type,
        r.gene_id,
        r.variant_id,
        r.phenotype,
        coalesce(nullif(_slug(pm.category), ''), '') AS disease_category,
        r.variation_type,
        r.clinical_significance,
        r.most_severe_consequence,
        r.p_value,
        r.af_total,
        r.af_afr,
        r.af_amr,
        r.af_asj,
        r.af_eas,
        r.af_fin,
        r.af_nfe,
        r.af_oth
    FROM raw r
    LEFT JOIN phenotype_map pm
      ON pm.phenotype_slug = r.phenotype
     AND pm.dataset_type = ?
    WHERE r.gene_id <> '' AND r.variant_id <> '' AND r.phenotype <> ''
),
expanded AS (
{ancestry_union}
),
aggregated AS (
    SELECT
        dataset_id,
        source,
        dataset_type,
        gene_id,
        variant_id,
        phenotype,
        coalesce(min(nullif(disease_category, '')), '') AS disease_category,
        min(nullif(variation_type, '')) AS variation_type,
        min(nullif(clinical_significance, '')) AS clinical_significance,
        min(nullif(most_severe_consequence, '')) AS most_severe_consequence,
        min(p_value) AS p_value,
        ancestry,
        min(ancestry_af) AS ancestry_af
    FROM expanded
    GROUP BY
        dataset_id,
        source,
        dataset_type,
        gene_id,
        variant_id,
        phenotype,
        ancestry
)
SELECT
    dataset_id,
    dataset_type,
    source,
    gene_id,
    variant_id,
    phenotype,
    disease_category,
    variation_type,
    clinical_significance,
    most_severe_consequence,
    p_value,
    ancestry,
    ancestry_af,
    NULL::VARCHAR AS phenotype_key,
    ? AS source_file,
    now() AS ingested_at
FROM aggregated;
"""

    params = [
        str(file_path),
        dataset_id,
        source,
        dataset_type,
        dataset_type,
        str(file_path),
    ]
    return sql, params


def _ensure_target_table(connection: Any, table_name: str) -> None:
    connection.execute(
        f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    dataset_id VARCHAR,
    dataset_type VARCHAR,
    source VARCHAR,
    gene_id VARCHAR,
    variant_id VARCHAR,
    phenotype VARCHAR,
    disease_category VARCHAR,
    variation_type VARCHAR,
    clinical_significance VARCHAR,
    most_severe_consequence VARCHAR,
    p_value DOUBLE,
    ancestry VARCHAR,
    ancestry_af DOUBLE,
    phenotype_key VARCHAR,
    source_file VARCHAR,
    ingested_at TIMESTAMP
);
"""
    )


def _ensure_slug_macro(connection: Any) -> None:
    connection.execute(
        "CREATE OR REPLACE TEMP MACRO _slug(v) AS "
        "lower(regexp_replace(replace(trim(coalesce(v, '')), '/', '_'), '\\s+', '_', 'g'))"
    )


def _build_jobs(args: argparse.Namespace) -> list[LegacyRawIngestJob]:
    jobs: list[LegacyRawIngestJob] = []

    for path in sorted(expand_input_paths(args.cvd_input_paths), key=lambda item: str(item)):
        jobs.append(
            LegacyRawIngestJob(
                file_path=path,
                dataset_type="CVD",
                dataset_id=args.dataset_id_cvd,
                source=args.source_cvd,
            )
        )

    for path in sorted(expand_input_paths(args.trait_input_paths), key=lambda item: str(item)):
        jobs.append(
            LegacyRawIngestJob(
                file_path=path,
                dataset_type="TRAIT",
                dataset_id=args.dataset_id_trait,
                source=args.source_trait,
            )
        )

    jobs.sort(key=lambda item: (item.dataset_type, str(item.file_path)))
    return jobs


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("datahub.legacy_raw.ingest")

    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "duckdb is required for ingest_legacy_raw_duckdb.py. Install DataHub requirements first."
        ) from exc

    started = time.perf_counter()
    table_name = _safe_table_name(args.table_name)

    jobs = _build_jobs(args)
    if not jobs:
        raise ValueError(
            "No input files matched. Provide --cvd-input-path and/or --trait-input-path."
        )

    checkpoint_path = (
        Path(args.checkpoint_path)
        if args.checkpoint_path
        else Path(f"{args.db_path}.legacy_raw_checkpoint.json")
    )
    checkpoint = LegacyRawCheckpoint(checkpoint_path)
    checkpoint.load()
    if args.reset_checkpoint:
        checkpoint.reset()
        logger.info("Checkpoint reset: %s", checkpoint_path)

    completed_keys = checkpoint.completed() if not args.no_resume else set()
    pending_jobs = [job for job in jobs if _job_key(job) not in completed_keys]
    skipped_jobs = len(jobs) - len(pending_jobs)

    logger.info(
        "Legacy raw ingest start: files=%d pending=%d skipped=%d db=%s table=%s",
        len(jobs),
        len(pending_jobs),
        skipped_jobs,
        args.db_path,
        table_name,
    )

    mapper = _load_mapper(args)

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(db_path))

    if args.threads > 0:
        connection.execute(f"PRAGMA threads={int(args.threads)}")
    if args.memory_limit:
        mem = str(args.memory_limit).strip().replace("'", "")
        if mem:
            connection.execute(f"SET memory_limit='{mem}'")
    if not args.preserve_insertion_order:
        connection.execute("SET preserve_insertion_order=false")
    if args.temp_directory:
        temp_dir = Path(args.temp_directory)
        temp_dir.mkdir(parents=True, exist_ok=True)
        safe_temp = str(temp_dir).replace("'", "")
        connection.execute(f"SET temp_directory='{safe_temp}'")
    if args.duckdb_progress_bar:
        try:
            connection.execute("SET enable_progress_bar = true")
            try:
                connection.execute("SET enable_progress_bar_print = true")
            except Exception:
                pass
            logger.info("DuckDB internal progress bar enabled.")
        except Exception as exc:
            logger.warning("Could not enable DuckDB progress bar: %s", exc)

    logger.info(
        "DuckDB runtime settings: threads=%s memory_limit=%s preserve_insertion_order=%s temp_directory=%s",
        str(args.threads if args.threads > 0 else "default"),
        str(args.memory_limit or "default"),
        str(args.preserve_insertion_order),
        str(args.temp_directory or "default"),
    )
    logger.info(
        "CSV runtime settings: strict_mode=%s ignore_errors=%s null_padding=%s",
        str(args.csv_strict_mode),
        str(args.csv_ignore_errors),
        str(args.csv_null_padding),
    )

    _ensure_target_table(connection, table_name)
    _ensure_slug_macro(connection)
    _create_mapping_table(connection, mapper)

    total_inserted = 0
    processed_jobs = 0
    per_job_rows: dict[str, int] = {}

    try:
        for index, job in enumerate(pending_jobs, start=1):
            job_key = _job_key(job)
            token = _job_token(job_key)
            file_name = job.file_path.name
            logger.info(
                (
                    "Legacy raw ingest file %d/%d start: %s token=%s "
                    "dataset_type=%s source=%s"
                ),
                index,
                len(pending_jobs),
                job.file_path,
                token,
                job.dataset_type,
                job.source,
            )
            _log_file_progress(
                logger,
                file_index=index,
                total_files=len(pending_jobs),
                file_name=file_name,
                progress=0.0,
                phase="stage query start",
            )

            stage_sql, stage_params = _build_stage_sql(
                file_path=job.file_path,
                dataset_id=job.dataset_id,
                source=job.source,
                dataset_type=job.dataset_type,
                csv_strict_mode=args.csv_strict_mode,
                csv_ignore_errors=args.csv_ignore_errors,
                csv_null_padding=args.csv_null_padding,
            )

            rows_inserted = 0
            connection.execute("BEGIN TRANSACTION")
            try:
                stage_started = time.perf_counter()
                connection.execute(stage_sql, stage_params)
                stage_elapsed = time.perf_counter() - stage_started
                _log_file_progress(
                    logger,
                    file_index=index,
                    total_files=len(pending_jobs),
                    file_name=file_name,
                    progress=0.75,
                    phase="stage query complete",
                    extra=f"elapsed={stage_elapsed:.2f}s",
                )

                connection.execute(
                    f"DELETE FROM {table_name} WHERE source_file = ? AND source = ? AND dataset_type = ?",
                    [str(job.file_path), job.source, job.dataset_type],
                )

                insert_started = time.perf_counter()
                connection.execute(f"INSERT INTO {table_name} SELECT * FROM __legacy_file_rows")
                insert_elapsed = time.perf_counter() - insert_started
                _log_file_progress(
                    logger,
                    file_index=index,
                    total_files=len(pending_jobs),
                    file_name=file_name,
                    progress=0.90,
                    phase="insert complete",
                    extra=f"elapsed={insert_elapsed:.2f}s",
                )

                rows_inserted = int(
                    connection.execute("SELECT COUNT(*) FROM __legacy_file_rows").fetchone()[0]
                )
                connection.execute("DROP TABLE IF EXISTS __legacy_file_rows")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                connection.execute("DROP TABLE IF EXISTS __legacy_file_rows")
                raise

            total_inserted += rows_inserted
            processed_jobs += 1
            per_job_rows[job_key] = rows_inserted

            if not args.no_resume:
                checkpoint.mark_completed(job, rows_inserted)
                _log_file_progress(
                    logger,
                    file_index=index,
                    total_files=len(pending_jobs),
                    file_name=file_name,
                    progress=0.98,
                    phase="checkpoint updated",
                )

            logger.info(
                "Legacy raw ingest file %d/%d complete: %s rows_inserted=%d",
                index,
                len(pending_jobs),
                job.file_path,
                rows_inserted,
            )
            _log_file_progress(
                logger,
                file_index=index,
                total_files=len(pending_jobs),
                file_name=file_name,
                progress=1.0,
                phase="done",
                extra=f"rows_inserted={rows_inserted}",
            )
    finally:
        connection.close()

    elapsed = time.perf_counter() - started
    logger.info(
        (
            "Legacy raw ingest complete in %.2fs | files=%d processed=%d skipped=%d "
            "rows_inserted=%d"
        ),
        elapsed,
        len(jobs),
        processed_jobs,
        skipped_jobs,
        total_inserted,
    )

    print(
        json.dumps(
            {
                "engine": "duckdb_legacy_raw_ingest",
                "db_path": str(db_path),
                "table_name": table_name,
                "file_count": len(jobs),
                "processed_files": processed_jobs,
                "skipped_files": skipped_jobs,
                "rows_inserted": total_inserted,
                "resume_enabled": not args.no_resume,
                "checkpoint_path": str(checkpoint_path),
                "per_file_rows": per_job_rows,
                "elapsed_seconds": round(elapsed, 2),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
