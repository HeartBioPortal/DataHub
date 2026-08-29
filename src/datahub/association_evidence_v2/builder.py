"""Resumable production-scale builder for association evidence model v2."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import logging
import os
import platform
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from datahub.phenotype_paths import PhenotypePathResolver

from .contracts import (
    ASSOCIATION_RECORD_KIND_PROVIDER,
    ASSOCIATION_RECORD_KIND_SOURCE_SUMMARY,
    POPULATION_FIELDS,
    PROVIDER_DETAIL_AVAILABLE,
    PROVIDER_DETAIL_UNAVAILABLE,
    SCHEMA_VERSION,
    SOURCE_DETAIL_UNAVAILABLE_FIELDS,
)
from .normalization import normalized_alleles, variation_type_from_alleles
from .schema import create_indexes, create_schema


LOGGER = logging.getLogger("datahub.association_evidence_v2")

RAW_COLUMNS: tuple[tuple[str, str], ...] = (
    ("MarkerID", "marker_id_raw"),
    ("pval", "p_value_raw"),
    ("GWASsummaryPath", "gwas_summary_path_raw"),
    ("studyID", "study_id_raw"),
    ("Phenotype", "phenotype_raw"),
    ("Study", "study_raw"),
    ("PMID", "pmid_raw"),
    ("StudyGenomeBuild", "study_genome_build_raw"),
    ("dbsnp.rsid", "variant_id_raw"),
    ("dbsnp.dbsnp_build", "dbsnp_build_raw"),
    ("dbsnp.alleles.allele", "allele_string_raw"),
    ("dbsnp.chrom", "chromosome_raw"),
    ("dbsnp.hg19.start", "hg19_start_raw"),
    ("dbsnp.hg19.end", "hg19_end_raw"),
    ("dbsnp.vartype", "source_variation_type_raw"),
    ("gnomad_genome.af.af", "frequency_total_raw"),
    ("gnomad_genome.af.af_afr", "frequency_afr_raw"),
    ("gnomad_genome.af.af_amr", "frequency_amr_raw"),
    ("gnomad_genome.af.af_asj", "frequency_asj_raw"),
    ("gnomad_genome.af.af_eas", "frequency_eas_raw"),
    ("gnomad_genome.af.af_fin", "frequency_fin_raw"),
    ("gnomad_genome.af.af_nfe", "frequency_nfe_raw"),
    ("gnomad_genome.af.af_oth", "frequency_oth_raw"),
    ("snpeff.ann.gene_id", "gene_id_raw"),
    ("snpeff.ann.effect", "consequence_raw"),
    ("snpeff.ann.putative_impact", "putative_impact_raw"),
    ("snpeff.ann.feature_id", "feature_id_raw"),
    ("snpeff.ann.hgvs_p", "hgvs_p_raw"),
    ("snpeff.ann.protein.length", "protein_length_raw"),
    ("dbnsfp.chrom", "dbnsfp_chrom_raw"),
    ("dbnsfp.hg18.start", "dbnsfp_hg18_start_raw"),
    ("dbnsfp.hg18.end", "dbnsfp_hg18_end_raw"),
    ("dbnsfp.hg19.start", "dbnsfp_hg19_start_raw"),
    ("dbnsfp.hg19.end", "dbnsfp_hg19_end_raw"),
    ("dbnsfp.hg38.start", "dbnsfp_hg38_start_raw"),
    ("dbnsfp.hg38.end", "dbnsfp_hg38_end_raw"),
    ("dbnsfp.ensembl.proteinid", "ensembl_protein_id_raw"),
    ("dbnsfp.ensembl.transcriptid", "ensembl_transcript_id_raw"),
    ("clinvar.rcv.clinical_significance", "clinical_significance_raw"),
)

POPULATION_ALIASES = {
    "gnomad_genome.af.af": "frequency_total_raw",
    "gnomad_genome.af.af_afr": "frequency_afr_raw",
    "gnomad_genome.af.af_amr": "frequency_amr_raw",
    "gnomad_genome.af.af_asj": "frequency_asj_raw",
    "gnomad_genome.af.af_eas": "frequency_eas_raw",
    "gnomad_genome.af.af_fin": "frequency_fin_raw",
    "gnomad_genome.af.af_nfe": "frequency_nfe_raw",
    "gnomad_genome.af.af_oth": "frequency_oth_raw",
}


@dataclass(frozen=True)
class SourceJob:
    path: Path
    logical_path: str
    dataset_type: str
    dataset_id: str
    source: str
    delimiter: str

    @property
    def key(self) -> str:
        return f"{self.dataset_type}:{self.logical_path}"


class BuildCheckpoint:
    VERSION = 1

    def __init__(self, path: Path, configuration_hash: str) -> None:
        self.path = path
        self.configuration_hash = configuration_hash
        self.payload: dict[str, Any] = {
            "version": self.VERSION,
            "configuration_hash": configuration_hash,
            "completed_source_jobs": {},
            "completed_phases": {},
            "updated_at": None,
        }

    def load(self) -> None:
        if not self.path.exists():
            return
        loaded = json.loads(self.path.read_text())
        if loaded.get("version") != self.VERSION:
            raise RuntimeError("Checkpoint version does not match this builder")
        if loaded.get("configuration_hash") != self.configuration_hash:
            raise RuntimeError("Checkpoint configuration does not match this run")
        self.payload = loaded

    def save(self) -> None:
        self.payload["updated_at"] = _utc_now()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self.payload, indent=2, sort_keys=True) + "\n")
        temp.replace(self.path)

    def source_complete(self, key: str) -> bool:
        return key in self.payload["completed_source_jobs"]

    def mark_source_complete(self, job: SourceJob, rows: int, elapsed: float) -> None:
        self.payload["completed_source_jobs"][job.key] = {
            "rows": rows,
            "elapsed_seconds": round(elapsed, 3),
            "completed_at": _utc_now(),
        }
        self.save()

    def phase_complete(self, phase: str) -> bool:
        return phase in self.payload["completed_phases"]

    def mark_phase_complete(self, phase: str, details: dict[str, Any]) -> None:
        self.payload["completed_phases"][phase] = {
            **details,
            "completed_at": _utc_now(),
        }
        self.save()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _clinical_assertion_terms(raw_value: str) -> list[str]:
    """Return distinct source-reported assertion terms without reclassifying them."""
    text = str(raw_value or "").strip()
    if not text or text in {"NA", "[]"}:
        return []
    parsed: Any = None
    try:
        parsed = json.loads(text.replace("\x27", "\x22"))
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = None
    values = parsed if isinstance(parsed, list) else [text]
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _configuration_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read_header(path: Path, delimiter: str) -> list[str]:
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", errors="strict", newline="") as stream:
        return next(csv.reader(stream, delimiter=delimiter))


def _input_select(header: set[str]) -> str:
    seen_aliases: set[str] = set()
    expressions: list[str] = []
    for raw_name, alias in RAW_COLUMNS:
        if alias in seen_aliases:
            continue
        seen_aliases.add(alias)
        if raw_name in header:
            safe_name = raw_name.replace('"', '""')
            expressions.append(f'CAST("{safe_name}" AS VARCHAR) AS {alias}')
        else:
            expressions.append(f"NULL::VARCHAR AS {alias}")
    return ",\n        ".join(expressions)


def _concat_columns(prefix: str = "") -> str:
    seen: set[str] = set()
    expressions = []
    for _raw_name, alias in RAW_COLUMNS:
        if alias in seen:
            continue
        seen.add(alias)
        expressions.append(f"coalesce({prefix}{alias}, '')")
    return ", ".join(expressions)


class EvidenceV2Builder:
    def __init__(
        self,
        *,
        cvd_root: Path,
        trait_root: Path,
        variant_index_root: Path,
        output_db: Path,
        checkpoint_path: Path,
        phenotype_tree: Path,
        release_id: str,
        command: str,
        builder_git_commit: str | None = None,
        threads: int = 4,
        memory_limit: str = "12GB",
        temp_directory: Path | None = None,
        smoke_files: int | None = None,
        smoke_rows_per_file: int | None = None,
        provider_chunk_rows: int = 250_000,
        progress_interval: int = 1,
    ) -> None:
        self.cvd_root = cvd_root.resolve()
        self.trait_root = trait_root.resolve()
        self.variant_index_root = variant_index_root.resolve()
        self.output_db = output_db.resolve()
        self.phenotype_tree = phenotype_tree.resolve()
        self.release_id = release_id
        self.command = command
        self.builder_git_commit = builder_git_commit
        self.threads = max(1, int(threads))
        self.memory_limit = memory_limit
        self.temp_directory = temp_directory.resolve() if temp_directory else None
        self.smoke_files = smoke_files
        self.smoke_rows_per_file = smoke_rows_per_file
        self.provider_chunk_rows = int(provider_chunk_rows)
        self.progress_interval = max(1, int(progress_interval))
        configuration = {
            "schema_version": SCHEMA_VERSION,
            "cvd_root": str(self.cvd_root),
            "trait_root": str(self.trait_root),
            "variant_index_root": str(self.variant_index_root),
            "output_db": str(self.output_db),
            "phenotype_tree": str(self.phenotype_tree),
            "smoke_files": smoke_files,
            "smoke_rows_per_file": smoke_rows_per_file,
        }
        self.checkpoint = BuildCheckpoint(
            checkpoint_path.resolve(), _configuration_hash(configuration)
        )
        self.connection: Any | None = None

    def run(self, *, resume: bool = True) -> dict[str, Any]:
        import duckdb

        started = time.perf_counter()
        if resume:
            self.checkpoint.load()
        self.output_db.parent.mkdir(parents=True, exist_ok=True)
        if self.temp_directory:
            self.temp_directory.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(self.output_db))
        self.connection.execute(f"SET threads={self.threads}")
        self.connection.execute(f"SET memory_limit={_sql_literal(self.memory_limit)}")
        # Provider source lines come from a single-threaded sequence; retaining global
        # insertion order makes wide source-file inserts consume unbounded memory.
        self.connection.execute("SET preserve_insertion_order=false")
        if self.temp_directory:
            self.connection.execute(
                f"SET temp_directory={_sql_literal(str(self.temp_directory))}"
            )
        create_schema(self.connection)
        self._record_build_start()
        self._load_phenotype_registry()
        self._record_source_completeness()

        jobs = self._source_jobs()
        for index, job in enumerate(jobs, start=1):
            if resume and self.checkpoint.source_complete(job.key):
                continue
            file_started = time.perf_counter()
            rows = self._ingest_source_file(job)
            elapsed = time.perf_counter() - file_started
            self.checkpoint.mark_source_complete(job, rows, elapsed)
            if index % self.progress_interval == 0 or index == len(jobs):
                LOGGER.info(
                    "Provider ingest progress files=%d/%d rows=%d file=%s elapsed=%.1fs",
                    index,
                    len(jobs),
                    rows,
                    job.logical_path,
                    elapsed,
                )

        self._run_phase("provider_relations", self._build_provider_relations)
        self._run_phase("source_summaries", self._load_source_summary_records)
        self._run_phase("variants", self._build_variants)
        self._run_phase("consequences", self._build_consequences)
        self._run_phase("clinical_assertions", self._build_clinical_assertions)
        self._run_phase("population_observations", self._build_population_observations)
        self._run_phase("summaries", self._build_summaries)
        self._run_phase("qa", self._run_qa)
        self._run_phase("indexes", lambda: create_indexes(self.connection))
        counts = self._counts()
        completed_at = _utc_now()
        self.connection.execute(
            "UPDATE build_metadata SET status='complete', completed_at=?, counts_json=?",
            [completed_at, _canonical_json(counts)],
        )
        self.connection.execute("CHECKPOINT")
        elapsed = time.perf_counter() - started
        summary = {
            "release_id": self.release_id,
            "schema_version": SCHEMA_VERSION,
            "output_db": str(self.output_db),
            "checkpoint": str(self.checkpoint.path),
            "completed_at": completed_at,
            "elapsed_seconds": round(elapsed, 3),
            "counts": counts,
        }
        LOGGER.info("Evidence v2 build complete: %s", _canonical_json(summary))
        self.connection.close()
        self.connection = None
        return summary

    def _record_build_start(self) -> None:
        assert self.connection is not None
        attempt = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "python": sys.version,
            "platform": platform.platform(),
            "duckdb": self.connection.execute("SELECT version()").fetchone()[0],
            "threads": self.threads,
            "memory_limit": self.memory_limit,
            "temp_directory": str(self.temp_directory) if self.temp_directory else None,
            "command": self.command,
        }
        existing = self.connection.execute(
            "SELECT runtime_json FROM build_metadata ORDER BY built_at LIMIT 1"
        ).fetchone()
        if existing:
            runtime = json.loads(str(existing[0])) if existing[0] else {}
            history = list(runtime.get("run_attempts") or [])
            if not history:
                history.append({key: runtime.get(key) for key in (
                    "python", "platform", "duckdb", "threads", "memory_limit",
                    "temp_directory"
                )})
            history.append(attempt)
            runtime["run_attempts"] = history
            runtime["active_settings"] = attempt
            self.connection.execute(
                "UPDATE build_metadata SET schema_version=?, runtime_json=?, builder_git_commit=?, builder_command=?",
                [SCHEMA_VERSION, _canonical_json(runtime), self.builder_git_commit, self.command],
            )
            return
        runtime = {
            **attempt,
            "run_attempts": [attempt],
            "active_settings": attempt,
        }
        self.connection.execute(
            "INSERT INTO build_metadata VALUES (?, ?, now(), NULL, 'building', ?, ?, ?, ?, ?, NULL, ?)",
            [
                SCHEMA_VERSION,
                self.release_id,
                str(self.cvd_root.parent),
                str(self.variant_index_root),
                self.builder_git_commit,
                self.command,
                _canonical_json(runtime),
                _canonical_json(
                    [
                        "MVP provider-level inputs are unavailable on AWS.",
                        "Legacy gnomAD frequency fields do not identify REF, ALT, or association effect allele.",
                        "Legacy SnpEff and ClinVar enrichment versions are not retained in source rows.",
                    ]
                ),
            ],
        )

    def _source_jobs(self) -> list[SourceJob]:
        jobs = []
        for dataset_type, root, delimiter, dataset_id, source in (
            ("CVD", self.cvd_root, ",", "hbp_legacy_cvd_association", "legacy_cvd_raw"),
            ("TRAIT", self.trait_root, "\t", "hbp_legacy_trait_association", "legacy_trait_raw"),
        ):
            paths = sorted(path for path in root.iterdir() if path.is_file())
            if self.smoke_files is not None:
                paths = paths[: self.smoke_files]
            for path in paths:
                jobs.append(
                    SourceJob(
                        path=path,
                        logical_path=f"raw_data/{root.name}/{path.name}",
                        dataset_type=dataset_type,
                        dataset_id=dataset_id,
                        source=source,
                        delimiter=delimiter,
                    )
                )
        return jobs

    def _load_phenotype_registry(self) -> None:
        assert self.connection is not None
        if self.connection.execute("SELECT count(*) FROM phenotype_registry").fetchone()[0]:
            return
        resolver = PhenotypePathResolver.from_tree_json(self.phenotype_tree)
        rows = []
        for (dataset_type, phenotype), path in sorted(resolver.leaf_paths.items()):
            normalized_path = [str(item) for item in path]
            rows.append(
                (
                    dataset_type,
                    phenotype,
                    _canonical_json(normalized_path),
                    " > ".join(normalized_path),
                )
            )
        if rows:
            self.connection.executemany("INSERT INTO phenotype_registry VALUES (?, ?, ?, ?)", rows)

    def _record_source_completeness(self) -> None:
        assert self.connection is not None
        self.connection.execute("DELETE FROM source_completeness")
        rows = [
            (
                "legacy_cvd_raw",
                '["CVD"]',
                PROVIDER_DETAIL_AVAILABLE,
                None,
                _canonical_json(SOURCE_DETAIL_UNAVAILABLE_FIELDS),
                None,
                _canonical_json([str(self.cvd_root)]),
                "GWAS Catalog-derived AWS snapshot; exact upstream release unavailable",
                None,
                "GWAS Catalog-derived provider rows are recoverable from the archived CVD files.",
            ),
            (
                "legacy_trait_raw",
                '["TRAIT"]',
                PROVIDER_DETAIL_AVAILABLE,
                None,
                _canonical_json(SOURCE_DETAIL_UNAVAILABLE_FIELDS),
                None,
                _canonical_json([str(self.trait_root)]),
                "GWAS Catalog-derived AWS snapshot; exact upstream release unavailable",
                None,
                "GWAS Catalog-derived provider rows are recoverable from the archived trait files.",
            ),
            (
                "million_veteran_program",
                '["CVD", "TRAIT"]',
                PROVIDER_DETAIL_UNAVAILABLE,
                "Provider-level MVP input is not present in the AWS DataHub snapshot.",
                _canonical_json(SOURCE_DETAIL_UNAVAILABLE_FIELDS),
                "variant_index",
                _canonical_json([str(self.variant_index_root)]),
                None,
                None,
                "Only retained compact variant-index source summaries are published; no studies are reconstructed.",
            ),
        ]
        self.connection.executemany("INSERT INTO source_completeness VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

    def _ingest_source_file(self, job: SourceJob) -> int:
        assert self.connection is not None
        header = set(_read_header(job.path, job.delimiter))
        select_columns = _input_select(header)
        raw_concat = _concat_columns("r.")
        delimiter = "\\t" if job.delimiter == "\t" else job.delimiter
        select_sql_template = f"""
WITH source_rows AS (
    SELECT
        nextval('evidence_v2_source_line_seq') AS source_line,
        {select_columns}
    FROM read_csv(
        __HBP_SOURCE_PATH__,
        header=true,
        all_varchar=true,
        delim={_sql_literal(delimiter)},
        quote='"',
        strict_mode=false,
        ignore_errors=false,
        null_padding=true,
        parallel=false,
        maximum_line_size=8388608
    )
),
normalized AS (
    SELECT
        *,
        lower(regexp_replace(replace(trim(coalesce(phenotype_raw, '')), '/', '_'), '\\s+', '_', 'g')) AS phenotype_slug,
        sha256(concat_ws(chr(31), {raw_concat})) AS source_row_sha256
    FROM source_rows r
)
SELECT
    'provider:' || sha256(concat_ws(chr(31), {_sql_literal(job.source)}, {_sql_literal(job.logical_path)}, cast(source_line AS VARCHAR), source_row_sha256)) AS provider_record_id,
    'association:' || sha256(concat_ws(chr(31),
        {_sql_literal(job.dataset_type)}, {_sql_literal(job.source)},
        coalesce(study_id_raw, ''), coalesce(study_raw, ''), coalesce(pmid_raw, ''),
        coalesce(gwas_summary_path_raw, ''), coalesce(study_genome_build_raw, ''),
        coalesce(nullif(trim(variant_id_raw), ''), nullif(trim(marker_id_raw), ''), ''),
        phenotype_slug, coalesce(p_value_raw, '')
    )) AS association_record_id,
    {_sql_literal(job.dataset_id)} AS dataset_id,
    {_sql_literal(job.dataset_type)} AS dataset_type,
    {_sql_literal(job.source)} AS source,
    {_sql_literal(job.logical_path)} AS source_file_logical,
    {_sql_literal(str(job.path))} AS source_file_absolute,
    source_line,
    source_row_sha256,
    marker_id_raw, p_value_raw, gwas_summary_path_raw, study_id_raw, phenotype_raw,
    phenotype_slug, study_raw, pmid_raw, study_genome_build_raw,
    coalesce(nullif(trim(variant_id_raw), ''), nullif(trim(marker_id_raw), '')) AS variant_id_raw,
    dbsnp_build_raw, allele_string_raw, chromosome_raw, hg19_start_raw, hg19_end_raw,
    source_variation_type_raw, frequency_total_raw, frequency_afr_raw, frequency_amr_raw,
    frequency_asj_raw, frequency_eas_raw, frequency_fin_raw, frequency_nfe_raw,
    frequency_oth_raw, gene_id_raw, consequence_raw, putative_impact_raw, feature_id_raw,
    hgvs_p_raw, protein_length_raw, dbnsfp_chrom_raw, dbnsfp_hg18_start_raw,
    dbnsfp_hg18_end_raw, dbnsfp_hg19_start_raw, dbnsfp_hg19_end_raw,
    dbnsfp_hg38_start_raw, dbnsfp_hg38_end_raw, ensembl_protein_id_raw,
    ensembl_transcript_id_raw, clinical_significance_raw, now() AS ingested_at
FROM normalized
"""
        # A failed file is restarted from its first source row; completed files remain
        # protected by the durable file-level checkpoint.
        self.connection.execute("BEGIN")
        try:
            self.connection.execute(
                "DELETE FROM provider_records WHERE dataset_type=? AND source_file_logical=?",
                [job.dataset_type, job.logical_path],
            )
            self.connection.execute("COMMIT")
        except Exception:
            try:
                self.connection.execute("ROLLBACK")
            except Exception:
                pass
            raise

        total_rows = 0
        for chunk_path, offset, expected_rows in self._iter_source_chunks(job):
            insert_sql = select_sql_template.replace(
                "__HBP_SOURCE_PATH__", _sql_literal(str(chunk_path))
            )
            self.connection.execute("BEGIN")
            try:
                # Single-threaded parsing plus a record offset preserves stable source lines
                # without retaining a multi-million-row transaction in memory.
                self.connection.execute("DROP SEQUENCE IF EXISTS evidence_v2_source_line_seq")
                self.connection.execute(
                    f"CREATE SEQUENCE evidence_v2_source_line_seq START {offset + 2}"
                )
                inserted = self.connection.execute(
                    "INSERT INTO provider_records " + insert_sql
                ).fetchone()
                rows = int(inserted[0]) if inserted else 0
                if expected_rows is not None and rows != expected_rows:
                    raise RuntimeError(
                        f"Provider chunk row mismatch for {job.logical_path}: "
                        f"expected {expected_rows}, inserted {rows}"
                    )
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            total_rows += rows
            if total_rows % 1_000_000 == 0:
                LOGGER.info(
                    "Provider file progress file=%s rows=%d", job.logical_path, total_rows
                )
        return total_rows

    def _iter_source_chunks(self, job: SourceJob) -> Iterable[tuple[Path, int, int | None]]:
        """Yield the original source or bounded CSV-aware chunks in one pass."""

        oversized_threshold = 2_000_000_000
        if (
            self.provider_chunk_rows <= 0
            and self.smoke_rows_per_file is None
            and job.path.stat().st_size <= oversized_threshold
        ):
            yield job.path, 0, None
            return
        configured_chunk_rows = self.provider_chunk_rows if self.provider_chunk_rows > 0 else 1_000_000
        chunk_rows = int(self.smoke_rows_per_file or configured_chunk_rows)
        chunk_rows = min(chunk_rows, configured_chunk_rows)
        chunk_root = (self.temp_directory or self.output_db.parent / "duckdb_tmp") / "source_chunks"
        chunk_root.mkdir(parents=True, exist_ok=True)
        chunk_key = hashlib.sha256(job.logical_path.encode()).hexdigest()[:16]
        csv.field_size_limit(8 * 1024 * 1024)
        with job.path.open("r", encoding="utf-8", newline="") as source_handle:
            reader = csv.reader(
                source_handle, delimiter=job.delimiter, quotechar='"', strict=True
            )
            try:
                header = next(reader)
            except StopIteration:
                return
            offset = 0
            chunk_index = 0
            while self.smoke_rows_per_file is None or offset < self.smoke_rows_per_file:
                path = chunk_root / f"{chunk_key}.{chunk_index:06d}.csv"
                row_limit = min(
                    chunk_rows,
                    (self.smoke_rows_per_file - offset)
                    if self.smoke_rows_per_file is not None
                    else chunk_rows,
                )
                written = 0
                with path.open("w", encoding="utf-8", newline="") as chunk_handle:
                    writer = csv.writer(
                        chunk_handle,
                        delimiter=job.delimiter,
                        quotechar='"',
                        lineterminator="\n",
                    )
                    writer.writerow(header)
                    for row in reader:
                        writer.writerow(row)
                        written += 1
                        if written >= row_limit:
                            break
                if written == 0:
                    path.unlink(missing_ok=True)
                    break
                try:
                    yield path, offset, written
                finally:
                    path.unlink(missing_ok=True)
                offset += written
                chunk_index += 1

    def _run_phase(self, name: str, callback: Any) -> None:
        if self.checkpoint.phase_complete(name):
            return
        started = time.perf_counter()
        LOGGER.info("Evidence v2 phase start: %s", name)
        callback()
        elapsed = time.perf_counter() - started
        self.checkpoint.mark_phase_complete(name, {"elapsed_seconds": round(elapsed, 3)})
        LOGGER.info("Evidence v2 phase complete: %s elapsed=%.1fs", name, elapsed)

    def _build_provider_relations(self) -> None:
        assert self.connection is not None
        partition_root = (
            self.temp_directory or self.output_db.parent / "duckdb_tmp"
        ) / "provider_relation_partitions"
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + ".provider-relations.json"
        )
        provider_count = int(
            self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0]
        )
        state: dict[str, Any] = {}
        if state_path.exists():
            state = json.loads(state_path.read_text())
            if int(state.get("provider_record_count", -1)) != provider_count:
                raise RuntimeError(
                    "Provider-relation checkpoint does not match provider record count"
                )

        def save_state() -> None:
            state["provider_record_count"] = provider_count
            state["updated_at"] = _utc_now()
            temporary = state_path.with_suffix(state_path.suffix + ".tmp")
            temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
            temporary.replace(state_path)

        if not state.get("partitions_complete"):
            if partition_root.exists():
                stale = partition_root.with_name(
                    partition_root.name + ".stale-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                )
                partition_root.rename(stale)
            partition_root.parent.mkdir(parents=True, exist_ok=True)
            LOGGER.info(
                "Provider relation partitioning start rows=%d root=%s",
                provider_count,
                partition_root,
            )
            self.connection.execute(
                f"""
COPY (
    SELECT provider_record_id, association_record_id, dataset_id, dataset_type,
           source, source_file_logical, variant_id_raw, phenotype_raw, phenotype_slug,
           study_id_raw, study_raw, pmid_raw, gwas_summary_path_raw,
           study_genome_build_raw, p_value_raw, gene_id_raw,
           substr(association_record_id, 13, 2) AS relation_bucket
    FROM provider_records
    WHERE coalesce(trim(variant_id_raw), '') <> ''
      AND coalesce(trim(phenotype_slug), '') <> ''
) TO {_sql_literal(str(partition_root))}
(FORMAT PARQUET, PARTITION_BY (relation_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
            )
            parquet_files = sorted(partition_root.rglob("*.parquet"))
            if not parquet_files:
                raise RuntimeError("Provider relation partitioning produced no Parquet files")
            state.update(
                {
                    "partitions_complete": True,
                    "partition_files": len(parquet_files),
                    "completed_buckets": [],
                }
            )
            save_state()
            LOGGER.info(
                "Provider relation partitioning complete files=%d", len(parquet_files)
            )

        completed = set(state.get("completed_buckets") or [])
        bucket_directories = sorted(
            path for path in partition_root.iterdir() if path.is_dir()
        )
        if not completed:
            self.connection.execute(
                "CREATE OR REPLACE TABLE association_record_genes AS "
                "SELECT * FROM association_record_genes LIMIT 0"
            )
            self.connection.execute(
                "CREATE OR REPLACE TABLE association_record_provider_records AS "
                "SELECT * FROM association_record_provider_records LIMIT 0"
            )
            for table in (
                "consequence_annotation_provider_records",
                "clinical_assertion_provider_records",
                "population_observation_provider_records",
            ):
                self.connection.execute(
                    f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {table} LIMIT 0"
                )
            self.connection.execute(
                "DELETE FROM association_records WHERE record_kind=?",
                [ASSOCIATION_RECORD_KIND_PROVIDER],
            )

        for index, bucket_directory in enumerate(bucket_directories, start=1):
            bucket = bucket_directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            source_sql = (
                "read_parquet("
                + _sql_literal(str(bucket_directory / "*.parquet"))
                + ")"
            )
            self.connection.execute("BEGIN")
            try:
                # Each bucket is committed atomically. A bucket absent from the
                # durable checkpoint has no retained rows, so rescanning growing
                # output tables for defensive deletes would be quadratic.
                self.connection.execute(
                    f"""
INSERT INTO association_records
SELECT
    p.association_record_id,
    {_sql_literal(ASSOCIATION_RECORD_KIND_PROVIDER)},
    arg_min(p.dataset_id, p.provider_record_id),
    p.dataset_type,
    p.source,
    NULL,
    p.variant_id_raw,
    arg_min(nullif(trim(p.phenotype_raw), ''), p.provider_record_id),
    p.phenotype_slug,
    coalesce(arg_min(pr.phenotype_path_json, p.provider_record_id), to_json([p.phenotype_slug])),
    coalesce(arg_min(pr.phenotype_path_key, p.provider_record_id), p.phenotype_slug),
    arg_min(nullif(trim(p.study_id_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.study_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.pmid_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.gwas_summary_path_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.study_genome_build_raw), ''), p.provider_record_id),
    min(try_cast(p.p_value_raw AS DOUBLE)),
    NULL, 'unavailable',
    NULL, NULL, 'unavailable',
    NULL, 'unavailable',
    NULL, 'unavailable',
    NULL, 'unavailable',
    NULL, 'unavailable',
    'available',
    NULL,
    '["effect_allele","effect_size","standard_error","sample_size","ancestry","fine_mapping"]',
    NULL,
    NULL,
    count(*),
    count(DISTINCT p.source_file_logical)
FROM {source_sql} p
LEFT JOIN phenotype_registry pr
  ON pr.dataset_type=p.dataset_type AND pr.phenotype_slug=p.phenotype_slug
GROUP BY p.association_record_id, p.dataset_type, p.source, p.variant_id_raw, p.phenotype_slug
"""
                )
                self.connection.execute(
                    f"""
INSERT INTO association_record_provider_records
SELECT association_record_id, provider_record_id FROM {source_sql}
"""
                )
                self.connection.execute(
                    f"""
INSERT INTO association_record_genes
SELECT association_record_id, trim(gene_id_raw),
       'source_reported_annotation_gene', count(*)
FROM {source_sql}
WHERE coalesce(trim(gene_id_raw), '') <> ''
  AND regexp_matches(trim(gene_id_raw), '^[A-Za-z][A-Za-z0-9_.-]*$')
GROUP BY association_record_id, trim(gene_id_raw)
"""
                )
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            completed.add(bucket)
            state["completed_buckets"] = sorted(completed)
            save_state()
            if index % 16 == 0 or index == len(bucket_directories):
                LOGGER.info(
                    "Provider relation progress buckets=%d/%d",
                    len(completed),
                    len(bucket_directories),
                )

    def _load_source_summary_records(self) -> None:
        """Register unavailable-provider artifacts without inventing study rows."""

        assert self.connection is not None
        files: list[tuple[str, Path]] = []
        if self.smoke_files is not None:
            requested = {"TTN", "PCSK9", "ANK2", "HMGCR", "BMPR2"}
            for dataset_type in ("CVD", "TRAIT"):
                root = self.variant_index_root / dataset_type
                for gene in sorted(requested):
                    for suffix in (".json.gz", ".json"):
                        path = root / f"{gene}{suffix}"
                        if path.exists():
                            files.append((dataset_type, path))
                            break
        else:
            for dataset_type in ("CVD", "TRAIT"):
                root = self.variant_index_root / dataset_type
                if root.exists():
                    files.extend(
                        (dataset_type, Path(entry.path))
                        for entry in sorted(os.scandir(root), key=lambda item: item.name)
                        if entry.is_file()
                        and (entry.name.endswith(".json") or entry.name.endswith(".json.gz"))
                    )

        logical_files = [
            f"variant_index/{dataset_type}/{path.name}"
            for dataset_type, path in files
        ]
        fingerprint = hashlib.sha256(_canonical_json(logical_files).encode()).hexdigest()
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + ".source-summaries.json"
        )
        state: dict[str, Any] = {}
        if state_path.exists():
            state = json.loads(state_path.read_text())
            prior_fingerprint = state.get("input_fingerprint")
            if prior_fingerprint and prior_fingerprint != fingerprint:
                raise RuntimeError("Source-summary checkpoint input mismatch")

        def save_state() -> None:
            state["input_fingerprint"] = fingerprint
            state.pop("registered_files", None)
            state.pop("completed_files", None)
            state["completed_file_count"] = len(logical_files)
            state["total_file_count"] = len(files)
            state["payload_contract_version"] = "gene-scoped-artifact-registry-v1"
            state["source_summary_engine_version"] = "artifact-registry-v1"
            state["updated_at"] = _utc_now()
            temporary = state_path.with_suffix(state_path.suffix + ".tmp")
            temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
            temporary.replace(state_path)

        missing_fields = _canonical_json(SOURCE_DETAIL_UNAVAILABLE_FIELDS)
        reason = (
            "Provider-level MVP input is unavailable on AWS; the retained compact "
            "variant-index file is a source-summary artifact, not a study record."
        )
        rows = []
        for dataset_type, path in files:
            logical = f"variant_index/{dataset_type}/{path.name}"
            gene = path.name.removesuffix(".gz").removesuffix(".json")
            stat = path.stat()
            rows.append(
                (
                    "source-summary-artifact:"
                    + hashlib.sha256(
                        _canonical_json(
                            ["million_veteran_program", dataset_type, gene, logical]
                        ).encode()
                    ).hexdigest(),
                    "million_veteran_program",
                    dataset_type,
                    gene,
                    logical,
                    "json.gz" if path.name.endswith(".json.gz") else "json",
                    stat.st_size,
                    stat.st_mtime_ns,
                    PROVIDER_DETAIL_UNAVAILABLE,
                    reason,
                    missing_fields,
                    "retained_compact_variant_index_v1",
                    "gene_scoped_on_demand",
                )
            )

        self.connection.execute("BEGIN")
        try:
            # Remove rows left by the superseded per-entry materialization attempt.
            self.connection.execute(
                "CREATE OR REPLACE TABLE association_records AS "
                "SELECT * FROM association_records WHERE record_kind<>'source_summary'"
            )
            self.connection.execute(
                "CREATE OR REPLACE TABLE association_record_genes AS "
                "SELECT g.* FROM association_record_genes g "
                "JOIN association_records a USING (association_record_id)"
            )
            self.connection.execute("DELETE FROM source_summary_artifacts")
            if rows:
                self.connection.executemany(
                    "INSERT INTO source_summary_artifacts VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
            self.connection.execute("COMMIT")
        except Exception:
            try:
                self.connection.execute("ROLLBACK")
            except Exception:
                pass
            raise

        state["superseded_per_entry_rows_removed"] = True
        state["registered_artifact_count"] = len(rows)
        save_state()
        LOGGER.info("MVP source-summary artifact registry complete files=%d", len(rows))

    def _provider_variant_partitions(self) -> list[Path]:
        """Materialize one reusable provider projection partitioned by variant ID."""

        assert self.connection is not None
        root = (
            self.temp_directory or self.output_db.parent / "duckdb_tmp"
        ) / "provider_variant_partitions"
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + ".provider-variants.json"
        )
        provider_count = int(
            self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0]
        )
        if state_path.exists():
            state = json.loads(state_path.read_text())
            if (
                int(state.get("provider_record_count", -1)) == provider_count
                and state.get("partitions_complete")
                and root.exists()
            ):
                return sorted(path for path in root.iterdir() if path.is_dir())
            raise RuntimeError("Provider-variant partition checkpoint is inconsistent")
        if root.exists():
            stale = root.with_name(
                root.name + ".stale-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
            root.rename(stale)
        root.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info(
            "Provider variant partitioning start rows=%d root=%s", provider_count, root
        )
        self.connection.execute(
            f"""
COPY (
    SELECT provider_record_id, association_record_id, variant_id_raw, allele_string_raw,
           dbsnp_build_raw, chromosome_raw, hg19_start_raw, hg19_end_raw,
           source_variation_type_raw, gene_id_raw, consequence_raw, putative_impact_raw,
           feature_id_raw, ensembl_transcript_id_raw, ensembl_protein_id_raw, hgvs_p_raw,
           protein_length_raw, study_genome_build_raw, clinical_significance_raw,
           frequency_total_raw, frequency_afr_raw, frequency_amr_raw, frequency_asj_raw,
           frequency_eas_raw, frequency_fin_raw, frequency_nfe_raw, frequency_oth_raw,
           substr(sha256(variant_id_raw), 1, 2) AS variant_bucket
    FROM provider_records
    WHERE coalesce(trim(variant_id_raw), '') <> ''
) TO {_sql_literal(str(root))}
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
        )
        files = sorted(root.rglob("*.parquet"))
        if not files:
            raise RuntimeError("Provider variant partitioning produced no Parquet files")
        state = {
            "provider_record_count": provider_count,
            "partitions_complete": True,
            "partition_files": len(files),
            "completed_at": _utc_now(),
        }
        temporary = state_path.with_suffix(state_path.suffix + ".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        temporary.replace(state_path)
        LOGGER.info("Provider variant partitioning complete files=%d", len(files))
        return sorted(path for path in root.iterdir() if path.is_dir())

    def _partition_phase_state(
        self, phase: str, provider_count: int
    ) -> tuple[Path, dict[str, Any]]:
        path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + f".{phase}.json"
        )
        state: dict[str, Any] = {}
        if path.exists():
            state = json.loads(path.read_text())
            if int(state.get("provider_record_count", -1)) != provider_count:
                raise RuntimeError(f"{phase} checkpoint provider count mismatch")
        return path, state

    @staticmethod
    def _save_partition_phase_state(
        path: Path, state: dict[str, Any], provider_count: int
    ) -> None:
        state["provider_record_count"] = provider_count
        state["updated_at"] = _utc_now()
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        temporary.replace(path)

    def _build_variants(self) -> None:
        assert self.connection is not None
        provider_count = int(
            self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0]
        )
        partitions = self._provider_variant_partitions()
        state_path, state = self._partition_phase_state("variants", provider_count)
        completed = set(state.get("completed_buckets") or [])
        if not completed:
            self.connection.execute("DELETE FROM variants")
        variant_sql = """
INSERT INTO variants
WITH parsed AS (
    SELECT
        variant_id_raw,
        allele_string_raw,
        dbsnp_build_raw,
        chromosome_raw,
        hg19_start_raw,
        hg19_end_raw,
        source_variation_type_raw,
        regexp_replace(
            upper(trim(allele_string_raw)),
            '[\\[\\]''"[:space:]]',
            '',
            'g'
        ) AS clean_alleles
    FROM __HBP_VARIANT_SOURCE__
    WHERE coalesce(trim(variant_id_raw), '') <> ''
),
typed AS (
    SELECT
        *,
        CASE
            WHEN regexp_full_match(clean_alleles, '^[ACGTN]+([,/|][ACGTN]+)+$')
            THEN regexp_split_to_array(clean_alleles, '[,/|]')
            ELSE NULL
        END AS normalized_alleles
    FROM parsed
),
classified AS (
    SELECT
        *,
        CASE
            WHEN normalized_alleles IS NULL THEN NULL
            WHEN list_min(list_transform(normalized_alleles, allele -> length(allele)))=1
             AND list_max(list_transform(normalized_alleles, allele -> length(allele)))=1
            THEN 'SNV'
            WHEN list_min(list_transform(normalized_alleles, allele -> length(allele)))
              <> list_max(list_transform(normalized_alleles, allele -> length(allele)))
            THEN 'INDEL'
            ELSE 'MNV'
        END AS derived_variation_type,
        coalesce(trim(dbsnp_build_raw), '') <> ''
          OR coalesce(trim(chromosome_raw), '') <> ''
          OR coalesce(trim(hg19_start_raw), '') <> ''
          OR coalesce(trim(hg19_end_raw), '') <> '' AS has_coordinate
    FROM typed
),
grouped AS (
    SELECT
        variant_id_raw AS variant_id,
        coalesce(
            to_json(
                list_sort(
                    list(
                        DISTINCT struct_pack(
                            raw := allele_string_raw,
                            normalized_unordered := list_sort(normalized_alleles)
                        )
                    ) FILTER (WHERE normalized_alleles IS NOT NULL)
                )
            ),
            '[]'
        ) AS allele_contexts,
        coalesce(
            to_json(
                list_sort(
                    list(
                        DISTINCT struct_pack(
                            dbsnp_build := dbsnp_build_raw,
                            chromosome := chromosome_raw,
                            hg19_start := hg19_start_raw,
                            hg19_end := hg19_end_raw
                        )
                    ) FILTER (WHERE has_coordinate)
                )
            ),
            '[]'
        ) AS coordinate_contexts,
        coalesce(
            to_json(
                list_sort(
                    list(DISTINCT source_variation_type_raw)
                    FILTER (
                        WHERE coalesce(trim(source_variation_type_raw), '') NOT IN ('', 'NA')
                    )
                )
            ),
            '[]'
        ) AS source_variation_types,
        count(DISTINCT derived_variation_type)
            FILTER (WHERE derived_variation_type IS NOT NULL) AS derived_type_count,
        min(derived_variation_type)
            FILTER (WHERE derived_variation_type IS NOT NULL) AS only_derived_type,
        coalesce(
            bool_or(len(normalized_alleles) > 2)
                FILTER (WHERE normalized_alleles IS NOT NULL),
            false
        ) AS multiallelic_observed,
        count(
            DISTINCT concat_ws(
                chr(31),
                coalesce(dbsnp_build_raw, ''),
                coalesce(chromosome_raw, ''),
                coalesce(hg19_start_raw, ''),
                coalesce(hg19_end_raw, '')
            )
        ) FILTER (WHERE has_coordinate) > 1 AS build_or_position_conflict
    FROM classified
    GROUP BY variant_id_raw
)
SELECT
    variant_id,
    allele_contexts,
    coordinate_contexts,
    source_variation_types,
    CASE WHEN derived_type_count=1 THEN only_derived_type ELSE NULL END,
    CASE WHEN derived_type_count=1 THEN 'derived' ELSE 'unresolved' END,
    'normalized_unordered_source_allele_lengths',
    CASE
        WHEN derived_type_count=1
        THEN 'All parseable source allele contexts agree. REF/ALT roles remain unresolved.'
        WHEN derived_type_count>1
        THEN 'Source allele contexts imply conflicting variation classes.'
        ELSE 'No source allele context supports an unambiguous sequence-length class.'
    END,
    'unresolved',
    'unresolved',
    'unresolved',
    multiallelic_observed,
    build_or_position_conflict
FROM grouped
"""
        for index, directory in enumerate(partitions, start=1):
            bucket = directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            source_sql = "read_parquet(" + _sql_literal(str(directory / "*.parquet")) + ")"
            self.connection.execute("BEGIN")
            try:
                # Uncheckpointed buckets contain no committed rows.
                self.connection.execute(
                    variant_sql.replace("__HBP_VARIANT_SOURCE__", source_sql)
                )
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            completed.add(bucket)
            state["completed_buckets"] = sorted(completed)
            self._save_partition_phase_state(state_path, state, provider_count)
            if index % 16 == 0 or index == len(partitions):
                LOGGER.info(
                    "Variant derivation progress buckets=%d/%d variants=%d",
                    len(completed), len(partitions),
                    int(self.connection.execute("SELECT count(*) FROM variants").fetchone()[0]),
                )

        LOGGER.info(
            "Variant derivation complete total_variants=%d",
            int(self.connection.execute("SELECT count(*) FROM variants").fetchone()[0]),
        )

    def _build_consequences(self) -> None:
        assert self.connection is not None
        provider_count = int(self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0])
        partitions = self._provider_variant_partitions()
        state_path, state = self._partition_phase_state("consequences", provider_count)
        completed = set(state.get("completed_buckets") or [])
        if not completed:
            self.connection.execute("DELETE FROM consequence_annotation_provider_records")
            self.connection.execute("DELETE FROM consequence_annotations")
        annotation_sql = """
INSERT INTO consequence_annotations
SELECT
    'consequence:' || sha256(concat_ws(chr(31), p.variant_id_raw, coalesce(trim(p.gene_id_raw), ''),
        trim(p.consequence_raw), coalesce(trim(p.putative_impact_raw), ''),
        coalesce(trim(p.feature_id_raw), ''), coalesce(trim(p.ensembl_transcript_id_raw), ''),
        coalesce(trim(p.ensembl_protein_id_raw), ''), coalesce(trim(p.hgvs_p_raw), ''),
        coalesce(trim(p.protein_length_raw), ''), coalesce(trim(p.study_genome_build_raw), ''))) AS consequence_annotation_id,
    p.variant_id_raw,
    nullif(coalesce(trim(p.gene_id_raw), ''), ''),
    trim(p.consequence_raw),
    arg_min(nullif(trim(p.putative_impact_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.feature_id_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.ensembl_transcript_id_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.ensembl_protein_id_raw), ''), p.provider_record_id),
    arg_min(nullif(trim(p.hgvs_p_raw), ''), p.provider_record_id),
    arg_min(try_cast(p.protein_length_raw AS DOUBLE), p.provider_record_id),
    'legacy SnpEff-derived source fields',
    NULL,
    arg_min(nullif(trim(p.study_genome_build_raw), ''), p.provider_record_id),
    'available',
    'not_performed',
    count(*)
FROM __HBP_VARIANT_SOURCE__ p
WHERE coalesce(trim(p.variant_id_raw), '') <> ''
  AND coalesce(trim(p.consequence_raw), '') NOT IN ('', 'NA')
GROUP BY p.variant_id_raw, coalesce(trim(p.gene_id_raw), ''), trim(p.consequence_raw),
         coalesce(trim(p.putative_impact_raw), ''), coalesce(trim(p.feature_id_raw), ''),
         coalesce(trim(p.ensembl_transcript_id_raw), ''), coalesce(trim(p.ensembl_protein_id_raw), ''),
         coalesce(trim(p.hgvs_p_raw), ''), coalesce(trim(p.protein_length_raw), ''),
         coalesce(trim(p.study_genome_build_raw), '')
"""
        link_sql = """
INSERT INTO consequence_annotation_provider_records
SELECT
    'consequence:' || sha256(concat_ws(chr(31), p.variant_id_raw, coalesce(trim(p.gene_id_raw), ''),
        trim(p.consequence_raw), coalesce(trim(p.putative_impact_raw), ''),
        coalesce(trim(p.feature_id_raw), ''), coalesce(trim(p.ensembl_transcript_id_raw), ''),
        coalesce(trim(p.ensembl_protein_id_raw), ''), coalesce(trim(p.hgvs_p_raw), ''),
        coalesce(trim(p.protein_length_raw), ''), coalesce(trim(p.study_genome_build_raw), ''))),
    p.provider_record_id,
    p.association_record_id
FROM __HBP_VARIANT_SOURCE__ p
WHERE coalesce(trim(p.variant_id_raw), '') <> ''
  AND coalesce(trim(p.consequence_raw), '') NOT IN ('', 'NA')
"""
        for index, directory in enumerate(partitions, start=1):
            bucket = directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            source_sql = "read_parquet(" + _sql_literal(str(directory / "*.parquet")) + ")"
            self.connection.execute("BEGIN")
            try:
                # Uncheckpointed buckets contain no committed rows.
                self.connection.execute(annotation_sql.replace("__HBP_VARIANT_SOURCE__", source_sql))
                self.connection.execute(link_sql.replace("__HBP_VARIANT_SOURCE__", source_sql))
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            completed.add(bucket)
            state["completed_buckets"] = sorted(completed)
            self._save_partition_phase_state(state_path, state, provider_count)
            if index % 16 == 0 or index == len(partitions):
                LOGGER.info(
                    "Consequence publication progress buckets=%d/%d annotations=%d",
                    len(completed), len(partitions),
                    int(self.connection.execute("SELECT count(*) FROM consequence_annotations").fetchone()[0]),
                )

    def _build_clinical_assertions(self) -> None:
        assert self.connection is not None
        provider_count = int(self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0])
        partitions = self._provider_variant_partitions()
        state_path, state = self._partition_phase_state("clinical-assertions", provider_count)
        completed = set(state.get("completed_buckets") or [])
        if not completed:
            self.connection.execute("DELETE FROM clinical_assertion_provider_records")
            self.connection.execute("DELETE FROM clinical_assertions")
        dimension_sql = """
INSERT INTO clinical_assertions
SELECT
    'clinical:' || sha256(concat_ws(chr(31), p.variant_id_raw,
        t.raw_source_value, t.clinical_significance)),
    p.variant_id_raw,
    t.clinical_significance,
    t.raw_source_value,
    'ClinVar-derived legacy enrichment field',
    NULL,
    NULL,
    'unavailable',
    'available',
    count(*)
FROM __HBP_VARIANT_SOURCE__ p
JOIN __clinical_terms t
  ON t.raw_source_value=trim(p.clinical_significance_raw)
WHERE coalesce(trim(p.variant_id_raw), '') <> ''
GROUP BY p.variant_id_raw, t.raw_source_value, t.clinical_significance
"""
        link_sql = """
INSERT INTO clinical_assertion_provider_records
SELECT
    'clinical:' || sha256(concat_ws(chr(31), p.variant_id_raw,
        t.raw_source_value, t.clinical_significance)),
    p.provider_record_id,
    p.association_record_id
FROM __HBP_VARIANT_SOURCE__ p
JOIN __clinical_terms t
  ON t.raw_source_value=trim(p.clinical_significance_raw)
WHERE coalesce(trim(p.variant_id_raw), '') <> ''
"""
        for index, directory in enumerate(partitions, start=1):
            bucket = directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            source_sql = "read_parquet(" + _sql_literal(str(directory / "*.parquet")) + ")"
            raw_values = self.connection.execute(
                f"""SELECT DISTINCT trim(clinical_significance_raw) FROM {source_sql}
                       WHERE coalesce(trim(clinical_significance_raw), '') NOT IN ('', 'NA', '[]')"""
            ).fetchall()
            term_rows = [
                (raw_value, term)
                for (raw_value,) in raw_values
                for term in _clinical_assertion_terms(raw_value)
            ]
            self.connection.execute(
                "CREATE OR REPLACE TEMP TABLE __clinical_terms("
                "raw_source_value VARCHAR, clinical_significance VARCHAR)"
            )
            if term_rows:
                self.connection.executemany("INSERT INTO __clinical_terms VALUES (?, ?)", term_rows)
            self.connection.execute("BEGIN")
            try:
                # Uncheckpointed buckets contain no committed rows.
                if term_rows:
                    self.connection.execute(dimension_sql.replace("__HBP_VARIANT_SOURCE__", source_sql))
                    self.connection.execute(link_sql.replace("__HBP_VARIANT_SOURCE__", source_sql))
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            completed.add(bucket)
            state["completed_buckets"] = sorted(completed)
            self._save_partition_phase_state(state_path, state, provider_count)
            if index % 16 == 0 or index == len(partitions):
                LOGGER.info(
                    "Clinical assertion progress buckets=%d/%d assertions=%d",
                    len(completed), len(partitions),
                    int(self.connection.execute("SELECT count(*) FROM clinical_assertions").fetchone()[0]),
                )

    def _build_population_observations(self) -> None:
        assert self.connection is not None
        provider_count = int(
            self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0]
        )
        partitions = self._provider_variant_partitions()
        state_path, state = self._partition_phase_state(
            "population-observations", provider_count
        )
        completed = set(state.get("completed_buckets") or [])
        if not completed:
            self.connection.execute("DELETE FROM population_observation_provider_records")
            self.connection.execute("DELETE FROM population_observations")
        population_aliases = [
            POPULATION_ALIASES[raw_name] for raw_name, _ in POPULATION_FIELDS
        ]
        population_labels = " ".join(
            f"WHEN {_sql_literal(POPULATION_ALIASES[raw_name])} "
            f"THEN {_sql_literal(population)}"
            for raw_name, population in POPULATION_FIELDS
        )
        identity = "sha256(concat_ws(chr(31), variant_id_raw, population, cast(reported_frequency AS VARCHAR), coalesce(allele_string_raw, ''), coalesce(study_genome_build_raw, '')))"
        observation_sql = f"""
INSERT INTO population_observations
SELECT
    'population:' || {identity},
    variant_id_raw,
    'gnomAD genome legacy enrichment',
    NULL,
    population,
    reported_frequency,
    to_json(struct_pack(raw_allele_string := allele_string_raw, ref_alt_status := 'unresolved')),
    nullif(trim(study_genome_build_raw), ''),
    'source_row_variant_id',
    'unresolved',
    'unresolved',
    'available',
    count(*)
FROM __population_long
GROUP BY variant_id_raw, population, reported_frequency, allele_string_raw, study_genome_build_raw
"""
        link_sql = f"""
INSERT INTO population_observation_provider_records
SELECT 'population:' || {identity}, provider_record_id, association_record_id
FROM __population_long
"""
        for index, directory in enumerate(partitions, start=1):
            bucket = directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            source_sql = "read_parquet(" + _sql_literal(str(directory / "*.parquet")) + ")"
            # UNPIVOT scans each provider partition once. The previous UNION ALL
            # formulation reread it once per population column and was
            # prohibitively I/O-bound at production scale.
            long_sql = f"""
SELECT
    provider_record_id,
    association_record_id,
    variant_id_raw,
    allele_string_raw,
    study_genome_build_raw,
    CASE population_field {population_labels} END AS population,
    try_cast(reported_frequency_raw AS DOUBLE) AS reported_frequency
FROM (
    UNPIVOT (
        SELECT provider_record_id, association_record_id, variant_id_raw,
               allele_string_raw, study_genome_build_raw,
               {', '.join(population_aliases)}
        FROM {source_sql}
    )
    ON {', '.join(population_aliases)}
    INTO NAME population_field VALUE reported_frequency_raw
)
WHERE try_cast(reported_frequency_raw AS DOUBLE) BETWEEN 0 AND 1
"""
            self.connection.execute(
                "CREATE OR REPLACE TEMP TABLE __population_long AS " + long_sql
            )
            self.connection.execute("BEGIN")
            try:
                # Uncheckpointed buckets contain no committed rows.
                self.connection.execute(observation_sql)
                self.connection.execute(link_sql)
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            completed.add(bucket)
            state["completed_buckets"] = sorted(completed)
            self._save_partition_phase_state(state_path, state, provider_count)
            if index % 16 == 0 or index == len(partitions):
                LOGGER.info(
                    "Population observation progress buckets=%d/%d observations=%d",
                    len(completed),
                    len(partitions),
                    int(
                        self.connection.execute(
                            "SELECT count(*) FROM population_observations"
                        ).fetchone()[0]
                    ),
                )

    def _published_variant_partitions(self, table: str) -> dict[str, Path]:
        """Partition one derived table once for bounded summary joins."""

        assert self.connection is not None
        allowed = {
            "consequence_annotations",
            "clinical_assertions",
            "population_observations",
        }
        if table not in allowed:
            raise ValueError(f"Unsupported published variant table: {table}")
        row_count = int(
            self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        )
        root = (
            self.temp_directory or self.output_db.parent / "duckdb_tmp"
        ) / "published_variant_partitions" / table
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + f".published-{table}.json"
        )
        if state_path.exists():
            state = json.loads(state_path.read_text())
            directories = {
                path.name.split("=", 1)[-1]: path
                for path in root.iterdir()
                if path.is_dir()
            } if root.exists() else {}
            if (
                int(state.get("row_count", -1)) == row_count
                and state.get("partitions_complete")
                and (row_count == 0 or directories)
            ):
                return directories

        if root.exists():
            stale = root.with_name(
                root.name
                + ".stale-"
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
            root.rename(stale)
        root.parent.mkdir(parents=True, exist_ok=True)
        if row_count:
            LOGGER.info(
                "Published variant partitioning start table=%s rows=%d",
                table,
                row_count,
            )
            self.connection.execute(
                f"""
COPY (
    SELECT *, substr(sha256(variant_id), 1, 2) AS variant_bucket
    FROM {table}
    WHERE coalesce(trim(variant_id), '') <> ''
) TO {_sql_literal(str(root))}
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
            )
        directories = {
            path.name.split("=", 1)[-1]: path
            for path in root.iterdir()
            if path.is_dir()
        } if root.exists() else {}
        state = {
            "table": table,
            "row_count": row_count,
            "partitions_complete": True,
            "partition_files": len(list(root.rglob("*.parquet"))) if root.exists() else 0,
            "completed_at": _utc_now(),
        }
        temporary = state_path.with_suffix(state_path.suffix + ".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        temporary.replace(state_path)
        LOGGER.info(
            "Published variant partitioning complete table=%s buckets=%d",
            table,
            len(directories),
        )
        return directories

    def _summary_base_partitions(self) -> list[Path]:
        """Materialize a resumable variant-bucketed summary input projection."""

        assert self.connection is not None
        counts = {
            "association_records": int(
                self.connection.execute("SELECT count(*) FROM association_records").fetchone()[0]
            ),
            "association_record_genes": int(
                self.connection.execute("SELECT count(*) FROM association_record_genes").fetchone()[0]
            ),
            "variants": int(
                self.connection.execute("SELECT count(*) FROM variants").fetchone()[0]
            ),
        }
        root = (
            self.temp_directory or self.output_db.parent / "duckdb_tmp"
        ) / "summary_base_partitions"
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + ".summary-base.json"
        )
        if state_path.exists():
            state = json.loads(state_path.read_text())
            directories = sorted(path for path in root.iterdir() if path.is_dir()) if root.exists() else []
            if (
                state.get("input_counts") == counts
                and state.get("partitions_complete")
                and directories
            ):
                return directories

        if root.exists():
            stale = root.with_name(
                root.name
                + ".stale-"
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
            root.rename(stale)
        root.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Summary base partitioning start inputs=%s", counts)
        self.connection.execute(
            f"""
COPY (
    SELECT
        'summary:' || sha256(concat_ws(chr(31), ar.dataset_type, arg.gene_id, ar.variant_id, ar.phenotype_path_key)) AS summary_id,
        ar.dataset_type,
        arg.gene_id,
        ar.variant_id,
        ar.phenotype_slug,
        ar.phenotype_path_json,
        ar.phenotype_path_key,
        ar.association_record_id,
        ar.source,
        ar.provider_detail_status,
        ar.provider_record_count,
        ar.study_id,
        ar.study_title,
        ar.reported_p_value,
        v.variation_type,
        coalesce(v.variation_type_status, 'unresolved') AS variation_type_status,
        substr(sha256(ar.variant_id), 1, 2) AS variant_bucket
    FROM association_records ar
    JOIN association_record_genes arg USING (association_record_id)
    LEFT JOIN variants v USING (variant_id)
) TO {_sql_literal(str(root))}
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
        )
        directories = sorted(path for path in root.iterdir() if path.is_dir())
        if not directories:
            raise RuntimeError("Summary base partitioning produced no Parquet files")
        state = {
            "input_counts": counts,
            "partitions_complete": True,
            "partition_files": len(list(root.rglob("*.parquet"))),
            "completed_at": _utc_now(),
        }
        temporary = state_path.with_suffix(state_path.suffix + ".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        temporary.replace(state_path)
        LOGGER.info("Summary base partitioning complete buckets=%d", len(directories))
        return directories

    def _build_summaries(self) -> None:
        assert self.connection is not None
        provider_count = int(
            self.connection.execute("SELECT count(*) FROM provider_records").fetchone()[0]
        )
        summary_partitions = self._summary_base_partitions()
        consequence_partitions = self._published_variant_partitions(
            "consequence_annotations"
        )
        clinical_partitions = self._published_variant_partitions(
            "clinical_assertions"
        )
        population_partitions = self._published_variant_partitions(
            "population_observations"
        )
        state_path, state = self._partition_phase_state("summaries", provider_count)
        completed = set(state.get("completed_buckets") or [])
        if not completed:
            for table in (
                "summary_association_records",
                "summary_consequence_annotations",
                "summary_clinical_assertions",
                "variant_phenotype_summaries",
            ):
                self.connection.execute(f"DELETE FROM {table}")

        summary_sql = """
INSERT INTO variant_phenotype_summaries
WITH grouped AS (
    SELECT
        summary_id,
        first(dataset_type) AS dataset_type,
        first(gene_id) AS gene_id,
        first(variant_id) AS variant_id,
        first(phenotype_slug) AS phenotype_slug,
        first(phenotype_path_json) AS phenotype_path_json,
        first(phenotype_path_key) AS phenotype_path_key,
        sum(provider_record_count) AS source_observation_count,
        count(DISTINCT association_record_id) AS association_record_count,
        count(DISTINCT CASE
            WHEN provider_detail_status='available' THEN association_record_id
        END) AS provider_association_record_count,
        count(DISTINCT CASE
            WHEN provider_detail_status='unavailable' THEN association_record_id
        END) AS retained_source_summary_count,
        count(DISTINCT CASE
            WHEN provider_detail_status='available'
             AND coalesce(study_id, study_title) IS NOT NULL
            THEN coalesce(study_id, study_title)
        END) AS study_count,
        min(reported_p_value) AS minimum_p,
        first(variation_type) AS variation_type,
        first(variation_type_status) AS variation_type_status,
        to_json(
            list_sort(
                list(
                    DISTINCT struct_pack(
                        source := source,
                        provider_detail_status := provider_detail_status
                    )
                )
            )
        ) AS source_completeness
    FROM __summary_base
    GROUP BY summary_id
),
annotation_counts AS (
    SELECT
        sb.summary_id,
        count(DISTINCT ca.consequence_annotation_id) AS consequence_count,
        count(DISTINCT ca.consequence) > 1 AS consequence_conflict
    FROM __summary_base sb
    JOIN __bucket_consequences ca
      ON ca.variant_id=sb.variant_id AND ca.gene_id=sb.gene_id
    GROUP BY sb.summary_id
),
clinical_counts AS (
    SELECT
        sb.summary_id,
        count(DISTINCT ca.clinical_assertion_id) AS clinical_count,
        count(DISTINCT ca.clinical_significance) > 1 AS clinical_conflict
    FROM __summary_base sb
    JOIN __bucket_clinical ca ON ca.variant_id=sb.variant_id
    GROUP BY sb.summary_id
),
population_counts AS (
    SELECT variant_id, count(*) AS population_count
    FROM __bucket_population
    GROUP BY variant_id
)
SELECT
    g.summary_id,
    g.dataset_type,
    g.gene_id,
    g.variant_id,
    g.phenotype_slug,
    g.phenotype_path_json,
    g.phenotype_path_key,
    g.source_observation_count,
    g.association_record_count,
    g.provider_association_record_count,
    g.retained_source_summary_count,
    g.study_count,
    coalesce(pc.population_count, 0),
    coalesce(ac.consequence_count, 0),
    coalesce(cc.clinical_count, 0),
    g.minimum_p,
    (
        SELECT min(supplier.association_record_id)
        FROM __summary_base supplier
        WHERE supplier.summary_id=g.summary_id
          AND supplier.reported_p_value=g.minimum_p
    ),
    coalesce(
        (
            SELECT to_json(list(association_record_id ORDER BY association_record_id))
            FROM __summary_base tied
            WHERE tied.summary_id=g.summary_id
              AND tied.reported_p_value=g.minimum_p
        ),
        '[]'
    ),
    NULL,
    'unavailable',
    NULL,
    'unavailable',
    NULL,
    'unavailable',
    coalesce(ac.consequence_conflict, false),
    coalesce(cc.clinical_conflict, false),
    g.variation_type,
    g.variation_type_status,
    g.source_completeness
FROM grouped g
LEFT JOIN annotation_counts ac ON ac.summary_id=g.summary_id
LEFT JOIN clinical_counts cc ON cc.summary_id=g.summary_id
LEFT JOIN population_counts pc ON pc.variant_id=g.variant_id
"""
        for index, directory in enumerate(summary_partitions, start=1):
            bucket = directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            summary_source = "read_parquet(" + _sql_literal(str(directory / "*.parquet")) + ")"
            consequence_directory = consequence_partitions.get(bucket)
            clinical_directory = clinical_partitions.get(bucket)
            population_directory = population_partitions.get(bucket)
            consequence_source = (
                "read_parquet(" + _sql_literal(str(consequence_directory / "*.parquet")) + ")"
                if consequence_directory
                else "(SELECT * FROM consequence_annotations WHERE false)"
            )
            clinical_source = (
                "read_parquet(" + _sql_literal(str(clinical_directory / "*.parquet")) + ")"
                if clinical_directory
                else "(SELECT * FROM clinical_assertions WHERE false)"
            )
            population_source = (
                "read_parquet(" + _sql_literal(str(population_directory / "*.parquet")) + ")"
                if population_directory
                else "(SELECT * FROM population_observations WHERE false)"
            )
            self.connection.execute(
                f"CREATE OR REPLACE TEMP VIEW __summary_base AS SELECT * FROM {summary_source}"
            )
            self.connection.execute(
                f"CREATE OR REPLACE TEMP VIEW __bucket_consequences AS SELECT * FROM {consequence_source}"
            )
            self.connection.execute(
                f"CREATE OR REPLACE TEMP VIEW __bucket_clinical AS SELECT * FROM {clinical_source}"
            )
            self.connection.execute(
                f"CREATE OR REPLACE TEMP VIEW __bucket_population AS SELECT * FROM {population_source}"
            )
            self.connection.execute("BEGIN")
            try:
                # Uncheckpointed buckets contain no committed rows.
                self.connection.execute(
                    """
INSERT INTO summary_association_records
SELECT DISTINCT summary_id, association_record_id
FROM __summary_base
"""
                )
                self.connection.execute(
                    """
INSERT INTO summary_consequence_annotations
SELECT DISTINCT sb.summary_id, ca.consequence_annotation_id
FROM __summary_base sb
JOIN __bucket_consequences ca
  ON ca.variant_id=sb.variant_id AND ca.gene_id=sb.gene_id
"""
                )
                self.connection.execute(
                    """
INSERT INTO summary_clinical_assertions
SELECT DISTINCT sb.summary_id, ca.clinical_assertion_id
FROM __summary_base sb
JOIN __bucket_clinical ca ON ca.variant_id=sb.variant_id
"""
                )
                self.connection.execute(summary_sql)
                self.connection.execute("COMMIT")
            except Exception:
                try:
                    self.connection.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            completed.add(bucket)
            state["completed_buckets"] = sorted(completed)
            self._save_partition_phase_state(state_path, state, provider_count)
            if index % 16 == 0 or index == len(summary_partitions):
                LOGGER.info(
                    "Summary publication progress buckets=%d/%d summaries=%d",
                    len(completed),
                    len(summary_partitions),
                    int(
                        self.connection.execute(
                            "SELECT count(*) FROM variant_phenotype_summaries"
                        ).fetchone()[0]
                    ),
                )

    def _minimum_p_summary_partitions(self) -> dict[str, Path]:
        """Partition persisted minimum-p suppliers once for bounded QA."""

        assert self.connection is not None
        row_count = int(
            self.connection.execute(
                """
SELECT count(*)
FROM variant_phenotype_summaries
WHERE minimum_reported_p_value IS NOT NULL
"""
            ).fetchone()[0]
        )
        root = (
            self.temp_directory or self.output_db.parent / "duckdb_tmp"
        ) / "minimum_p_summary_partitions"
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + ".minimum-p-summary.json"
        )
        if state_path.exists():
            state = json.loads(state_path.read_text())
            directories = {
                path.name.split("=", 1)[-1]: path
                for path in root.iterdir()
                if path.is_dir()
            } if root.exists() else {}
            if (
                int(state.get("row_count", -1)) == row_count
                and state.get("partitions_complete")
                and (row_count == 0 or directories)
            ):
                return directories

        if root.exists():
            stale = root.with_name(
                root.name
                + ".stale-"
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
            root.rename(stale)
        root.parent.mkdir(parents=True, exist_ok=True)
        if row_count:
            LOGGER.info(
                "Minimum-p summary QA partitioning start rows=%d", row_count
            )
            self.connection.execute(
                f"""
COPY (
    SELECT
        variant_phenotype_summary_id,
        minimum_reported_p_value,
        minimum_reported_p_value_association_record_id,
        substr(sha256(variant_id), 1, 2) AS variant_bucket
    FROM variant_phenotype_summaries
    WHERE minimum_reported_p_value IS NOT NULL
) TO {_sql_literal(str(root))}
(FORMAT PARQUET, PARTITION_BY (variant_bucket), COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
"""
            )
        directories = {
            path.name.split("=", 1)[-1]: path
            for path in root.iterdir()
            if path.is_dir()
        } if root.exists() else {}
        state = {
            "row_count": row_count,
            "partitions_complete": True,
            "partition_files": len(list(root.rglob("*.parquet"))) if root.exists() else 0,
            "completed_at": _utc_now(),
        }
        temporary = state_path.with_suffix(state_path.suffix + ".tmp")
        temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        temporary.replace(state_path)
        LOGGER.info(
            "Minimum-p summary QA partitioning complete buckets=%d",
            len(directories),
        )
        return directories

    def _qa_minimum_p_supplier_is_deterministic_tied_minimum(self) -> bool:
        """Reconcile tied minimum-p suppliers one variant bucket at a time."""

        assert self.connection is not None
        base_partitions = self._summary_base_partitions()
        persisted_partitions = self._minimum_p_summary_partitions()
        state_path = self.checkpoint.path.with_name(
            self.checkpoint.path.stem + ".qa-minimum-p-supplier.json"
        )
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        completed = set(state.get("completed_buckets") or [])
        for index, directory in enumerate(base_partitions, start=1):
            bucket = directory.name.split("=", 1)[-1]
            if bucket in completed:
                continue
            persisted_directory = persisted_partitions.get(bucket)
            persisted_source = (
                "read_parquet(" + _sql_literal(str(persisted_directory / "*.parquet")) + ")"
                if persisted_directory
                else "(SELECT * FROM variant_phenotype_summaries WHERE false)"
            )
            base_source = "read_parquet(" + _sql_literal(str(directory / "*.parquet")) + ")"
            passed = bool(
                self.connection.execute(
                    f"""
WITH minima AS (
    SELECT summary_id, min(reported_p_value) AS minimum_p
    FROM {base_source}
    WHERE reported_p_value IS NOT NULL
    GROUP BY summary_id
), expected AS (
    SELECT b.summary_id, m.minimum_p,
           min(b.association_record_id) AS supplier_id
    FROM {base_source} b
    JOIN minima m
      ON m.summary_id=b.summary_id
     AND b.reported_p_value=m.minimum_p
    GROUP BY b.summary_id, m.minimum_p
), persisted AS (
    SELECT * FROM {persisted_source}
)
SELECT count(*)=0
FROM expected e
FULL OUTER JOIN persisted p
  ON p.variant_phenotype_summary_id=e.summary_id
WHERE e.summary_id IS NULL
   OR p.variant_phenotype_summary_id IS NULL
   OR p.minimum_reported_p_value IS DISTINCT FROM e.minimum_p
   OR p.minimum_reported_p_value_association_record_id IS DISTINCT FROM e.supplier_id
"""
                ).fetchone()[0]
            )
            if not passed:
                return False
            completed.add(bucket)
            state = {
                "completed_buckets": sorted(completed),
                "total_buckets": len(base_partitions),
                "updated_at": _utc_now(),
            }
            temporary = state_path.with_suffix(state_path.suffix + ".tmp")
            temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
            temporary.replace(state_path)
            if index % 16 == 0 or index == len(base_partitions):
                LOGGER.info(
                    "Minimum-p supplier QA progress buckets=%d/%d",
                    len(completed),
                    len(base_partitions),
                )
        return True

    def _run_qa(self) -> None:
        assert self.connection is not None
        checks = {
            "provider_record_ids_are_unique": "SELECT count(*)=count(DISTINCT provider_record_id) FROM provider_records",
            "provider_source_lines_are_unique": "SELECT count(*)=0 FROM (SELECT source_file_logical, source_line FROM provider_records GROUP BY ALL HAVING count(*)<>1) duplicates",
            "association_provider_links_resolve": "SELECT count(*)=0 FROM association_record_provider_records l LEFT JOIN provider_records p USING(provider_record_id) WHERE p.provider_record_id IS NULL",
            "association_links_resolve": "SELECT count(*)=0 FROM association_record_provider_records l LEFT JOIN association_records a USING(association_record_id) WHERE a.association_record_id IS NULL",
            "consequence_provider_links_resolve": "SELECT count(*)=0 FROM consequence_annotation_provider_records l LEFT JOIN provider_records p USING(provider_record_id) WHERE p.provider_record_id IS NULL",
            "clinical_provider_links_resolve": "SELECT count(*)=0 FROM clinical_assertion_provider_records l LEFT JOIN provider_records p USING(provider_record_id) WHERE p.provider_record_id IS NULL",
            "population_provider_links_resolve": "SELECT count(*)=0 FROM population_observation_provider_records l LEFT JOIN provider_records p USING(provider_record_id) WHERE p.provider_record_id IS NULL",
            "population_observation_integrity": """
                WITH observations AS (
                    SELECT population_observation_id, count(*) AS row_count,
                           min(provider_record_count) AS expected_links,
                           max(provider_record_count) AS maximum_expected_links
                    FROM population_observations
                    GROUP BY population_observation_id
                ),
                links AS (
                    SELECT population_observation_id, count(*) AS link_count
                    FROM population_observation_provider_records
                    GROUP BY population_observation_id
                )
                SELECT count(*)=0
                FROM observations o
                FULL OUTER JOIN links l USING(population_observation_id)
                WHERE coalesce(o.row_count, 0)<>1
                   OR o.expected_links<>o.maximum_expected_links
                   OR coalesce(l.link_count, 0)<>coalesce(o.expected_links, 0)
            """,
            "summary_minimum_p_reconciles": "SELECT count(*)=0 FROM variant_phenotype_summaries s LEFT JOIN association_records a ON a.association_record_id=s.minimum_reported_p_value_association_record_id WHERE s.minimum_reported_p_value IS NOT NULL AND (a.reported_p_value IS NULL OR a.reported_p_value<>s.minimum_reported_p_value)",
            "summary_associations_resolve": "SELECT count(*)=0 FROM summary_association_records l LEFT JOIN association_records a USING(association_record_id) WHERE a.association_record_id IS NULL",
            "summary_ids_are_unique": "SELECT count(*)=count(DISTINCT variant_phenotype_summary_id) FROM variant_phenotype_summaries",
            "summary_association_links_are_unique": "SELECT count(*)=count(DISTINCT variant_phenotype_summary_id || chr(31) || association_record_id) FROM summary_association_records",
            "summary_consequence_links_are_unique": "SELECT count(*)=count(DISTINCT variant_phenotype_summary_id || chr(31) || consequence_annotation_id) FROM summary_consequence_annotations",
            "summary_clinical_links_are_unique": "SELECT count(*)=count(DISTINCT variant_phenotype_summary_id || chr(31) || clinical_assertion_id) FROM summary_clinical_assertions",
            "unavailable_sources_have_no_provider_links": "SELECT count(*)=0 FROM association_records a JOIN association_record_provider_records l USING(association_record_id) WHERE a.provider_detail_status='unavailable'",
            "all_association_variants_resolve": "SELECT count(*)=0 FROM association_records a LEFT JOIN variants v ON v.variant_id=a.variant_id WHERE v.variant_id IS NULL",
            "source_summary_paths_are_logical": "SELECT count(*)=0 FROM source_summary_artifacts WHERE logical_artifact LIKE '/%'",
            "source_summary_registry_is_unique": "SELECT count(*)=count(DISTINCT source || chr(31) || dataset_type || chr(31) || gene_id || chr(31) || logical_artifact) FROM source_summary_artifacts",
            "source_summary_registry_is_unavailable": "SELECT count(*)=0 FROM source_summary_artifacts WHERE provider_detail_status<>'unavailable' OR publication_mode<>'gene_scoped_on_demand'",
            "no_unavailable_provider_rows_reconstructed": "SELECT count(*)=0 FROM association_records WHERE provider_detail_status='unavailable' OR record_kind='source_summary'",
            "clinical_assertion_links_resolve": "SELECT count(*)=0 FROM clinical_assertion_provider_records l LEFT JOIN clinical_assertions c USING(clinical_assertion_id) WHERE c.clinical_assertion_id IS NULL",
            "available_associations_have_provider_links": "SELECT count(*)=0 FROM association_records a WHERE a.provider_detail_status='available' AND NOT EXISTS (SELECT 1 FROM association_record_provider_records l WHERE l.association_record_id=a.association_record_id)",
            "consequence_link_counts_reconcile": """
                WITH links AS (
                    SELECT consequence_annotation_id, count(*) AS link_count
                    FROM consequence_annotation_provider_records
                    GROUP BY consequence_annotation_id
                )
                SELECT count(*)=0
                FROM consequence_annotations c
                LEFT JOIN links l USING(consequence_annotation_id)
                WHERE c.provider_record_count<>coalesce(l.link_count, 0)
            """,
            "clinical_link_counts_reconcile": """
                WITH links AS (
                    SELECT clinical_assertion_id, count(*) AS link_count
                    FROM clinical_assertion_provider_records
                    GROUP BY clinical_assertion_id
                )
                SELECT count(*)=0
                FROM clinical_assertions c
                LEFT JOIN links l USING(clinical_assertion_id)
                WHERE c.provider_record_count<>coalesce(l.link_count, 0)
            """,
            "minimum_p_supplier_is_deterministic_tied_minimum": None,
        }
        completed = {
            str(row[0])
            for row in self.connection.execute(
                "SELECT check_name FROM qa_results WHERE status='passed'"
            ).fetchall()
        }
        for name, query in checks.items():
            if name in completed:
                LOGGER.info("Evidence v2 QA check already passed: %s", name)
                continue
            started = time.perf_counter()
            LOGGER.info("Evidence v2 QA check start: %s", name)
            if name == "minimum_p_supplier_is_deterministic_tied_minimum":
                passed = (
                    self._qa_minimum_p_supplier_is_deterministic_tied_minimum()
                )
            else:
                assert query is not None
                passed = bool(self.connection.execute(query).fetchone()[0])
            row = (
                name,
                "passed" if passed else "failed",
                str(passed),
                "true",
                "{}",
                datetime.now(timezone.utc),
            )
            self.connection.execute(
                "DELETE FROM qa_results WHERE check_name=?", [name]
            )
            self.connection.execute(
                "INSERT INTO qa_results VALUES (?, ?, ?, ?, ?, ?)", row
            )
            LOGGER.info(
                "Evidence v2 QA check complete: %s passed=%s elapsed=%.1fs",
                name,
                passed,
                time.perf_counter() - started,
            )
            if not passed:
                raise RuntimeError(f"Evidence v2 QA failed: {name}")

    def _counts(self) -> dict[str, int]:
        assert self.connection is not None
        tables = (
            "provider_records",
            "source_summary_artifacts",
            "variants",
            "association_records",
            "association_record_genes",
            "consequence_annotations",
            "clinical_assertions",
            "population_observations",
            "variant_phenotype_summaries",
        )
        return {
            table: int(self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in tables
        }
