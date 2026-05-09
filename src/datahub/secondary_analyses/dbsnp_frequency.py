"""Build a queryable dbSNP population-frequency index.

The NCBI dbSNP frequency downloads are distributed here as large tarballs of
per-rsID TSV files. This module streams those tarballs into DuckDB so the raw
evidence can stay complete without extracting millions of small files.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
import tarfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Sequence

import duckdb


LOGGER = logging.getLogger(__name__)

FREQUENCY_MEMBER_SUFFIX = "_frequency.csv"
DEFAULT_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class FrequencyRecord:
    rsid: str
    study: str | None
    population: str | None
    population_group: str | None
    sample_size: int | None
    ref_allele: str | None
    ref_frequency: float | None
    alt_allele: str | None
    alt_frequency: float | None
    ref_allele_raw: str | None
    alt_allele_raw: str | None
    bioproject_id: str | None
    biosample_id: str | None
    source_system: str
    source_archive: str | None
    source_member: str | None
    source_url: str | None
    ncbi_build: str | None
    released: str | None
    organism: str | None
    position: str | None
    variation_type: str | None


@dataclass(frozen=True)
class SourceSummary:
    source_archive: str
    source_system: str
    members_seen: int
    frequency_members: int
    rows_loaded: int
    distinct_rsids: int


@dataclass(frozen=True)
class BuildSummary:
    output_db: str
    raw_root: str
    legacy_dbsnp_root: str | None
    checkpoint_path: str | None
    sources: list[SourceSummary]
    rows_loaded: int
    distinct_rsids: int
    built_at: str


@dataclass(frozen=True)
class ParquetExportSummary:
    output_root: str
    records_root: str
    source_archive: str
    source_system: str
    parquet_files: list[str]
    members_seen: int
    frequency_members: int
    rows_loaded: int
    distinct_rsids: int
    built_at: str


def discover_frequency_archives(raw_root: Path) -> list[Path]:
    """Return dbSNP frequency tarballs in deterministic order."""

    return sorted(path for path in raw_root.glob("*.tar.gz") if path.is_file())


def parse_frequency_member(
    *,
    text: str,
    source_archive: str | None,
    source_member: str | None,
    source_system: str = "ncbi_dbsnp_frequency",
) -> list[FrequencyRecord]:
    """Parse one dbSNP frequency TSV member into normalized records."""

    lines = text.splitlines()
    metadata: dict[str, str] = {}
    header: list[str] | None = None
    data_lines: list[str] = []

    for raw_line in lines:
        line = raw_line.strip("\ufeff")
        if not line:
            continue
        if line.startswith("#"):
            comment = line[1:]
            if not comment or set(comment) == {"#"}:
                continue
            parts = comment.split("\t")
            if parts and parts[0] == "Study":
                header = parts
                continue
            if len(parts) >= 2:
                metadata[parts[0].strip()] = "\t".join(parts[1:]).strip() or None
            continue
        data_lines.append(line)

    rsid = _metadata_rsid(metadata) or _rsid_from_path(source_member)
    if not rsid or not header:
        return []

    reader = csv.DictReader(data_lines, delimiter="\t", fieldnames=header)
    records: list[FrequencyRecord] = []
    for row in reader:
        ref_raw = _clean(row.get("Ref Allele"))
        alt_raw = _clean(row.get("Alt Allele"))
        ref_allele, ref_frequency = _parse_allele_frequency(ref_raw)
        alt_allele, alt_frequency = _parse_allele_frequency(alt_raw)
        records.append(
            FrequencyRecord(
                rsid=rsid,
                study=_clean(row.get("Study")),
                population=_clean(row.get("Population")),
                population_group=_clean(row.get("Group")),
                sample_size=_parse_int(row.get("Samplesize") or row.get("Sample Size")),
                ref_allele=ref_allele,
                ref_frequency=ref_frequency,
                alt_allele=alt_allele,
                alt_frequency=alt_frequency,
                ref_allele_raw=ref_raw,
                alt_allele_raw=alt_raw,
                bioproject_id=_clean(row.get("BioProject ID")),
                biosample_id=_clean(row.get("BioSample ID")),
                source_system=source_system,
                source_archive=source_archive,
                source_member=source_member,
                source_url=_clean(metadata.get("URL")),
                ncbi_build=_clean(metadata.get("Current Build")),
                released=_clean(metadata.get("Released")),
                organism=_clean(metadata.get("Organism")),
                position=_clean(metadata.get("Position")),
                variation_type=_clean(metadata.get("Variation Type")),
            )
        )
    return records


def iter_archive_records(
    archive_path: Path,
    *,
    limit_members: int | None = None,
) -> Iterator[tuple[FrequencyRecord, int, bool]]:
    """Yield records from a tar.gz archive.

    The yielded tuple is ``(record, members_seen, member_loaded)`` so callers can
    track progress without reading the archive twice.
    """

    members_seen = 0
    frequency_members = 0
    with tarfile.open(archive_path, mode="r|gz") as tar:
        for member in tar:
            members_seen += 1
            if limit_members is not None and frequency_members >= limit_members:
                break
            if not member.isfile() or not member.name.endswith(FREQUENCY_MEMBER_SUFFIX):
                continue
            handle = tar.extractfile(member)
            if handle is None:
                continue
            frequency_members += 1
            text = handle.read().decode("utf-8", errors="replace")
            records = parse_frequency_member(
                text=text,
                source_archive=archive_path.name,
                source_member=member.name,
            )
            for record in records:
                yield record, members_seen, True


def iter_legacy_records(legacy_dbsnp_root: Path) -> Iterator[FrequencyRecord]:
    """Yield existing HBP dbSNP CSV rows as separate legacy-provenance records."""

    csv_roots = [legacy_dbsnp_root / "csvs", legacy_dbsnp_root / "overall"]
    seen: set[tuple[str | None, ...]] = set()
    for csv_root in csv_roots:
        if not csv_root.exists():
            continue
        for path in sorted(csv_root.glob("*.csv")):
            with path.open(newline="", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    rsid = _clean(row.get("rsid")) or _rsid_from_path(path.name)
                    if not rsid:
                        continue
                    ref_raw = _clean(row.get("Ref Allele"))
                    alt_raw = _clean(row.get("Alt Allele"))
                    ref_allele, ref_frequency = _parse_allele_frequency(ref_raw)
                    alt_allele, alt_frequency = _parse_allele_frequency(alt_raw)
                    dedupe_key = (
                        rsid,
                        _clean(row.get("Study")),
                        _clean(row.get("Population")),
                        _clean(row.get("Group")),
                        str(_parse_int(row.get("Samplesize") or row.get("Sample Size"))),
                        ref_raw,
                        alt_raw,
                    )
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    yield FrequencyRecord(
                        rsid=rsid,
                        study=_clean(row.get("Study")),
                        population=_clean(row.get("Population")),
                        population_group=_clean(row.get("Group")),
                        sample_size=_parse_int(row.get("Samplesize") or row.get("Sample Size")),
                        ref_allele=ref_allele,
                        ref_frequency=ref_frequency,
                        alt_allele=alt_allele,
                        alt_frequency=alt_frequency,
                        ref_allele_raw=ref_raw,
                        alt_allele_raw=alt_raw,
                        bioproject_id=_clean(row.get("BioProject ID")),
                        biosample_id=_clean(row.get("BioSample ID")),
                        source_system="hbp_legacy_dbsnp",
                        source_archive=None,
                        source_member=str(path),
                        source_url=None,
                        ncbi_build=None,
                        released=None,
                        organism=None,
                        position=None,
                        variation_type=None,
                    )


def build_dbsnp_frequency_index(
    *,
    raw_root: Path,
    output_db: Path,
    legacy_dbsnp_root: Path | None = None,
    include_legacy: bool = False,
    limit_members: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    checkpoint_path: Path | None = None,
    resume: bool = True,
    progress: bool = True,
    progress_interval: float = 5.0,
) -> BuildSummary:
    """Stream dbSNP frequency archives into a DuckDB index."""

    raw_root = raw_root.resolve()
    output_db = output_db.resolve()
    legacy_dbsnp_root = legacy_dbsnp_root.resolve() if legacy_dbsnp_root is not None else None
    checkpoint_path = (checkpoint_path or output_db.with_suffix(".checkpoint.json")).resolve()
    output_db.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    archives = discover_frequency_archives(raw_root)
    if not archives:
        raise FileNotFoundError(f"No dbSNP frequency archives found under {raw_root}")

    output_db_existed = output_db.exists()
    connection = duckdb.connect(str(output_db))
    try:
        checkpoint = _load_checkpoint(checkpoint_path) if resume else None
        if checkpoint is None or not output_db_existed:
            checkpoint = _new_checkpoint(
                raw_root=raw_root,
                output_db=output_db,
                legacy_dbsnp_root=legacy_dbsnp_root,
                archives=archives,
                include_legacy=include_legacy,
                limit_members=limit_members,
            )
            _initialise_schema(connection)
            _write_checkpoint(checkpoint_path, checkpoint)
        else:
            _validate_checkpoint(
                checkpoint=checkpoint,
                raw_root=raw_root,
                output_db=output_db,
                legacy_dbsnp_root=legacy_dbsnp_root,
                archives=archives,
                include_legacy=include_legacy,
                limit_members=limit_members,
            )
            _cleanup_incomplete_resume_rows(connection, archives, checkpoint)

        completed_archives = set(checkpoint.get("completed_archives", []))
        reporter = _ProgressReporter(enabled=progress, interval_seconds=progress_interval)

        for archive_index, archive_path in enumerate(archives, start=1):
            if archive_path.name in completed_archives:
                LOGGER.info("Skipping completed dbSNP frequency archive: %s", archive_path.name)
                continue
            LOGGER.info("Loading dbSNP frequency archive: %s", archive_path)
            rows_loaded = 0
            members_seen = 0
            batch: list[FrequencyRecord] = []
            frequency_members = 0
            last_member: str | None = None
            archive_size = archive_path.stat().st_size

            with archive_path.open("rb") as raw_handle:
                with tarfile.open(fileobj=raw_handle, mode="r|gz") as tar:
                    for member in tar:
                        members_seen += 1
                        if limit_members is not None and frequency_members >= limit_members:
                            break
                        if not member.isfile() or not member.name.endswith(FREQUENCY_MEMBER_SUFFIX):
                            continue
                        handle = tar.extractfile(member)
                        if handle is None:
                            continue
                        frequency_members += 1
                        last_member = member.name
                        text = handle.read().decode("utf-8", errors="replace")
                        records = parse_frequency_member(
                            text=text,
                            source_archive=archive_path.name,
                            source_member=member.name,
                        )
                        for record in records:
                            batch.append(record)
                            rows_loaded += 1
                            if len(batch) >= batch_size:
                                _insert_records(connection, batch)
                                batch.clear()
                        if reporter.should_report():
                            total_rows = _count_records(connection) + len(batch)
                            progress_state = _archive_progress_state(
                                archive_index=archive_index,
                                archive_count=len(archives),
                                archive_path=archive_path,
                                archive_size=archive_size,
                                archive_bytes_read=raw_handle.tell(),
                                members_seen=members_seen,
                                frequency_members=frequency_members,
                                rows_loaded=rows_loaded,
                                total_rows=total_rows,
                                last_member=last_member,
                            )
                            checkpoint["progress"] = progress_state
                            _write_checkpoint(checkpoint_path, checkpoint)
                            reporter.report(progress_state)

            if batch:
                _insert_records(connection, batch)
            total_rows = _count_records(connection)
            progress_state = _archive_progress_state(
                archive_index=archive_index,
                archive_count=len(archives),
                archive_path=archive_path,
                archive_size=archive_size,
                archive_bytes_read=archive_size,
                members_seen=members_seen,
                frequency_members=frequency_members,
                rows_loaded=rows_loaded,
                total_rows=total_rows,
                last_member=last_member,
            )
            reporter.report(progress_state, force=True)
            reporter.finish_line()

            distinct_rsids = _count_distinct_rsids(
                connection,
                source_system="ncbi_dbsnp_frequency",
                source_archive=archive_path.name,
            )
            summary = SourceSummary(
                source_archive=archive_path.name,
                source_system="ncbi_dbsnp_frequency",
                members_seen=members_seen,
                frequency_members=frequency_members,
                rows_loaded=rows_loaded,
                distinct_rsids=distinct_rsids,
            )
            _insert_source_summary(connection, summary)
            completed_archives.add(archive_path.name)
            checkpoint["completed_archives"] = sorted(completed_archives)
            checkpoint["progress"] = progress_state
            _write_checkpoint(checkpoint_path, checkpoint)

        if (
            include_legacy
            and legacy_dbsnp_root is not None
            and legacy_dbsnp_root.exists()
            and not checkpoint.get("legacy_loaded")
        ):
            LOGGER.info("Loading legacy HBP dbSNP CSV artifacts: %s", legacy_dbsnp_root)
            rows_loaded = 0
            batch = []
            for record in iter_legacy_records(legacy_dbsnp_root):
                batch.append(record)
                rows_loaded += 1
                if len(batch) >= batch_size:
                    _insert_records(connection, batch)
                    batch.clear()
                if reporter.should_report():
                    total_rows = _count_records(connection) + len(batch)
                    progress_state = {
                        "phase": "legacy_dbsnp",
                        "archive_index": len(archives),
                        "archive_count": len(archives),
                        "archive_name": str(legacy_dbsnp_root),
                        "archive_percent": None,
                        "members_seen": 0,
                        "frequency_members": 0,
                        "rows_loaded": rows_loaded,
                        "total_rows": total_rows,
                        "last_member": None,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    checkpoint["progress"] = progress_state
                    _write_checkpoint(checkpoint_path, checkpoint)
                    reporter.report(progress_state)
            if batch:
                _insert_records(connection, batch)
            distinct_rsids = _count_distinct_rsids(connection, source_system="hbp_legacy_dbsnp")
            summary = SourceSummary(
                source_archive=str(legacy_dbsnp_root),
                source_system="hbp_legacy_dbsnp",
                members_seen=0,
                frequency_members=0,
                rows_loaded=rows_loaded,
                distinct_rsids=distinct_rsids,
            )
            _insert_source_summary(connection, summary)
            checkpoint["legacy_loaded"] = True
            checkpoint["progress"] = {
                "phase": "legacy_dbsnp",
                "archive_index": len(archives),
                "archive_count": len(archives),
                "archive_name": str(legacy_dbsnp_root),
                "archive_percent": 100.0,
                "members_seen": 0,
                "frequency_members": 0,
                "rows_loaded": rows_loaded,
                "total_rows": _count_records(connection),
                "last_member": None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            _write_checkpoint(checkpoint_path, checkpoint)
            reporter.report(checkpoint["progress"], force=True)
            reporter.finish_line()

        _create_indexes_and_views(connection)

        source_summaries = _fetch_source_summaries(connection)
        build_summary = BuildSummary(
            output_db=str(output_db),
            raw_root=str(raw_root),
            legacy_dbsnp_root=str(legacy_dbsnp_root) if legacy_dbsnp_root else None,
            checkpoint_path=str(checkpoint_path),
            sources=source_summaries,
            rows_loaded=_count_records(connection),
            distinct_rsids=_count_distinct_rsids(connection),
            built_at=datetime.now(timezone.utc).isoformat(),
        )
        _insert_build_metadata(connection, build_summary)
        checkpoint["complete"] = True
        checkpoint["completed_at"] = build_summary.built_at
        checkpoint["progress"] = {
            "phase": "complete",
            "archive_index": len(archives),
            "archive_count": len(archives),
            "archive_name": None,
            "archive_percent": 100.0,
            "members_seen": None,
            "frequency_members": None,
            "rows_loaded": build_summary.rows_loaded,
            "total_rows": build_summary.rows_loaded,
            "last_member": None,
            "updated_at": build_summary.built_at,
        }
        _write_checkpoint(checkpoint_path, checkpoint)
        return build_summary
    finally:
        connection.close()


def export_archive_to_parquet(
    *,
    archive_path: Path,
    output_root: Path,
    batch_size: int = 100_000,
    limit_members: int | None = None,
    checkpoint_path: Path | None = None,
    resume: bool = True,
    progress: bool = True,
    progress_interval: float = 30.0,
    compression: str = "zstd",
) -> ParquetExportSummary:
    """Export one dbSNP frequency tarball to compressed Parquet part files.

    This is the preferred HPC handoff path. Each Slurm task should own one
    archive and write disjoint Parquet files; a later single-writer step can
    import the Parquet bundle into DuckDB.
    """

    archive_path = archive_path.resolve()
    output_root = output_root.resolve()
    records_root = output_root / "records"
    manifest_root = output_root / "manifests"
    checkpoint_path = (
        checkpoint_path
        or output_root / "checkpoints" / f"{_safe_path_component(archive_path.name)}.checkpoint.json"
    ).resolve()
    records_root.mkdir(parents=True, exist_ok=True)
    manifest_root.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    if not resume:
        _remove_parquet_parts(records_root, _archive_part_prefix(archive_path))
        checkpoint_path.unlink(missing_ok=True)

    checkpoint = _load_checkpoint(checkpoint_path) if resume else None
    if checkpoint is None:
        checkpoint = {
            "schema_version": 1,
            "mode": "archive_parquet_export",
            "archive_path": str(archive_path),
            "output_root": str(output_root),
            "limit_members": limit_members,
            "completed_frequency_members": 0,
            "members_seen": 0,
            "rows_loaded": 0,
            "parquet_files": [],
            "next_part_index": 0,
            "complete": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "progress": None,
        }
        _write_checkpoint(checkpoint_path, checkpoint)
    else:
        _validate_parquet_export_checkpoint(
            checkpoint=checkpoint,
            mode="archive_parquet_export",
            source_path=archive_path,
            output_root=output_root,
            limit_members=limit_members,
        )
        if checkpoint.get("complete"):
            return _parquet_export_summary_from_checkpoint(checkpoint, output_root=output_root)

    completed_frequency_members = int(checkpoint.get("completed_frequency_members", 0) or 0)
    rows_loaded = int(checkpoint.get("rows_loaded", 0) or 0)
    part_index = int(checkpoint.get("next_part_index", 0) or 0)
    parquet_files = [str(path) for path in checkpoint.get("parquet_files", [])]
    members_seen = 0
    frequency_members = 0
    archive_size = archive_path.stat().st_size
    reporter = _ProgressReporter(enabled=progress, interval_seconds=progress_interval)
    batch: list[FrequencyRecord] = []
    last_member: str | None = None

    LOGGER.info("Exporting dbSNP frequency archive to Parquet: %s", archive_path)
    with archive_path.open("rb") as raw_handle:
        with tarfile.open(fileobj=raw_handle, mode="r|gz") as tar:
            for member in tar:
                members_seen += 1
                if not member.isfile() or not member.name.endswith(FREQUENCY_MEMBER_SUFFIX):
                    continue
                frequency_members += 1
                if limit_members is not None and frequency_members > limit_members:
                    break
                if frequency_members <= completed_frequency_members:
                    continue

                handle = tar.extractfile(member)
                if handle is None:
                    continue
                last_member = member.name
                text = handle.read().decode("utf-8", errors="replace")
                batch.extend(
                    parse_frequency_member(
                        text=text,
                        source_archive=archive_path.name,
                        source_member=member.name,
                    )
                )
                if len(batch) >= batch_size:
                    part_path = _parquet_part_path(records_root, archive_path, part_index)
                    _write_records_parquet(batch, part_path, compression=compression)
                    parquet_files.append(str(part_path))
                    rows_loaded += len(batch)
                    batch.clear()
                    part_index += 1
                    completed_frequency_members = frequency_members
                    _update_parquet_checkpoint(
                        checkpoint_path=checkpoint_path,
                        checkpoint=checkpoint,
                        completed_frequency_members=completed_frequency_members,
                        members_seen=members_seen,
                        rows_loaded=rows_loaded,
                        parquet_files=parquet_files,
                        next_part_index=part_index,
                        progress_state=_archive_progress_state(
                            archive_index=1,
                            archive_count=1,
                            archive_path=archive_path,
                            archive_size=archive_size,
                            archive_bytes_read=raw_handle.tell(),
                            members_seen=members_seen,
                            frequency_members=frequency_members,
                            rows_loaded=rows_loaded,
                            total_rows=rows_loaded,
                            last_member=last_member,
                        ),
                    )
                if reporter.should_report():
                    state = _archive_progress_state(
                        archive_index=1,
                        archive_count=1,
                        archive_path=archive_path,
                        archive_size=archive_size,
                        archive_bytes_read=raw_handle.tell(),
                        members_seen=members_seen,
                        frequency_members=frequency_members,
                        rows_loaded=rows_loaded + len(batch),
                        total_rows=rows_loaded + len(batch),
                        last_member=last_member,
                    )
                    checkpoint["progress"] = state
                    _write_checkpoint(checkpoint_path, checkpoint)
                    reporter.report(state)

    if batch:
        part_path = _parquet_part_path(records_root, archive_path, part_index)
        _write_records_parquet(batch, part_path, compression=compression)
        parquet_files.append(str(part_path))
        rows_loaded += len(batch)
        batch.clear()
        part_index += 1
        completed_frequency_members = frequency_members

    built_at = datetime.now(timezone.utc).isoformat()
    summary = ParquetExportSummary(
        output_root=str(output_root),
        records_root=str(records_root),
        source_archive=archive_path.name,
        source_system="ncbi_dbsnp_frequency",
        parquet_files=parquet_files,
        members_seen=members_seen,
        frequency_members=frequency_members,
        rows_loaded=rows_loaded,
        distinct_rsids=frequency_members,
        built_at=built_at,
    )
    checkpoint["distinct_rsids"] = summary.distinct_rsids
    state = _archive_progress_state(
        archive_index=1,
        archive_count=1,
        archive_path=archive_path,
        archive_size=archive_size,
        archive_bytes_read=archive_size,
        members_seen=members_seen,
        frequency_members=frequency_members,
        rows_loaded=rows_loaded,
        total_rows=rows_loaded,
        last_member=last_member,
    )
    _update_parquet_checkpoint(
        checkpoint_path=checkpoint_path,
        checkpoint=checkpoint,
        completed_frequency_members=completed_frequency_members,
        members_seen=members_seen,
        rows_loaded=rows_loaded,
        parquet_files=parquet_files,
        next_part_index=part_index,
        progress_state=state,
        complete=True,
        completed_at=built_at,
    )
    _write_parquet_manifest(manifest_root / f"{_safe_path_component(archive_path.name)}.manifest.json", summary)
    reporter.report(state, force=True)
    reporter.finish_line()
    return summary


def export_legacy_to_parquet(
    *,
    legacy_dbsnp_root: Path,
    output_root: Path,
    batch_size: int = 100_000,
    checkpoint_path: Path | None = None,
    resume: bool = True,
    progress: bool = True,
    progress_interval: float = 30.0,
    compression: str = "zstd",
) -> ParquetExportSummary:
    """Export existing HBP legacy dbSNP CSV artifacts to Parquet."""

    legacy_dbsnp_root = legacy_dbsnp_root.resolve()
    output_root = output_root.resolve()
    records_root = output_root / "records"
    manifest_root = output_root / "manifests"
    checkpoint_path = (checkpoint_path or output_root / "checkpoints" / "hbp_legacy_dbsnp.checkpoint.json").resolve()
    records_root.mkdir(parents=True, exist_ok=True)
    manifest_root.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    if not resume:
        _remove_parquet_parts(records_root, "hbp_legacy_dbsnp")
        checkpoint_path.unlink(missing_ok=True)

    checkpoint = _load_checkpoint(checkpoint_path) if resume else None
    if checkpoint is None:
        checkpoint = {
            "schema_version": 1,
            "mode": "legacy_parquet_export",
            "source_path": str(legacy_dbsnp_root),
            "output_root": str(output_root),
            "completed_rows": 0,
            "rows_loaded": 0,
            "parquet_files": [],
            "next_part_index": 0,
            "complete": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "progress": None,
        }
        _write_checkpoint(checkpoint_path, checkpoint)
    else:
        _validate_parquet_export_checkpoint(
            checkpoint=checkpoint,
            mode="legacy_parquet_export",
            source_path=legacy_dbsnp_root,
            output_root=output_root,
            limit_members=None,
        )
        if checkpoint.get("complete"):
            return _parquet_export_summary_from_checkpoint(checkpoint, output_root=output_root)

    completed_rows = int(checkpoint.get("completed_rows", 0) or 0)
    rows_seen = 0
    rows_loaded = int(checkpoint.get("rows_loaded", 0) or 0)
    part_index = int(checkpoint.get("next_part_index", 0) or 0)
    parquet_files = [str(path) for path in checkpoint.get("parquet_files", [])]
    reporter = _ProgressReporter(enabled=progress, interval_seconds=progress_interval)
    batch: list[FrequencyRecord] = []
    distinct_rsids: set[str] = set()

    LOGGER.info("Exporting legacy HBP dbSNP CSV artifacts to Parquet: %s", legacy_dbsnp_root)
    for record in iter_legacy_records(legacy_dbsnp_root):
        rows_seen += 1
        distinct_rsids.add(record.rsid)
        if rows_seen <= completed_rows:
            continue
        batch.append(record)
        if len(batch) >= batch_size:
            part_path = _parquet_part_path(records_root, Path("hbp_legacy_dbsnp"), part_index)
            _write_records_parquet(batch, part_path, compression=compression)
            parquet_files.append(str(part_path))
            rows_loaded += len(batch)
            batch.clear()
            part_index += 1
            completed_rows = rows_seen
            _update_legacy_parquet_checkpoint(
                checkpoint_path=checkpoint_path,
                checkpoint=checkpoint,
                completed_rows=completed_rows,
                rows_loaded=rows_loaded,
                parquet_files=parquet_files,
                next_part_index=part_index,
            )
        if reporter.should_report():
            state = {
                "phase": "legacy_parquet",
                "archive_index": 1,
                "archive_count": 1,
                "archive_name": str(legacy_dbsnp_root),
                "archive_percent": None,
                "members_seen": rows_seen,
                "frequency_members": 0,
                "rows_loaded": rows_loaded + len(batch),
                "total_rows": rows_loaded + len(batch),
                "last_member": None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            checkpoint["progress"] = state
            _write_checkpoint(checkpoint_path, checkpoint)
            reporter.report(state)

    if batch:
        part_path = _parquet_part_path(records_root, Path("hbp_legacy_dbsnp"), part_index)
        _write_records_parquet(batch, part_path, compression=compression)
        parquet_files.append(str(part_path))
        rows_loaded += len(batch)
        batch.clear()
        part_index += 1
        completed_rows = rows_seen

    built_at = datetime.now(timezone.utc).isoformat()
    summary = ParquetExportSummary(
        output_root=str(output_root),
        records_root=str(records_root),
        source_archive=str(legacy_dbsnp_root),
        source_system="hbp_legacy_dbsnp",
        parquet_files=parquet_files,
        members_seen=0,
        frequency_members=0,
        rows_loaded=rows_loaded,
        distinct_rsids=len(distinct_rsids) if distinct_rsids else 0,
        built_at=built_at,
    )
    checkpoint["distinct_rsids"] = summary.distinct_rsids
    _update_legacy_parquet_checkpoint(
        checkpoint_path=checkpoint_path,
        checkpoint=checkpoint,
        completed_rows=completed_rows,
        rows_loaded=rows_loaded,
        parquet_files=parquet_files,
        next_part_index=part_index,
        complete=True,
        completed_at=built_at,
    )
    _write_parquet_manifest(manifest_root / "hbp_legacy_dbsnp.manifest.json", summary)
    reporter.finish_line()
    return summary


def build_dbsnp_frequency_index_from_parquet(
    *,
    parquet_root: Path,
    output_db: Path,
    replace: bool = False,
    threads: int | None = None,
    memory_limit: str | None = None,
    temp_directory: Path | None = None,
) -> BuildSummary:
    """Build the final DuckDB frequency index from normalized Parquet records."""

    parquet_root = parquet_root.resolve()
    records_root = parquet_root / "records"
    output_db = output_db.resolve()
    parquet_files = sorted(records_root.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No dbSNP frequency Parquet files found under {records_root}")
    if replace and output_db.exists():
        output_db.unlink()
    output_db.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(output_db))
    try:
        _configure_duckdb(connection, threads=threads, memory_limit=memory_limit, temp_directory=temp_directory)
        _initialise_schema(connection)
        columns = ", ".join(_frequency_record_columns())
        connection.execute(
            f"""
            INSERT INTO dbsnp_frequency_records
            SELECT {columns}
            FROM read_parquet({_duckdb_string_list(parquet_files)}, union_by_name = true)
            """
        )
        _insert_parquet_source_summaries(connection)
        _create_indexes_and_views(connection)

        source_summaries = _fetch_source_summaries(connection)
        build_summary = BuildSummary(
            output_db=str(output_db),
            raw_root=str(parquet_root),
            legacy_dbsnp_root=None,
            checkpoint_path=None,
            sources=source_summaries,
            rows_loaded=_count_records(connection),
            distinct_rsids=_count_distinct_rsids(connection),
            built_at=datetime.now(timezone.utc).isoformat(),
        )
        _insert_build_metadata(connection, build_summary)
        _write_checkpoint(
            parquet_root / "dbsnp_frequency_duckdb_import_manifest.json",
            {
                "schema_version": 1,
                "mode": "parquet_to_duckdb",
                "parquet_root": str(parquet_root),
                "output_db": str(output_db),
                "parquet_files": [str(path) for path in parquet_files],
                "rows_loaded": build_summary.rows_loaded,
                "distinct_rsids": build_summary.distinct_rsids,
                "built_at": build_summary.built_at,
                "sources": [asdict(source) for source in source_summaries],
            },
        )
        return build_summary
    finally:
        connection.close()


class _ProgressReporter:
    def __init__(self, *, enabled: bool, interval_seconds: float) -> None:
        self.enabled = enabled
        self.interval_seconds = max(interval_seconds, 0.1)
        self.last_report_at = 0.0
        self.used_tty_line = False

    def should_report(self) -> bool:
        if not self.enabled:
            return False
        now = time.monotonic()
        if now - self.last_report_at < self.interval_seconds:
            return False
        self.last_report_at = now
        return True

    def report(self, state: dict[str, object], *, force: bool = False) -> None:
        if not self.enabled:
            return
        if force:
            self.last_report_at = time.monotonic()
        message = _format_progress_message(state)
        if sys.stderr.isatty():
            self.used_tty_line = True
            sys.stderr.write("\r" + message)
            sys.stderr.flush()
        else:
            LOGGER.info(message)

    def finish_line(self) -> None:
        if self.enabled and self.used_tty_line:
            sys.stderr.write("\n")
            sys.stderr.flush()
            self.used_tty_line = False


def _format_progress_message(state: dict[str, object]) -> str:
    phase = state.get("phase") or "archive"
    archive_name = state.get("archive_name") or "complete"
    rows_loaded = _format_int(state.get("rows_loaded"))
    total_rows = _format_int(state.get("total_rows"))
    members = _format_int(state.get("frequency_members"))
    last_member = state.get("last_member") or "-"
    percent = state.get("archive_percent")
    if isinstance(percent, (float, int)):
        bar = _progress_bar(float(percent))
        percent_text = f"{float(percent):5.1f}%"
    else:
        bar = "[????????????????????]"
        percent_text = "  n/a"
    return (
        f"{phase} {state.get('archive_index')}/{state.get('archive_count')} "
        f"{archive_name} {bar} {percent_text} "
        f"members={members} rows={rows_loaded} total_rows={total_rows} last={last_member}"
    )


def _progress_bar(percent: float, *, width: int = 20) -> str:
    bounded = max(0.0, min(100.0, percent))
    filled = int(round((bounded / 100.0) * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _format_int(value: object) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    return "n/a"


def _archive_progress_state(
    *,
    archive_index: int,
    archive_count: int,
    archive_path: Path,
    archive_size: int,
    archive_bytes_read: int,
    members_seen: int,
    frequency_members: int,
    rows_loaded: int,
    total_rows: int,
    last_member: str | None,
) -> dict[str, object]:
    percent = (archive_bytes_read / archive_size * 100.0) if archive_size else None
    return {
        "phase": "archive",
        "archive_index": archive_index,
        "archive_count": archive_count,
        "archive_name": archive_path.name,
        "archive_size_bytes": archive_size,
        "archive_bytes_read": archive_bytes_read,
        "archive_percent": min(percent, 100.0) if percent is not None else None,
        "members_seen": members_seen,
        "frequency_members": frequency_members,
        "rows_loaded": rows_loaded,
        "total_rows": total_rows,
        "last_member": last_member,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _new_checkpoint(
    *,
    raw_root: Path,
    output_db: Path,
    legacy_dbsnp_root: Path | None,
    archives: Sequence[Path],
    include_legacy: bool,
    limit_members: int | None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "raw_root": str(raw_root),
        "output_db": str(output_db),
        "legacy_dbsnp_root": str(legacy_dbsnp_root) if legacy_dbsnp_root else None,
        "include_legacy": include_legacy,
        "limit_members": limit_members,
        "archives": [path.name for path in archives],
        "completed_archives": [],
        "legacy_loaded": False,
        "complete": False,
        "progress": None,
    }


def _load_checkpoint(checkpoint_path: Path) -> dict[str, object] | None:
    if not checkpoint_path.exists():
        return None
    with checkpoint_path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _write_checkpoint(checkpoint_path: Path, checkpoint: dict[str, object]) -> None:
    temporary_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8") as handle:
        json.dump(checkpoint, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temporary_path.replace(checkpoint_path)


def _validate_checkpoint(
    *,
    checkpoint: dict[str, object],
    raw_root: Path,
    output_db: Path,
    legacy_dbsnp_root: Path | None,
    archives: Sequence[Path],
    include_legacy: bool,
    limit_members: int | None,
) -> None:
    expected = {
        "raw_root": str(raw_root),
        "output_db": str(output_db),
        "legacy_dbsnp_root": str(legacy_dbsnp_root) if legacy_dbsnp_root else None,
        "include_legacy": include_legacy,
        "limit_members": limit_members,
        "archives": [path.name for path in archives],
    }
    mismatches = [
        f"{key}: checkpoint={checkpoint.get(key)!r} current={value!r}"
        for key, value in expected.items()
        if checkpoint.get(key) != value
    ]
    if mismatches:
        raise ValueError(
            "dbSNP checkpoint does not match this run. Use --reset to rebuild from scratch. "
            + "; ".join(mismatches)
        )


def _cleanup_incomplete_resume_rows(
    connection: duckdb.DuckDBPyConnection,
    archives: Sequence[Path],
    checkpoint: dict[str, object],
) -> None:
    completed = set(checkpoint.get("completed_archives", []))
    for archive_path in archives:
        if archive_path.name in completed:
            continue
        connection.execute(
            "DELETE FROM dbsnp_frequency_records WHERE source_system = ? AND source_archive = ?",
            ("ncbi_dbsnp_frequency", archive_path.name),
        )
        connection.execute(
            "DELETE FROM dbsnp_frequency_sources WHERE source_system = ? AND source_archive = ?",
            ("ncbi_dbsnp_frequency", archive_path.name),
        )
    if not checkpoint.get("legacy_loaded"):
        connection.execute("DELETE FROM dbsnp_frequency_records WHERE source_system = ?", ("hbp_legacy_dbsnp",))
        connection.execute("DELETE FROM dbsnp_frequency_sources WHERE source_system = ?", ("hbp_legacy_dbsnp",))
    connection.execute("DELETE FROM dbsnp_frequency_build_metadata")


def _count_records(connection: duckdb.DuckDBPyConnection) -> int:
    return int(connection.execute("SELECT count(*) FROM dbsnp_frequency_records").fetchone()[0])


def _count_distinct_rsids(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_system: str | None = None,
    source_archive: str | None = None,
) -> int:
    clauses: list[str] = []
    params: list[str] = []
    if source_system is not None:
        clauses.append("source_system = ?")
        params.append(source_system)
    if source_archive is not None:
        clauses.append("source_archive = ?")
        params.append(source_archive)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    return int(connection.execute(f"SELECT count(DISTINCT rsid) FROM dbsnp_frequency_records{where}", params).fetchone()[0])


def _fetch_source_summaries(connection: duckdb.DuckDBPyConnection) -> list[SourceSummary]:
    rows = connection.execute(
        """
        SELECT source_archive, source_system, members_seen, frequency_members, rows_loaded, distinct_rsids
        FROM dbsnp_frequency_sources
        ORDER BY source_system, source_archive
        """
    ).fetchall()
    return [
        SourceSummary(
            source_archive=row[0],
            source_system=row[1],
            members_seen=int(row[2]),
            frequency_members=int(row[3]),
            rows_loaded=int(row[4]),
            distinct_rsids=int(row[5]),
        )
        for row in rows
    ]


def _initialise_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("DROP VIEW IF EXISTS dbsnp_frequency_population_summary")
    connection.execute("DROP VIEW IF EXISTS dbsnp_frequency_rsid_summary")
    connection.execute("DROP TABLE IF EXISTS dbsnp_frequency_build_metadata")
    connection.execute("DROP TABLE IF EXISTS dbsnp_frequency_sources")
    connection.execute("DROP TABLE IF EXISTS dbsnp_frequency_records")
    connection.execute(
        """
        CREATE TABLE dbsnp_frequency_records (
            rsid VARCHAR NOT NULL,
            study VARCHAR,
            population VARCHAR,
            population_group VARCHAR,
            sample_size BIGINT,
            ref_allele VARCHAR,
            ref_frequency DOUBLE,
            alt_allele VARCHAR,
            alt_frequency DOUBLE,
            ref_allele_raw VARCHAR,
            alt_allele_raw VARCHAR,
            bioproject_id VARCHAR,
            biosample_id VARCHAR,
            source_system VARCHAR NOT NULL,
            source_archive VARCHAR,
            source_member VARCHAR,
            source_url VARCHAR,
            ncbi_build VARCHAR,
            released VARCHAR,
            organism VARCHAR,
            position VARCHAR,
            variation_type VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE dbsnp_frequency_sources (
            source_archive VARCHAR,
            source_system VARCHAR,
            members_seen BIGINT,
            frequency_members BIGINT,
            rows_loaded BIGINT,
            distinct_rsids BIGINT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE dbsnp_frequency_build_metadata (
            built_at VARCHAR,
            output_db VARCHAR,
            raw_root VARCHAR,
            legacy_dbsnp_root VARCHAR,
            rows_loaded BIGINT,
            distinct_rsids BIGINT,
            sources_json VARCHAR
        )
        """
    )


def _insert_records(connection: duckdb.DuckDBPyConnection, records: Sequence[FrequencyRecord]) -> None:
    connection.executemany(
        """
        INSERT INTO dbsnp_frequency_records VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [tuple(asdict(record).values()) for record in records],
    )


def _insert_source_summary(connection: duckdb.DuckDBPyConnection, summary: SourceSummary) -> None:
    connection.execute(
        "INSERT INTO dbsnp_frequency_sources VALUES (?, ?, ?, ?, ?, ?)",
        (
            summary.source_archive,
            summary.source_system,
            summary.members_seen,
            summary.frequency_members,
            summary.rows_loaded,
            summary.distinct_rsids,
        ),
    )


def _insert_build_metadata(connection: duckdb.DuckDBPyConnection, summary: BuildSummary) -> None:
    connection.execute(
        "INSERT INTO dbsnp_frequency_build_metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            summary.built_at,
            summary.output_db,
            summary.raw_root,
            summary.legacy_dbsnp_root,
            summary.rows_loaded,
            summary.distinct_rsids,
            json.dumps([asdict(source) for source in summary.sources], sort_keys=True),
        ),
    )


def _create_indexes_and_views(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE INDEX IF NOT EXISTS dbsnp_frequency_records_rsid_idx ON dbsnp_frequency_records(rsid)")
    connection.execute("CREATE INDEX IF NOT EXISTS dbsnp_frequency_records_study_idx ON dbsnp_frequency_records(study)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS dbsnp_frequency_records_population_idx ON dbsnp_frequency_records(population)"
    )
    connection.execute("DROP VIEW IF EXISTS dbsnp_frequency_population_summary")
    connection.execute("DROP VIEW IF EXISTS dbsnp_frequency_rsid_summary")
    connection.execute(
        """
        CREATE VIEW dbsnp_frequency_rsid_summary AS
        SELECT
            rsid,
            count(*) AS row_count,
            count(DISTINCT study) AS study_count,
            count(DISTINCT population) AS population_count,
            count(DISTINCT population_group) AS population_group_count,
            count(DISTINCT source_system) AS source_count,
            max(sample_size) AS max_sample_size,
            min(alt_frequency) AS min_alt_frequency,
            max(alt_frequency) AS max_alt_frequency
        FROM dbsnp_frequency_records
        GROUP BY rsid
        """
    )
    connection.execute(
        """
        CREATE VIEW dbsnp_frequency_population_summary AS
        SELECT
            rsid,
            study,
            population,
            population_group,
            alt_allele,
            count(*) AS row_count,
            count(DISTINCT source_system) AS source_count,
            max(sample_size) AS max_sample_size,
            min(alt_frequency) AS min_alt_frequency,
            avg(alt_frequency) AS mean_alt_frequency,
            max(alt_frequency) AS max_alt_frequency
        FROM dbsnp_frequency_records
        GROUP BY rsid, study, population, population_group, alt_allele
        """
    )


def _frequency_record_columns() -> list[str]:
    return list(FrequencyRecord.__dataclass_fields__)


def _write_records_parquet(
    records: Sequence[FrequencyRecord],
    path: Path,
    *,
    compression: str,
) -> None:
    if not records:
        return

    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    frame = pd.DataFrame([asdict(record) for record in records], columns=_frequency_record_columns())
    connection = duckdb.connect()
    try:
        connection.register("records_frame", frame)
        connection.execute(
            f"""
            COPY records_frame TO {_duckdb_string(temporary_path)}
            (FORMAT PARQUET, COMPRESSION {_duckdb_string(compression.upper())})
            """
        )
        connection.unregister("records_frame")
    finally:
        connection.close()
    temporary_path.replace(path)


def _insert_parquet_source_summaries(connection: duckdb.DuckDBPyConnection) -> None:
    rows = connection.execute(
        """
        SELECT
            coalesce(source_archive, 'hbp_legacy_dbsnp') AS source_archive,
            source_system,
            CASE
                WHEN source_system = 'ncbi_dbsnp_frequency' THEN count(DISTINCT source_member)
                ELSE 0
            END AS members_seen,
            CASE
                WHEN source_system = 'ncbi_dbsnp_frequency' THEN count(DISTINCT source_member)
                ELSE 0
            END AS frequency_members,
            count(*) AS rows_loaded,
            count(DISTINCT rsid) AS distinct_rsids
        FROM dbsnp_frequency_records
        GROUP BY source_archive, source_system
        ORDER BY source_system, source_archive
        """
    ).fetchall()
    for row in rows:
        _insert_source_summary(
            connection,
            SourceSummary(
                source_archive=str(row[0]),
                source_system=str(row[1]),
                members_seen=int(row[2]),
                frequency_members=int(row[3]),
                rows_loaded=int(row[4]),
                distinct_rsids=int(row[5]),
            ),
        )


def _configure_duckdb(
    connection: duckdb.DuckDBPyConnection,
    *,
    threads: int | None,
    memory_limit: str | None,
    temp_directory: Path | None,
) -> None:
    if threads:
        connection.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        connection.execute(f"SET memory_limit = {_duckdb_string(memory_limit)}")
    if temp_directory:
        temp_directory.mkdir(parents=True, exist_ok=True)
        connection.execute(f"SET temp_directory = {_duckdb_string(temp_directory)}")


def _safe_path_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "part"


def _archive_part_prefix(archive_path: Path) -> str:
    name = archive_path.name
    if name.endswith(".tar.gz"):
        name = name[:-7]
    return _safe_path_component(name)


def _parquet_part_path(records_root: Path, source_path: Path, part_index: int) -> Path:
    return records_root / f"{_archive_part_prefix(source_path)}.part-{part_index:06d}.parquet"


def _remove_parquet_parts(records_root: Path, part_prefix: str) -> None:
    for path in records_root.glob(f"{part_prefix}.part-*.parquet"):
        path.unlink()


def _write_parquet_manifest(path: Path, summary: ParquetExportSummary) -> None:
    _write_checkpoint(path, asdict(summary))


def _validate_parquet_export_checkpoint(
    *,
    checkpoint: dict[str, object],
    mode: str,
    source_path: Path,
    output_root: Path,
    limit_members: int | None,
) -> None:
    expected = {
        "mode": mode,
        "output_root": str(output_root),
    }
    if mode == "archive_parquet_export":
        expected["archive_path"] = str(source_path)
        expected["limit_members"] = limit_members
    else:
        expected["source_path"] = str(source_path)
    mismatches = [
        f"{key}: checkpoint={checkpoint.get(key)!r} current={value!r}"
        for key, value in expected.items()
        if checkpoint.get(key) != value
    ]
    if mismatches:
        raise ValueError(
            "dbSNP Parquet checkpoint does not match this run. Use --reset to rebuild this shard. "
            + "; ".join(mismatches)
        )


def _update_parquet_checkpoint(
    *,
    checkpoint_path: Path,
    checkpoint: dict[str, object],
    completed_frequency_members: int,
    members_seen: int,
    rows_loaded: int,
    parquet_files: list[str],
    next_part_index: int,
    progress_state: dict[str, object],
    complete: bool = False,
    completed_at: str | None = None,
) -> None:
    checkpoint["completed_frequency_members"] = completed_frequency_members
    checkpoint["members_seen"] = members_seen
    checkpoint["rows_loaded"] = rows_loaded
    checkpoint["parquet_files"] = parquet_files
    checkpoint["next_part_index"] = next_part_index
    checkpoint["progress"] = progress_state
    checkpoint["complete"] = complete
    checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
    if completed_at:
        checkpoint["completed_at"] = completed_at
    _write_checkpoint(checkpoint_path, checkpoint)


def _update_legacy_parquet_checkpoint(
    *,
    checkpoint_path: Path,
    checkpoint: dict[str, object],
    completed_rows: int,
    rows_loaded: int,
    parquet_files: list[str],
    next_part_index: int,
    complete: bool = False,
    completed_at: str | None = None,
) -> None:
    checkpoint["completed_rows"] = completed_rows
    checkpoint["rows_loaded"] = rows_loaded
    checkpoint["parquet_files"] = parquet_files
    checkpoint["next_part_index"] = next_part_index
    checkpoint["complete"] = complete
    checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
    if completed_at:
        checkpoint["completed_at"] = completed_at
    _write_checkpoint(checkpoint_path, checkpoint)


def _parquet_export_summary_from_checkpoint(
    checkpoint: dict[str, object],
    *,
    output_root: Path,
) -> ParquetExportSummary:
    source_archive = str(checkpoint.get("archive_path") or checkpoint.get("source_path") or "")
    if source_archive.endswith(".tar.gz"):
        source_archive = Path(source_archive).name
        source_system = "ncbi_dbsnp_frequency"
        frequency_members = int(checkpoint.get("completed_frequency_members", 0) or 0)
        members_seen = int(checkpoint.get("members_seen", 0) or frequency_members)
    else:
        source_system = "hbp_legacy_dbsnp"
        frequency_members = 0
        members_seen = 0
    rows_loaded = int(checkpoint.get("rows_loaded", 0) or 0)
    distinct_rsids = int(checkpoint.get("distinct_rsids", 0) or frequency_members)
    return ParquetExportSummary(
        output_root=str(output_root),
        records_root=str(output_root / "records"),
        source_archive=source_archive,
        source_system=source_system,
        parquet_files=[str(path) for path in checkpoint.get("parquet_files", [])],
        members_seen=members_seen,
        frequency_members=frequency_members,
        rows_loaded=rows_loaded,
        distinct_rsids=distinct_rsids,
        built_at=str(checkpoint.get("completed_at") or checkpoint.get("updated_at") or ""),
    )


def _duckdb_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _duckdb_string_list(paths: Sequence[Path]) -> str:
    return "[" + ", ".join(_duckdb_string(path) for path in paths) + "]"


def _metadata_rsid(metadata: dict[str, str]) -> str | None:
    report = metadata.get("NCBI Reference SNP (rs) Report ALPHA") or ""
    match = re.search(r"\brs\d+\b", report)
    return match.group(0) if match else None


def _rsid_from_path(path: str | None) -> str | None:
    if not path:
        return None
    match = re.search(r"\brs\d+\b", path)
    return match.group(0) if match else None


def _parse_allele_frequency(value: str | None) -> tuple[str | None, float | None]:
    value = _clean(value)
    if not value:
        return None, None
    if "=" not in value:
        return value, None
    allele, frequency = value.split("=", 1)
    try:
        parsed_frequency = float(frequency)
    except ValueError:
        parsed_frequency = None
    return _clean(allele), parsed_frequency


def _parse_int(value: str | None) -> int | None:
    value = _clean(value)
    if not value:
        return None
    try:
        return int(float(value.replace(",", "")))
    except ValueError:
        return None


def _clean(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
