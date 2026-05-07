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
import tarfile
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
    sources: list[SourceSummary]
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
) -> BuildSummary:
    """Stream dbSNP frequency archives into a DuckDB index."""

    raw_root = raw_root.resolve()
    output_db = output_db.resolve()
    output_db.parent.mkdir(parents=True, exist_ok=True)

    archives = discover_frequency_archives(raw_root)
    if not archives:
        raise FileNotFoundError(f"No dbSNP frequency archives found under {raw_root}")

    connection = duckdb.connect(str(output_db))
    try:
        _initialise_schema(connection)
        source_summaries: list[SourceSummary] = []
        total_rows = 0
        all_rsids: set[str] = set()

        for archive_path in archives:
            LOGGER.info("Loading dbSNP frequency archive: %s", archive_path)
            rows_loaded = 0
            members_seen = 0
            rsids: set[str] = set()
            batch: list[FrequencyRecord] = []
            frequency_members = 0
            last_member: str | None = None

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
                    last_member = member.name
                    text = handle.read().decode("utf-8", errors="replace")
                    records = parse_frequency_member(
                        text=text,
                        source_archive=archive_path.name,
                        source_member=member.name,
                    )
                    for record in records:
                        batch.append(record)
                        rsids.add(record.rsid)
                        all_rsids.add(record.rsid)
                        rows_loaded += 1
                        total_rows += 1
                        if len(batch) >= batch_size:
                            _insert_records(connection, batch)
                            batch.clear()
                    if frequency_members % 100_000 == 0:
                        LOGGER.info(
                            "dbSNP load progress: archive=%s members=%s rows=%s last=%s",
                            archive_path.name,
                            frequency_members,
                            rows_loaded,
                            last_member,
                        )

            if batch:
                _insert_records(connection, batch)
            summary = SourceSummary(
                source_archive=archive_path.name,
                source_system="ncbi_dbsnp_frequency",
                members_seen=members_seen,
                frequency_members=frequency_members,
                rows_loaded=rows_loaded,
                distinct_rsids=len(rsids),
            )
            source_summaries.append(summary)
            _insert_source_summary(connection, summary)

        if include_legacy and legacy_dbsnp_root is not None and legacy_dbsnp_root.exists():
            rows_loaded = 0
            rsids = set()
            batch = []
            for record in iter_legacy_records(legacy_dbsnp_root):
                batch.append(record)
                rsids.add(record.rsid)
                all_rsids.add(record.rsid)
                rows_loaded += 1
                total_rows += 1
                if len(batch) >= batch_size:
                    _insert_records(connection, batch)
                    batch.clear()
            if batch:
                _insert_records(connection, batch)
            summary = SourceSummary(
                source_archive=str(legacy_dbsnp_root),
                source_system="hbp_legacy_dbsnp",
                members_seen=0,
                frequency_members=0,
                rows_loaded=rows_loaded,
                distinct_rsids=len(rsids),
            )
            source_summaries.append(summary)
            _insert_source_summary(connection, summary)

        _create_indexes_and_views(connection)

        build_summary = BuildSummary(
            output_db=str(output_db),
            raw_root=str(raw_root),
            legacy_dbsnp_root=str(legacy_dbsnp_root) if legacy_dbsnp_root else None,
            sources=source_summaries,
            rows_loaded=total_rows,
            distinct_rsids=len(all_rsids),
            built_at=datetime.now(timezone.utc).isoformat(),
        )
        _insert_build_metadata(connection, build_summary)
        return build_summary
    finally:
        connection.close()


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
    connection.execute("CREATE INDEX dbsnp_frequency_records_rsid_idx ON dbsnp_frequency_records(rsid)")
    connection.execute("CREATE INDEX dbsnp_frequency_records_study_idx ON dbsnp_frequency_records(study)")
    connection.execute("CREATE INDEX dbsnp_frequency_records_population_idx ON dbsnp_frequency_records(population)")
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
