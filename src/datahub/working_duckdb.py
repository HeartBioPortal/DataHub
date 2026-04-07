"""Working DuckDB schema utilities for DataHub's target lifecycle model.

This module implements the first concrete slice of the DataHub lifecycle model:
source-native raw release registration, schema drift reporting, source-normalized
association rows, and analysis-ready association rows. It is intentionally
additive so the current unified publish pipeline can keep using its existing
points table while newer workflows migrate toward these tables.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


WORKING_SCHEMA_VERSION = 1

SOURCE_NORMALIZED_ASSOCIATION_TABLE = "source_normalized_association"
ANALYSIS_READY_ASSOCIATION_TABLE = "analysis_ready_association"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class CsvSchema:
    """Detected source file schema."""

    columns: tuple[str, ...]
    delimiter: str
    fingerprint: str


@dataclass(frozen=True)
class SchemaDriftResult:
    """Comparison between detected and expected source columns."""

    status: str
    detected_columns: tuple[str, ...]
    expected_columns: tuple[str, ...]
    added_columns: tuple[str, ...]
    removed_columns: tuple[str, ...]
    message: str

    @property
    def is_breaking(self) -> bool:
        return self.status == "breaking"


@dataclass(frozen=True)
class RawFileRegistration:
    """Registered raw source file and drift status."""

    file_path: Path
    schema: CsvSchema
    sha256: str | None
    drift: SchemaDriftResult


def quote_identifier(identifier: str) -> str:
    """Return a safe SQL identifier for DataHub-generated table names."""

    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return identifier


def json_dumps(value: Any) -> str:
    """Stable compact JSON serialization for metadata columns."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def normalize_columns(columns: Iterable[str]) -> tuple[str, ...]:
    """Normalize a column sequence while preserving order and removing blanks."""

    seen: set[str] = set()
    normalized: list[str] = []
    for column in columns:
        name = str(column).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return tuple(normalized)


def schema_fingerprint(columns: Sequence[str]) -> str:
    """Hash a source schema by ordered column names."""

    payload = json_dumps(list(columns)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def expected_columns_from_prep_profile(profile: Any) -> tuple[str, ...]:
    """Return expected raw source columns from a preparation profile."""

    columns: list[str] = []
    for candidates in getattr(profile, "field_candidates", {}).values():
        columns.extend(candidates)
    columns.extend(getattr(profile, "ancestry_fields", {}).keys())
    return normalize_columns(columns)


def expected_column_groups_from_prep_profile(profile: Any) -> tuple[tuple[str, ...], ...]:
    """Return candidate groups from a prep profile for schema drift checks.

    Prep profiles often define alternative source columns for the same logical
    field. A source release should satisfy at least one candidate in a group,
    not necessarily every alias ever listed for that field.
    """

    groups: list[tuple[str, ...]] = []
    for candidates in getattr(profile, "field_candidates", {}).values():
        group = normalize_columns(candidates)
        if group:
            groups.append(group)
    return tuple(groups)


def inspect_csv_schema(file_path: str | Path, *, delimiter: str | None = None) -> CsvSchema:
    """Inspect a source CSV/TSV header without loading the full file."""

    path = Path(file_path)
    open_func = gzip.open if path.name.lower().endswith(".gz") else open
    with open_func(path, "rt", encoding="utf-8", errors="replace", newline="") as stream:
        first_line = stream.readline()

    if not first_line:
        columns: tuple[str, ...] = ()
        detected_delimiter = delimiter or ","
    else:
        detected_delimiter = delimiter or _guess_delimiter(first_line)
        reader = csv.reader([first_line], delimiter=detected_delimiter)
        columns = normalize_columns(next(reader, []))

    return CsvSchema(
        columns=columns,
        delimiter=detected_delimiter,
        fingerprint=schema_fingerprint(columns),
    )


def _guess_delimiter(line: str) -> str:
    candidates = [",", "\t", "|", ";"]
    return max(candidates, key=line.count)


def file_sha256(file_path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 for a source file."""

    digest = hashlib.sha256()
    with Path(file_path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compare_schema(
    *,
    detected_columns: Sequence[str],
    expected_columns: Sequence[str] | None = None,
) -> SchemaDriftResult:
    """Compare detected source columns against an expected contract."""

    detected = normalize_columns(detected_columns)
    expected = normalize_columns(expected_columns or ())
    if not expected:
        return SchemaDriftResult(
            status="unchecked",
            detected_columns=detected,
            expected_columns=expected,
            added_columns=(),
            removed_columns=(),
            message="No expected schema was provided; source columns were inventoried only.",
        )

    detected_set = set(detected)
    expected_set = set(expected)
    added = tuple(column for column in detected if column not in expected_set)
    removed = tuple(column for column in expected if column not in detected_set)

    if removed:
        return SchemaDriftResult(
            status="breaking",
            detected_columns=detected,
            expected_columns=expected,
            added_columns=added,
            removed_columns=removed,
            message="Detected schema is missing expected columns required by the source contract.",
        )
    if added:
        return SchemaDriftResult(
            status="compatible_with_additions",
            detected_columns=detected,
            expected_columns=expected,
            added_columns=added,
            removed_columns=removed,
            message="Detected schema has extra columns; existing required columns are present.",
        )
    return SchemaDriftResult(
        status="compatible",
        detected_columns=detected,
        expected_columns=expected,
        added_columns=(),
        removed_columns=(),
        message="Detected schema matches the expected column contract.",
    )


def compare_schema_candidate_groups(
    *,
    detected_columns: Sequence[str],
    expected_column_groups: Sequence[Sequence[str]],
) -> SchemaDriftResult:
    """Compare detected columns against one-of candidate groups."""

    detected = normalize_columns(detected_columns)
    detected_set = set(detected)
    groups = tuple(tuple(group) for group in expected_column_groups if group)
    expected = normalize_columns(column for group in groups for column in group)
    added = tuple(column for column in detected if column not in set(expected))
    missing_groups = tuple(
        group for group in groups if not any(column in detected_set for column in group)
    )
    removed = tuple("one_of(" + "|".join(group) + ")" for group in missing_groups)
    if removed:
        return SchemaDriftResult(
            status="breaking",
            detected_columns=detected,
            expected_columns=expected,
            added_columns=added,
            removed_columns=removed,
            message="Detected schema is missing one or more required source-column candidate groups.",
        )
    if added:
        return SchemaDriftResult(
            status="compatible_with_additions",
            detected_columns=detected,
            expected_columns=expected,
            added_columns=added,
            removed_columns=(),
            message="Detected schema has extra columns; required source-column candidate groups are present.",
        )
    return SchemaDriftResult(
        status="compatible",
        detected_columns=detected,
        expected_columns=expected,
        added_columns=(),
        removed_columns=(),
        message="Detected schema satisfies the expected source-column candidate groups.",
    )


def ensure_working_schema(connection: Any) -> None:
    """Create DataHub working lifecycle tables if they do not exist."""

    connection.execute(
        """
CREATE TABLE IF NOT EXISTS datahub_working_schema_metadata (
    schema_name VARCHAR,
    schema_version INTEGER,
    created_at TIMESTAMP,
    metadata_json VARCHAR
);
"""
    )
    connection.execute(
        """
INSERT INTO datahub_working_schema_metadata
SELECT 'datahub_working_duckdb', ?, now(), ?
WHERE NOT EXISTS (
    SELECT 1 FROM datahub_working_schema_metadata
    WHERE schema_name = 'datahub_working_duckdb'
);
""",
        [WORKING_SCHEMA_VERSION, json_dumps({"module": "datahub.working_duckdb"})],
    )

    connection.execute(
        """
CREATE TABLE IF NOT EXISTS raw_release_registry (
    source_id VARCHAR,
    release_id VARCHAR,
    modality VARCHAR,
    source_version VARCHAR,
    release_label VARCHAR,
    source_uri VARCHAR,
    license VARCHAR,
    access_notes VARCHAR,
    registered_at TIMESTAMP,
    metadata_json VARCHAR
);
"""
    )
    connection.execute(
        """
CREATE TABLE IF NOT EXISTS raw_file_inventory (
    source_id VARCHAR,
    release_id VARCHAR,
    modality VARCHAR,
    file_path VARCHAR,
    file_name VARCHAR,
    file_size_bytes BIGINT,
    sha256 VARCHAR,
    columns_json VARCHAR,
    schema_fingerprint VARCHAR,
    detected_delimiter VARCHAR,
    registered_at TIMESTAMP
);
"""
    )
    connection.execute(
        """
CREATE TABLE IF NOT EXISTS schema_drift_reports (
    source_id VARCHAR,
    release_id VARCHAR,
    modality VARCHAR,
    file_path VARCHAR,
    status VARCHAR,
    message VARCHAR,
    expected_columns_json VARCHAR,
    detected_columns_json VARCHAR,
    added_columns_json VARCHAR,
    removed_columns_json VARCHAR,
    created_at TIMESTAMP
);
"""
    )
    connection.execute(
        f"""
CREATE TABLE IF NOT EXISTS {SOURCE_NORMALIZED_ASSOCIATION_TABLE} (
    source_id VARCHAR,
    release_id VARCHAR,
    source_row_id VARCHAR,
    source_file VARCHAR,
    dataset_type VARCHAR,
    source_name VARCHAR,
    marker_id VARCHAR,
    study_id VARCHAR,
    study VARCHAR,
    pmid VARCHAR,
    study_genome_build VARCHAR,
    variant_id VARCHAR,
    gene_id VARCHAR,
    phenotype VARCHAR,
    p_value DOUBLE,
    functional_class VARCHAR,
    variation_type VARCHAR,
    clinical_significance VARCHAR,
    most_severe_consequence VARCHAR,
    allele_string VARCHAR,
    protein_start VARCHAR,
    protein_end VARCHAR,
    ancestry_data_json VARCHAR,
    raw_payload_json VARCHAR,
    metadata_json VARCHAR,
    loaded_at TIMESTAMP
);
"""
    )
    connection.execute(
        f"""
CREATE TABLE IF NOT EXISTS {ANALYSIS_READY_ASSOCIATION_TABLE} (
    dataset_id VARCHAR,
    dataset_type VARCHAR,
    source VARCHAR,
    source_id VARCHAR,
    release_id VARCHAR,
    source_row_id VARCHAR,
    source_file VARCHAR,
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    variant_id VARCHAR,
    phenotype VARCHAR,
    phenotype_path_json VARCHAR,
    disease_category VARCHAR,
    variation_type VARCHAR,
    clinical_significance VARCHAR,
    most_severe_consequence VARCHAR,
    p_value DOUBLE,
    ancestry VARCHAR,
    ancestry_af DOUBLE,
    ancestry_source_code VARCHAR,
    ancestry_source_label VARCHAR,
    phenotype_key VARCHAR,
    metadata_json VARCHAR,
    ingested_at TIMESTAMP
);
"""
    )
    _create_best_effort_indexes(connection)


def _create_best_effort_indexes(connection: Any) -> None:
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_raw_release ON raw_release_registry (source_id, release_id, modality)",
        "CREATE INDEX IF NOT EXISTS idx_raw_file_inventory ON raw_file_inventory (source_id, release_id, modality)",
        "CREATE INDEX IF NOT EXISTS idx_schema_drift ON schema_drift_reports (source_id, release_id, modality, status)",
        f"CREATE INDEX IF NOT EXISTS idx_source_norm_assoc ON {SOURCE_NORMALIZED_ASSOCIATION_TABLE} (source_id, release_id, dataset_type, gene_id)",
        f"CREATE INDEX IF NOT EXISTS idx_analysis_ready_assoc ON {ANALYSIS_READY_ASSOCIATION_TABLE} (dataset_type, gene_id_normalized, variant_id)",
    ]
    for statement in statements:
        try:
            connection.execute(statement)
        except Exception:
            # Older DuckDB builds may not support all index forms. The schema is
            # still usable without indexes, so do not fail lifecycle setup.
            continue


def expand_input_paths(paths: Iterable[str]) -> list[Path]:
    """Resolve files, directories, and globs into a sorted file list."""

    import glob

    resolved: list[Path] = []
    for raw in paths:
        candidate = Path(raw)
        if candidate.is_dir():
            resolved.extend(sorted(path for path in candidate.iterdir() if path.is_file()))
            continue
        if candidate.exists():
            resolved.append(candidate)
            continue
        resolved.extend(sorted(Path(path) for path in glob.glob(raw) if Path(path).is_file()))
    return sorted(dict.fromkeys(resolved))


def register_raw_release(
    connection: Any,
    *,
    source_id: str,
    release_id: str,
    modality: str,
    input_paths: Sequence[str | Path],
    expected_columns: Sequence[str] | None = None,
    expected_column_groups: Sequence[Sequence[str]] | None = None,
    source_version: str = "",
    release_label: str = "",
    source_uri: str = "",
    license: str = "",
    access_notes: str = "",
    metadata: dict[str, Any] | None = None,
    compute_checksum: bool = True,
    delimiter: str | None = None,
) -> list[RawFileRegistration]:
    """Register a raw source release and inventory its files/schemas."""

    ensure_working_schema(connection)
    files = expand_input_paths(str(path) for path in input_paths)
    if not files:
        raise FileNotFoundError("No raw files matched the provided input paths.")

    connection.execute(
        """
DELETE FROM raw_release_registry
WHERE source_id = ? AND release_id = ? AND modality = ?;
""",
        [source_id, release_id, modality],
    )
    connection.execute(
        """
INSERT INTO raw_release_registry VALUES (?, ?, ?, ?, ?, ?, ?, ?, now(), ?);
""",
        [
            source_id,
            release_id,
            modality,
            source_version,
            release_label,
            source_uri,
            license,
            access_notes,
            json_dumps(metadata or {}),
        ],
    )
    connection.execute(
        """
DELETE FROM raw_file_inventory
WHERE source_id = ? AND release_id = ? AND modality = ?;
""",
        [source_id, release_id, modality],
    )
    connection.execute(
        """
DELETE FROM schema_drift_reports
WHERE source_id = ? AND release_id = ? AND modality = ?;
""",
        [source_id, release_id, modality],
    )

    registrations: list[RawFileRegistration] = []
    for path in files:
        schema = inspect_csv_schema(path, delimiter=delimiter)
        if expected_column_groups:
            drift = compare_schema_candidate_groups(
                detected_columns=schema.columns,
                expected_column_groups=expected_column_groups,
            )
        else:
            drift = compare_schema(
                detected_columns=schema.columns,
                expected_columns=expected_columns,
            )
        digest = file_sha256(path) if compute_checksum else None
        stat = path.stat()

        connection.execute(
            """
INSERT INTO raw_file_inventory VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now());
""",
            [
                source_id,
                release_id,
                modality,
                str(path),
                path.name,
                stat.st_size,
                digest,
                json_dumps(list(schema.columns)),
                schema.fingerprint,
                schema.delimiter,
            ],
        )
        connection.execute(
            """
INSERT INTO schema_drift_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now());
""",
            [
                source_id,
                release_id,
                modality,
                str(path),
                drift.status,
                drift.message,
                json_dumps(list(drift.expected_columns)),
                json_dumps(list(drift.detected_columns)),
                json_dumps(list(drift.added_columns)),
                json_dumps(list(drift.removed_columns)),
            ],
        )
        registrations.append(
            RawFileRegistration(
                file_path=path,
                schema=schema,
                sha256=digest,
                drift=drift,
            )
        )
    return registrations


def load_source_normalized_association_csv(
    connection: Any,
    *,
    prepared_csv: str | Path,
    source_id: str,
    release_id: str,
    replace_file: bool = True,
) -> int:
    """Load a prepared association CSV into ``source_normalized_association``."""

    ensure_working_schema(connection)
    path = Path(prepared_csv)
    if replace_file:
        connection.execute(
            f"DELETE FROM {SOURCE_NORMALIZED_ASSOCIATION_TABLE} WHERE source_file = ? AND source_id = ? AND release_id = ?",
            [str(path), source_id, release_id],
        )

    before = connection.execute(
        f"SELECT COUNT(*) FROM {SOURCE_NORMALIZED_ASSOCIATION_TABLE}"
    ).fetchone()[0]
    connection.execute(
        f"""
INSERT INTO {SOURCE_NORMALIZED_ASSOCIATION_TABLE}
SELECT
    ? AS source_id,
    ? AS release_id,
    concat(?, ':', row_number() OVER ()) AS source_row_id,
    ? AS source_file,
    upper(trim(coalesce(dataset_type, ''))) AS dataset_type,
    trim(coalesce(source_name, '')) AS source_name,
    trim(coalesce(marker_id, '')) AS marker_id,
    trim(coalesce(study_id, '')) AS study_id,
    trim(coalesce(study, '')) AS study,
    trim(coalesce(pmid, '')) AS pmid,
    trim(coalesce(study_genome_build, '')) AS study_genome_build,
    trim(coalesce(rsid, marker_id, '')) AS variant_id,
    trim(coalesce(gene, '')) AS gene_id,
    trim(coalesce(phenotype, '')) AS phenotype,
    TRY_CAST(pval AS DOUBLE) AS p_value,
    trim(coalesce(functional_class, '')) AS functional_class,
    trim(coalesce(var_class, '')) AS variation_type,
    trim(coalesce(clinical_significance, '')) AS clinical_significance,
    trim(coalesce(most_severe_consequence, '')) AS most_severe_consequence,
    trim(coalesce(allele_string, '')) AS allele_string,
    trim(coalesce(protein_start, '')) AS protein_start,
    trim(coalesce(protein_end, '')) AS protein_end,
    trim(coalesce(ancestry_data, '{{}}')) AS ancestry_data_json,
    '{{}}' AS raw_payload_json,
    '{{}}' AS metadata_json,
    now() AS loaded_at
FROM read_csv_auto(?, header=true, all_varchar=true, compression='auto');
""",
        [source_id, release_id, f"{source_id}:{release_id}", str(path), str(path)],
    )
    after = connection.execute(
        f"SELECT COUNT(*) FROM {SOURCE_NORMALIZED_ASSOCIATION_TABLE}"
    ).fetchone()[0]
    return int(after - before)


def materialize_analysis_ready_association_from_points(
    connection: Any,
    *,
    source_table: str,
    target_table: str = ANALYSIS_READY_ASSOCIATION_TABLE,
    replace: bool = False,
) -> int:
    """Materialize an analysis-ready association table from the current points table."""

    ensure_working_schema(connection)
    source = quote_identifier(source_table)
    target = quote_identifier(target_table)
    if replace:
        _drop_relation_if_exists(connection, target)
    connection.execute(
        f"""
CREATE TABLE IF NOT EXISTS {target} AS
SELECT
    dataset_id,
    dataset_type,
    source,
    source AS source_id,
    '' AS release_id,
    '' AS source_row_id,
    source_file,
    gene_id,
    upper(gene_id) AS gene_id_normalized,
    variant_id,
    phenotype,
    '[]' AS phenotype_path_json,
    disease_category,
    variation_type,
    clinical_significance,
    most_severe_consequence,
    p_value,
    ancestry,
    ancestry_af,
    ancestry_source_code,
    ancestry_source_label,
    phenotype_key,
    '{{}}' AS metadata_json,
    ingested_at
FROM {source}
WHERE false;
"""
    )
    if replace:
        connection.execute(f"DELETE FROM {target}")
    before = connection.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    connection.execute(
        f"""
INSERT INTO {target}
SELECT
    dataset_id,
    dataset_type,
    source,
    source AS source_id,
    '' AS release_id,
    '' AS source_row_id,
    source_file,
    gene_id,
    upper(gene_id) AS gene_id_normalized,
    variant_id,
    phenotype,
    '[]' AS phenotype_path_json,
    disease_category,
    variation_type,
    clinical_significance,
    most_severe_consequence,
    p_value,
    ancestry,
    ancestry_af,
    ancestry_source_code,
    ancestry_source_label,
    phenotype_key,
    '{{}}' AS metadata_json,
    ingested_at
FROM {source};
"""
    )
    after = connection.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    return int(after - before)


def _drop_relation_if_exists(connection: Any, relation_name: str) -> None:
    """Drop a table/view without assuming which relation type currently exists."""

    for statement in (
        f"DROP TABLE IF EXISTS {relation_name}",
        f"DROP VIEW IF EXISTS {relation_name}",
    ):
        try:
            connection.execute(statement)
        except Exception:
            continue
