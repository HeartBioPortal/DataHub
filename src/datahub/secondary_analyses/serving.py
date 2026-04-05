"""Apply secondary-analysis artifacts to an existing serving DuckDB."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifacts import read_gene_payload_artifacts
from .base import SecondaryAnalysisManifest, SecondaryArtifactRow


def ensure_secondary_tables(connection: Any) -> None:
    connection.execute(
        """
CREATE TABLE IF NOT EXISTS expression_gene_payloads (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
    )
    connection.execute(
        """
CREATE TABLE IF NOT EXISTS sga_gene_payloads (
    gene_id VARCHAR,
    gene_id_normalized VARCHAR,
    payload_json VARCHAR,
    source_path VARCHAR
)
"""
    )
    connection.execute(
        """
CREATE TABLE IF NOT EXISTS secondary_analysis_metadata (
    analysis_id VARCHAR,
    analysis_version BIGINT,
    analysis_mode VARCHAR,
    built_at TIMESTAMP,
    artifact_root VARCHAR,
    row_count BIGINT,
    metadata_json VARCHAR
)
"""
    )


def _refresh_gene_catalog(connection: Any) -> None:
    connection.execute("DELETE FROM gene_catalog")
    connection.execute(
        """
INSERT INTO gene_catalog
SELECT
    coalesce(a.gene_id, o.gene_id, e.gene_id, s.gene_id) AS gene_id,
    coalesce(
        a.gene_id_normalized,
        o.gene_id_normalized,
        e.gene_id_normalized,
        s.gene_id_normalized
    ) AS gene_id_normalized,
    coalesce(a.has_cvd_association, false) OR coalesce(o.has_cvd_overall, false) AS has_cvd,
    coalesce(a.has_trait_association, false) OR coalesce(o.has_trait_overall, false) AS has_trait,
    coalesce(a.has_cvd_association, false) AS has_cvd_association,
    coalesce(a.has_trait_association, false) AS has_trait_association,
    coalesce(o.has_cvd_overall, false) AS has_cvd_overall,
    coalesce(o.has_trait_overall, false) AS has_trait_overall,
    coalesce(e.has_expression, false) AS has_expression,
    coalesce(s.has_sga, false) AS has_sga
FROM (
    SELECT
        gene_id,
        gene_id_normalized,
        bool_or(dataset_type = 'CVD') AS has_cvd_association,
        bool_or(dataset_type = 'TRAIT') AS has_trait_association
    FROM association_gene_payloads
    GROUP BY gene_id, gene_id_normalized
) a
FULL OUTER JOIN (
    SELECT
        gene_id,
        gene_id_normalized,
        bool_or(dataset_type = 'CVD') AS has_cvd_overall,
        bool_or(dataset_type = 'TRAIT') AS has_trait_overall
    FROM overall_gene_payloads
    GROUP BY gene_id, gene_id_normalized
) o
  ON a.gene_id_normalized = o.gene_id_normalized
FULL OUTER JOIN (
    SELECT
        gene_id,
        gene_id_normalized,
        true AS has_expression
    FROM expression_gene_payloads
    GROUP BY gene_id, gene_id_normalized
) e
  ON coalesce(a.gene_id_normalized, o.gene_id_normalized) = e.gene_id_normalized
FULL OUTER JOIN (
    SELECT
        gene_id,
        gene_id_normalized,
        true AS has_sga
    FROM sga_gene_payloads
    GROUP BY gene_id, gene_id_normalized
) s
  ON coalesce(a.gene_id_normalized, o.gene_id_normalized, e.gene_id_normalized) = s.gene_id_normalized
"""
    )


def _metadata_json(artifact_root: Path, manifest: SecondaryAnalysisManifest) -> str:
    path = artifact_root.parent / "metadata.json"
    if not path.exists():
        return "{}"
    try:
        return json.dumps(json.loads(path.read_text()), separators=(",", ":"))
    except Exception:
        return "{}"


def _table_name_for_analysis(analysis_id: str) -> str:
    if analysis_id == "expression":
        return "expression_gene_payloads"
    if analysis_id == "sga":
        return "sga_gene_payloads"
    raise ValueError(f"Unsupported secondary analysis table mapping: {analysis_id}")


def _insert_rows(
    connection: Any,
    *,
    table_name: str,
    rows: list[SecondaryArtifactRow],
    batch_size: int,
) -> None:
    if not rows:
        return
    sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)"
    for start in range(0, len(rows), max(batch_size, 1)):
        batch = rows[start:start + max(batch_size, 1)]
        connection.executemany(
            sql,
            [
                (row.gene_id, row.gene_id_normalized, row.payload_json, row.source_path)
                for row in batch
            ],
        )


def apply_secondary_analysis_artifacts(
    connection: Any,
    *,
    input_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    batch_size: int = 500,
) -> int:
    ensure_secondary_tables(connection)
    rows = read_gene_payload_artifacts(
        input_root=input_root,
        manifest=manifest,
    )
    artifact_root = Path(input_root) / "final" / manifest.artifact_subdir / "genes"
    table_name = _table_name_for_analysis(manifest.analysis_id)
    connection.execute(f"DELETE FROM {table_name}")
    _insert_rows(
        connection,
        table_name=table_name,
        rows=rows,
        batch_size=batch_size,
    )
    connection.execute(
        "DELETE FROM secondary_analysis_metadata WHERE analysis_id = ?",
        [manifest.analysis_id],
    )
    connection.execute(
        """
INSERT INTO secondary_analysis_metadata
VALUES (?, ?, ?, current_timestamp, ?, ?, ?)
""",
        [
            manifest.analysis_id,
            manifest.version,
            manifest.mode,
            str(artifact_root),
            len(rows),
            _metadata_json(artifact_root, manifest),
        ],
    )
    _refresh_gene_catalog(connection)
    return len(rows)
