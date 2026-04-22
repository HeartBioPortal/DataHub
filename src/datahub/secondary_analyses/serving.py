"""Apply secondary-analysis artifacts to an existing serving DuckDB."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from .artifacts import (
    list_gene_payload_artifact_paths,
    read_gene_payload_artifact_path,
)
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
        part_paths = sorted(artifact_root.parent.glob("metadata.part*of*.json"))
        if not part_paths:
            return "{}"
        parts: list[dict[str, object]] = []
        for part_path in part_paths:
            try:
                parts.append(json.loads(part_path.read_text()))
            except Exception:
                continue
        return json.dumps(
            {
                "analysis_id": manifest.analysis_id,
                "version": manifest.version,
                "partition_metadata_count": len(parts),
                "partitions": parts,
            },
            separators=(",", ":"),
        )
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


def _progress_bar(done: int, total: int, *, width: int = 24) -> str:
    if total <= 0:
        return "[" + "#" * width + "]"
    filled = min(width, int(width * done / total))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _log_apply_progress(
    logger: logging.Logger | None,
    *,
    analysis_id: str,
    processed: int,
    total: int,
    inserted: int,
) -> None:
    if logger is None:
        return
    percent = 100.0 if total <= 0 else (processed / total) * 100.0
    logger.info(
        "Secondary apply progress: analysis=%s files=%d/%d inserted=%d %.1f%% %s",
        analysis_id,
        processed,
        total,
        inserted,
        percent,
        _progress_bar(processed, total),
    )


def _insert_secondary_artifact_paths(
    connection: Any,
    *,
    table_name: str,
    manifest: SecondaryAnalysisManifest,
    payload_paths: list[Path],
    batch_size: int,
    progress_interval: int,
    logger: logging.Logger | None,
) -> int:
    if not payload_paths:
        _log_apply_progress(
            logger,
            analysis_id=manifest.analysis_id,
            processed=0,
            total=0,
            inserted=0,
        )
        return 0

    sql = f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)"
    batch: list[SecondaryArtifactRow] = []
    processed = 0
    inserted = 0
    effective_batch_size = max(batch_size, 1)
    effective_progress_interval = max(progress_interval, 1)

    for payload_path in payload_paths:
        batch.append(read_gene_payload_artifact_path(payload_path))
        processed += 1

        if len(batch) >= effective_batch_size:
            connection.executemany(
                sql,
                [
                    (
                        row.gene_id,
                        row.gene_id_normalized,
                        row.payload_json,
                        row.source_path,
                    )
                    for row in batch
                ],
            )
            inserted += len(batch)
            batch = []

        if processed % effective_progress_interval == 0:
            _log_apply_progress(
                logger,
                analysis_id=manifest.analysis_id,
                processed=processed,
                total=len(payload_paths),
                inserted=inserted,
            )

    if batch:
        connection.executemany(
            sql,
            [
                (row.gene_id, row.gene_id_normalized, row.payload_json, row.source_path)
                for row in batch
            ],
        )
        inserted += len(batch)

    _log_apply_progress(
        logger,
        analysis_id=manifest.analysis_id,
        processed=processed,
        total=len(payload_paths),
        inserted=inserted,
    )

    return inserted


def apply_secondary_analysis_artifacts(
    connection: Any,
    *,
    input_root: str | Path,
    manifest: SecondaryAnalysisManifest,
    batch_size: int = 500,
    logger: logging.Logger | None = None,
    progress_interval: int = 1000,
) -> int:
    ensure_secondary_tables(connection)
    payload_paths = list_gene_payload_artifact_paths(
        input_root=input_root,
        manifest=manifest,
    )
    artifact_root = Path(input_root) / "final" / manifest.artifact_subdir / "genes"
    table_name = _table_name_for_analysis(manifest.analysis_id)
    if logger is not None:
        logger.info(
            "Secondary apply start: analysis=%s table=%s artifact_root=%s files=%d batch_size=%d",
            manifest.analysis_id,
            table_name,
            artifact_root,
            len(payload_paths),
            max(batch_size, 1),
        )

    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DELETE FROM {table_name}")
        inserted = _insert_secondary_artifact_paths(
            connection,
            table_name=table_name,
            manifest=manifest,
            payload_paths=payload_paths,
            batch_size=batch_size,
            progress_interval=progress_interval,
            logger=logger,
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
                inserted,
                _metadata_json(artifact_root, manifest),
            ],
        )
        if logger is not None:
            logger.info(
                "Secondary apply catalog refresh start: analysis=%s table=gene_catalog",
                manifest.analysis_id,
            )
        _refresh_gene_catalog(connection)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        if logger is not None:
            logger.exception(
                "Secondary apply failed and was rolled back: analysis=%s",
                manifest.analysis_id,
            )
        raise

    if logger is not None:
        logger.info(
            "Secondary apply complete: analysis=%s rows=%d table=%s",
            manifest.analysis_id,
            inserted,
            table_name,
        )
    return inserted
