"""Tests for evidence-v2 source-summary default rollups."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb

from datahub.association_evidence_v2.source_summary_rollup import (
    BASE_TABLE,
    PHENOTYPE_TABLE,
    SourceSummaryRollupBuilder,
)


def test_rollup_preserves_variant_and_exact_phenotype_counts(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    source = serving / "tables" / "unavailable_provider_summaries_by_gene" / "gene_bucket=00" / "gene_key=fixture"
    source.mkdir(parents=True)
    connection = duckdb.connect()
    connection.execute(
        """COPY (SELECT * FROM (VALUES
          ('mvp','CVD','HMGCR','rs1','[\"vascular\",\"hypertension\"]','vascular > hypertension','1e-6','unavailable','reason','[\"effect_allele\"]','variant_index/CVD/HMGCR.json.gz','gene_scoped_on_demand'),
          ('mvp','CVD','HMGCR','rs1','[\"vascular\",\"cad\"]','vascular > cad','1e-9','unavailable','reason','[\"effect_allele\"]','variant_index/CVD/HMGCR.json.gz','gene_scoped_on_demand'),
          ('mvp','CVD','HMGCR','rs2','[\"vascular\",\"hypertension\"]','vascular > hypertension','0.2','unavailable','reason','[\"effect_allele\"]','variant_index/CVD/HMGCR.json.gz','gene_scoped_on_demand')
        ) t(source,dataset_type,gene_id,variant_id,phenotype_path_json,phenotype_path_key,reported_p_value,provider_detail_status,provider_detail_reason,missing_fields_json,retained_source_summary_artifact,publication_mode))
        TO ? (FORMAT PARQUET)""",
        [str(source / "part.parquet")],
    )
    connection.close()
    manifest = {
        "contract": "association_evidence_v2_partitioned_serving",
        "schema_version": "2.7.0-rc1",
        "tables": {"unavailable_provider_summaries_by_gene": {
            "contract": "retained_compact_source_summary_index_v1",
            "files": 1,
            "source_artifacts": 1,
        }},
    }
    manifest_path = serving / "serving-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    checksum = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    (serving / "serving-manifest.json.sha256").write_text(f"{checksum}  serving-manifest.json\n")
    builder = SourceSummaryRollupBuilder(serving_root=serving)
    result = builder._rollup_bucket("00")
    assert result["base_rows"] == 2
    assert result["phenotype_rows"] == 2
    check = duckdb.connect()
    base = check.execute(
        "SELECT variant_id,p_value,retained_source_summary_count FROM read_parquet(?) ORDER BY variant_id",
        [str(serving / "tables" / BASE_TABLE / "gene_bucket=00" / "gene_key=*" / "*.parquet")],
    ).fetchall()
    phenotypes = check.execute(
        "SELECT phenotype_path_key,distinct_variant_count,variant_ids_json FROM read_parquet(?) ORDER BY phenotype_path_key",
        [str(serving / "tables" / PHENOTYPE_TABLE / "gene_bucket=00" / "gene_key=*" / "*.parquet")],
    ).fetchall()
    check.close()
    assert base == [("rs1", 1e-9, 2), ("rs2", 0.2, 1)]
    assert phenotypes == [
        ("vascular > cad", 1, '["rs1"]'),
        ("vascular > hypertension", 2, '["rs1","rs2"]'),
    ]
