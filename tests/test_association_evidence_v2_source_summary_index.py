from __future__ import annotations

import gzip
import json
from pathlib import Path

import duckdb

from datahub.association_evidence_v2.source_summary_index import SourceSummaryIndexBuilder


def test_index_is_gene_keyed_and_does_not_create_provider_rows(tmp_path: Path) -> None:
    artifact = tmp_path / "artifacts/variant_index/CVD/TTN.json.gz"
    artifact.parent.mkdir(parents=True)
    payload = {
        "variant_id": "rsMVP",
        "phenotype": "cardiomyopathy",
        "phenotype_path": ["cardiomyopathies", "cardiomyopathy"],
        "source": "million_veteran_program",
        "sources": ["million_veteran_program"],
        "p_value": 1e-9,
    }
    with gzip.open(artifact, "wt", encoding="utf-8") as stream:
        json.dump([payload, payload, {**payload, "sources": ["legacy_cvd_raw"]}], stream)
    stat = artifact.stat()
    source_db = tmp_path / "source.duckdb"
    connection = duckdb.connect(str(source_db))
    connection.execute(
        """CREATE TABLE source_summary_artifacts (
          source_summary_artifact_id VARCHAR, source VARCHAR, dataset_type VARCHAR,
          gene_id VARCHAR, logical_artifact VARCHAR, file_format VARCHAR,
          size_bytes UBIGINT, modified_at_ns UBIGINT,
          provider_detail_status VARCHAR, provider_detail_reason VARCHAR,
          missing_fields_json JSON, content_contract VARCHAR, publication_mode VARCHAR
        )"""
    )
    connection.execute(
        "INSERT INTO source_summary_artifacts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            "artifact-1", "million_veteran_program", "CVD", "TTN",
            "variant_index/CVD/TTN.json.gz", "json.gz", stat.st_size,
            stat.st_mtime_ns, "unavailable", "provider rows unavailable",
            json.dumps(["effect_allele", "sample_size"]),
            "retained_compact_variant_index_v1", "gene_scoped_on_demand",
        ],
    )
    connection.close()
    serving_root = tmp_path / "serving"
    serving_root.mkdir()
    (serving_root / "serving-manifest.json").write_text(
        json.dumps({"contract": "association_evidence_v2_partitioned_serving", "tables": {}})
    )

    result = SourceSummaryIndexBuilder(
        source_db=source_db,
        artifact_root=tmp_path / "artifacts",
        serving_root=serving_root,
        workers=1,
        progress_interval=1,
    ).build()

    assert result["selected_source_rows"] == 2
    assert result["unique_summary_rows"] == 1
    manifest = json.loads((serving_root / "serving-manifest.json").read_text())
    table = manifest["tables"]["unavailable_provider_summaries_by_gene"]
    assert table["provider_detail_status"] == "unavailable"
    rows = duckdb.connect().execute(
        f"SELECT source, variant_id, provider_detail_status, "
        f"retained_source_summary_artifact FROM read_parquet("
        f"'{serving_root / 'tables/unavailable_provider_summaries_by_gene/**/*.parquet'}')"
    ).fetchall()
    assert rows == [
        (
            "million_veteran_program", "rsMVP", "unavailable",
            "variant_index/CVD/TTN.json.gz",
        )
    ]
