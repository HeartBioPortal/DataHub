"""Contract tests for the resumable association evidence v2 release audit."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import logging
from pathlib import Path

import duckdb


def _module():
    path = Path(__file__).parents[1] / "scripts" / "dataset_specific_scripts" / "unified" / "audit_association_evidence_v2_release.py"
    spec = importlib.util.spec_from_file_location("evidence_v2_audit", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_count_intermediate_round_trip() -> None:
    module = _module()
    category = {("HMGCR", "CVD", "vc", "snv"): 379}
    totals = {("HMGCR", "CVD"): {"v2_distinct_variants": 379, "association_record_count": 400}}
    encoded = module.encode_count_bundle(category, totals)
    observed_category, observed_totals = module.decode_count_bundle(encoded, rich_totals=True)
    assert observed_category == category
    assert observed_totals == totals


def test_checkpoint_is_configuration_bound(tmp_path: Path) -> None:
    module = _module()
    checkpoint = tmp_path / "checkpoint.json"
    module.save_checkpoint(checkpoint, "config-a", "v2_counts_complete", {"rows": 3})
    assert module.load_checkpoint(checkpoint, "config-a")["details"]["rows"] == 3
    try:
        module.load_checkpoint(checkpoint, "config-b")
    except RuntimeError as exc:
        assert "does not match" in str(exc)
    else:
        raise AssertionError("mismatched audit configuration was accepted")


def test_category_difference_reason_distinguishes_missing_gene_and_new_term() -> None:
    module = _module()
    totals = {("HMGCR", "CVD"): {"v2_distinct_variants": 379}}
    missing = module.category_difference_reason(
        gene="5S_rRNA", dataset_type="CVD", dimension="vc",
        v1_count=40, v2_count=0, v2_totals=totals,
    )
    assert "No v2 gene/dataset summary" in missing
    added = module.category_difference_reason(
        gene="HMGCR", dataset_type="CVD", dimension="msc",
        v1_count=0, v2_count=12, v2_totals=totals,
    )
    assert "not exposed by the v1 representative" in added


def test_streaming_array_and_resumable_membership_registry(tmp_path: Path) -> None:
    module = _module()
    artifact_root = tmp_path / "artifacts"
    artifact = artifact_root / "variant_index" / "TRAIT" / "HMGCR.json.gz"
    artifact.parent.mkdir(parents=True)
    with gzip.open(artifact, "wt", encoding="utf-8") as stream:
        json.dump([
            {
                "variant_id": "rs1",
                "source": "million_veteran_program",
                "sources": ["million_veteran_program"],
            },
            {
                "variant_id": "rs1",
                "source": "million_veteran_program",
                "sources": ["million_veteran_program"],
            },
            {
                "variant_id": "rs2",
                "source": "other",
                "sources": ["other"],
            },
        ], stream)
    assert [row["variant_id"] for row in module.iter_json_array(artifact, chunk_size=7)] == [
        "rs1", "rs1", "rs2"
    ]

    registry_path = tmp_path / "registry.duckdb"
    registry = duckdb.connect(str(registry_path))
    registry.execute(
        """CREATE TABLE source_summary_artifacts (
        source VARCHAR, gene_id VARCHAR, dataset_type VARCHAR,
        logical_artifact VARCHAR, size_bytes UBIGINT, modified_at_ns UBIGINT,
        provider_detail_status VARCHAR, publication_mode VARCHAR);
        CREATE TABLE variant_phenotype_summaries (
        gene_id VARCHAR, dataset_type VARCHAR, variant_id VARCHAR,
        source_observation_count UBIGINT, association_record_count UBIGINT,
        provider_association_record_count UBIGINT);
        CREATE TABLE variants (variant_id VARCHAR, variation_type VARCHAR);
        CREATE TABLE consequence_annotations (
        variant_id VARCHAR, gene_id VARCHAR, consequence VARCHAR);
        CREATE TABLE clinical_assertions (
        variant_id VARCHAR, clinical_significance VARCHAR);
        INSERT INTO variants VALUES ('rs1', 'SNV');
        INSERT INTO consequence_annotations VALUES ('rs1', 'HMGCR', 'missense_variant');
        INSERT INTO clinical_assertions VALUES ('rs1', 'Pathogenic');"""
    )
    stat = artifact.stat()
    registry.execute(
        "INSERT INTO source_summary_artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            "million_veteran_program", "HMGCR", "TRAIT",
            "variant_index/TRAIT/HMGCR.json.gz", stat.st_size, stat.st_mtime_ns,
            "unavailable", "gene_scoped_on_demand",
        ],
    )
    runtime_path = tmp_path / "runtime.duckdb"
    logger = logging.getLogger("test.association_evidence_v2.audit")
    first = module.materialize_compact_membership(
        registry,
        artifact_root=artifact_root,
        runtime_db_path=runtime_path,
        logger=logger,
        progress_interval=1,
        batch_rows=1,
    )
    second = module.materialize_compact_membership(
        registry,
        artifact_root=artifact_root,
        runtime_db_path=runtime_path,
        logger=logger,
        progress_interval=1,
        batch_rows=1,
    )
    registry.close()
    read_only_registry = duckdb.connect(str(registry_path), read_only=True)
    category, totals = module.v2_counts(
        read_only_registry,
        runtime_db_path=runtime_path,
        v2_db_path=registry_path,
    )
    read_only_registry.close()
    assert first == second
    assert first["artifacts"] == 1
    assert first["selected_source_rows"] == 2
    assert first["distinct_gene_dataset_variants"] == 1
    assert category[("HMGCR", "TRAIT", "vc", "snv")] == 1
    assert category[("HMGCR", "TRAIT", "msc", "missense variant")] == 1
    assert category[("HMGCR", "TRAIT", "cs", "pathogenic")] == 1
    assert totals[("HMGCR", "TRAIT")]["v2_distinct_variants"] == 1
    assert totals[("HMGCR", "TRAIT")]["association_record_count"] == 0


def test_artifact_reader_preserves_selected_count_and_deduplicates_membership(
    tmp_path: Path,
) -> None:
    module = _module()
    artifact = tmp_path / "HMGCR.json.gz"
    with gzip.open(artifact, "wt", encoding="utf-8") as stream:
        json.dump([
            {"variant_id": "rs1", "sources": ["million_veteran_program"]},
            {"variant_id": "rs1", "sources": ["million_veteran_program"]},
            {"variant_id": "rs2", "source": "other"},
        ], stream)
    stat = artifact.stat()
    result = module._read_membership_artifact((
        "million_veteran_program", "HMGCR", "TRAIT", "logical/HMGCR.json.gz",
        stat.st_size, stat.st_mtime_ns, str(artifact),
    ))
    assert result["selected_rows"] == 2
    assert result["membership_rows"] == [("HMGCR", "TRAIT", "rs1")]



def test_indexed_membership_reuses_completed_serving_contract(tmp_path: Path) -> None:
    module = _module()
    serving = tmp_path / "serving"
    table_root = serving / "tables" / "source_summary_associations_by_gene"
    connection = duckdb.connect()
    manifest_rows = []
    for bucket in range(256):
        target = table_root / f"gene_bucket={bucket:02x}" / "gene_key=fixture"
        target.mkdir(parents=True, exist_ok=True)
        connection.execute(
            """COPY (SELECT 'source-summary:1'::VARCHAR AS source_summary_id,
                    'million_veteran_program'::VARCHAR AS source,
                    'CVD'::VARCHAR AS dataset_type,
                    'HMGCR'::VARCHAR AS gene_id,
                    'rs1'::VARCHAR AS variant_id)
               TO ? (FORMAT PARQUET)""",
            [str(target / f"part-{bucket:02x}.parquet")],
        )
        manifest_rows.append(json.dumps({
            "logical_artifact": f"variant_index/CVD/HMGCR-{bucket:02x}.json.gz",
            "source": "million_veteran_program",
            "gene_id": "HMGCR",
            "dataset_type": "CVD",
            "selected_source_rows": 1,
        }))
    connection.close()
    (serving / "source-summary-index-files.jsonl").write_text(
        "\n".join(manifest_rows) + "\n"
    )
    manifest = {
        "schema_version": "2.9.0-rc2",
        "tables": {"source_summary_associations_by_gene": {
            "path": "tables/source_summary_associations_by_gene",
            "contract": "source_summary_association_index_v2",
            "files": 256,
            "source_artifacts": 256,
            "selected_source_rows": 256,
        }},
    }
    manifest_path = serving / "serving-manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    checksum = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    (serving / "serving-manifest.json.sha256").write_text(
        f"{checksum}  serving-manifest.json\n"
    )
    for schema_version in ("2.9.0-rc2", "2.10.0-rc2"):
        manifest["schema_version"] = schema_version
        if schema_version == "2.10.0-rc2":
            manifest["tables"]["source_summary_association_base_by_gene"] = {
                "path": "tables/source_summary_associations_by_gene",
                "contract": "source_summary_association_rollup_v2",
                "files": 256,
                "rows": 256,
            }
            (serving / "source-summary-rollup-files.jsonl").write_text("{}\n")
        else:
            manifest["tables"].pop(
                "source_summary_association_base_by_gene", None
            )
        manifest_path.write_text(json.dumps(manifest))
        checksum = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        (serving / "serving-manifest.json.sha256").write_text(
            f"{checksum}  serving-manifest.json\n"
        )
        stats = module.materialize_indexed_compact_membership(
            serving_root=serving,
            runtime_db_path=tmp_path / f"runtime-indexed-{schema_version}.duckdb",
            logger=logging.getLogger("test.association_evidence_v2.indexed-audit"),
        )
        assert stats == {
            "artifacts": 256,
            "distinct_gene_dataset_variants": 1,
            "selected_source_rows": 256,
            "indexed_gene_buckets": 256,
        }
