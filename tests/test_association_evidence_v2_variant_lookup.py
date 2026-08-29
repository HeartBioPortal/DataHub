from __future__ import annotations

import importlib.util
import json
import sys
from argparse import Namespace
from pathlib import Path

import duckdb


def load_builder():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "build_association_evidence_v2_variant_lookup.py"
    )
    spec = importlib.util.spec_from_file_location("build_v2_variant_lookup", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_duckdb_builder():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "build_association_evidence_v2_variant_lookup_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("build_v2_variant_lookup_duckdb", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def create_source(path: Path) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(
        """
        CREATE TABLE variants (
            variant_id VARCHAR,
            normalized_allele_contexts_json JSON,
            coordinate_contexts_json JSON,
            source_reported_variation_types_json JSON,
            variation_type VARCHAR,
            variation_type_status VARCHAR,
            variation_type_method VARCHAR,
            variation_type_reason VARCHAR,
            ref_allele_status VARCHAR,
            alt_allele_status VARCHAR,
            effect_allele_status VARCHAR,
            multiallelic_observed BOOLEAN,
            build_or_position_conflict BOOLEAN
        )
        """
    )
    connection.executemany(
        "INSERT INTO variants VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("rs1", "[]", "[]", "[]", "SNV", "derived", "test", None,
             "unresolved", "unresolved", "unresolved", False, False),
            ("rs2", "[]", "[]", "[]", None, "unresolved", "test", "missing",
             "unresolved", "unresolved", "unresolved", False, False),
        ],
    )
    connection.close()


def arguments(source: Path, output: Path) -> Namespace:
    return Namespace(
        source_db=source,
        source_sha256="source-digest",
        output_root=output,
        bucket_characters=3,
        threads=1,
        memory_limit="1GB",
        progress_interval=1,
        smoke_limit=None,
        reset=False,
        verbose=False,
    )


def test_variant_lookup_is_partitioned_checksummed_and_resumable(tmp_path: Path) -> None:
    module = load_builder()
    source = tmp_path / "source.duckdb"
    output = tmp_path / "lookup"
    create_source(source)

    first = module.build(arguments(source, output))
    second = module.build(arguments(source, output))

    assert first == second
    assert first["tables"]["variants"]["rows"] == 2
    assert first["tables"]["variants"]["files"] == 2
    assert len((output / "checksums.sha256").read_text().splitlines()) == 3
    checkpoint = json.loads((output / "variant-lookup-checkpoint.json").read_text())
    assert checkpoint["complete"] is True

    rows = duckdb.connect().execute(
        "SELECT variant_id, variation_type FROM read_parquet(?, hive_partitioning=true)",
        [str(output / "tables" / "variants" / "**" / "*.parquet")],
    ).fetchall()
    assert sorted(rows) == [("rs1", "SNV"), ("rs2", None)]


def test_variant_lookup_rejects_checkpoint_configuration_change(tmp_path: Path) -> None:
    module = load_builder()
    source = tmp_path / "source.duckdb"
    output = tmp_path / "lookup"
    create_source(source)
    module.build(arguments(source, output))

    changed = arguments(source, output)
    changed.source_sha256 = "different"
    try:
        module.build(changed)
    except RuntimeError as error:
        assert "checkpoint configuration" in str(error)
    else:
        raise AssertionError("Configuration mismatch was accepted")


def test_indexed_duckdb_lookup_is_unique_checksummed_and_resumable(tmp_path: Path) -> None:
    partition_builder = load_builder()
    duckdb_builder = load_duckdb_builder()
    source = tmp_path / "source.duckdb"
    parquet_output = tmp_path / "lookup-parquet"
    create_source(source)
    partition_builder.build(arguments(source, parquet_output))
    output_db = tmp_path / "lookup.duckdb"
    args = Namespace(
        parquet_root=parquet_output / "tables" / "variants",
        parquet_manifest_sha256="parquet-manifest-digest",
        output_db=output_db,
        expected_rows=2,
        threads=1,
        memory_limit="1GB",
        progress_interval=1,
        smoke_limit=None,
        reset=False,
        verbose=False,
    )

    first = duckdb_builder.build(args)
    second = duckdb_builder.build(args)

    assert first == second
    assert first["rows"] == 2
    assert first["duplicate_variant_ids"] == 0
    assert first["database_sha256"] == duckdb_builder.sha256_file(output_db)
    connection = duckdb.connect(str(output_db), read_only=True)
    assert connection.execute(
        "SELECT variation_type FROM variants WHERE variant_id='rs1'"
    ).fetchone() == ("SNV",)
    indexes = connection.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name='variants'"
    ).fetchall()
    connection.close()
    assert indexes == [("idx_variants_variant_id",)]
