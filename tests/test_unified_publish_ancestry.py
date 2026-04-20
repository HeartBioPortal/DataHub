import importlib.util
import json
import logging
import sys
from pathlib import Path

import duckdb
import pytest


def _load_publish_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = (
        repo_root
        / "scripts"
        / "dataset_specific_scripts"
        / "unified"
        / "publish_unified_from_duckdb.py"
    )
    spec = importlib.util.spec_from_file_location("unified_publish_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_unified_publish_records_preserve_source_ancestry_metadata() -> None:
    module = _load_publish_module()

    row = (
        "hbp_mvp_association",
        "CVD",
        "million_veteran_program",
        "ANK2",
        "rs1",
        "arrhythmia_(cardiac)_nos",
        "cardiac_dysrhythmias",
        "SNP",
        None,
        None,
        0.123,
        "African",
        0.456,
        "AFR",
        "African",
        "Phe_427_5",
        "/tmp/source.csv.gz",
    )

    record = module._new_record(row)

    assert record.ancestry == {"African": 0.456}
    assert record.metadata["phenotype_key"] == "Phe_427_5"
    assert record.metadata["source_file"] == "/tmp/source.csv.gz"
    assert record.metadata["source_ancestry"] == {
        "AFR": {
            "source_ancestry_code": "AFR",
            "source_ancestry_label": "African",
            "canonical_ancestry_group": "African",
            "af": 0.456,
        }
    }


def test_preflight_stage_validation_accepts_canonical_payloads(tmp_path: Path) -> None:
    module = _load_publish_module()

    stage_root = tmp_path / "stage"
    association_path = stage_root / "association" / "final" / "association" / "CVD" / "GENE1.json"
    overall_path = stage_root / "association" / "final" / "overall" / "CVD" / "GENE1.json"
    variant_index_path = stage_root / "association" / "final" / "variant_index" / "CVD" / "GENE1.json"
    association_path.parent.mkdir(parents=True, exist_ok=True)
    overall_path.parent.mkdir(parents=True, exist_ok=True)
    variant_index_path.parent.mkdir(parents=True, exist_ok=True)

    association_path.write_text(
        json.dumps(
            [
                {
                    "disease": ["cardiomyopathies", "cardiomyopathy"],
                    "vc": [{"name": "INDEL", "value": 1}],
                    "msc": [{"name": "missense variant", "value": 1}],
                    "cs": [{"name": "likely benign", "value": 1}],
                    "ancestry": [],
                }
            ]
        )
    )
    overall_path.write_text(
        json.dumps(
            {
                "data": {
                    "vc": {"INDEL": 1},
                    "msc": {"missense variant": 1},
                    "cs": {"likely benign": 1},
                    "ancestry": {},
                },
                "pvals": {},
            }
        )
    )
    variant_index_path.write_text(
        json.dumps(
            [
                {
                    "variant_id": "rs1",
                    "gene_id": "GENE1",
                    "dataset_type": "CVD",
                    "phenotype_path": ["cardiomyopathies", "cardiomyopathy"],
                    "disease": ["cardiomyopathies", "cardiomyopathy"],
                    "variation_type": "INDEL",
                    "most_severe_consequence": "missense variant",
                    "clinical_significance": "likely benign",
                }
            ]
        )
    )

    unit = module.GeneWorkUnit(dataset_type="CVD", gene_id="GENE1", point_rows=1)
    module._validate_stage_output(
        stage_root=stage_root,
        unit=unit,
        disable_rollup=True,
        logger=logging.getLogger("test"),
    )


def test_preflight_stage_validation_rejects_noncanonical_axis_labels(tmp_path: Path) -> None:
    module = _load_publish_module()

    stage_root = tmp_path / "stage"
    association_path = stage_root / "association" / "final" / "association" / "CVD" / "GENE1.json"
    overall_path = stage_root / "association" / "final" / "overall" / "CVD" / "GENE1.json"
    variant_index_path = stage_root / "association" / "final" / "variant_index" / "CVD" / "GENE1.json"
    association_path.parent.mkdir(parents=True, exist_ok=True)
    overall_path.parent.mkdir(parents=True, exist_ok=True)
    variant_index_path.parent.mkdir(parents=True, exist_ok=True)

    association_path.write_text(
        json.dumps(
            [
                {
                    "disease": ["cardiomyopathies", "cardiomyopathy"],
                    "vc": [
                        {"name": "indel", "value": 1},
                        {"name": "INDEL", "value": 1},
                    ],
                    "msc": [],
                    "cs": [],
                    "ancestry": [],
                }
            ]
        )
    )
    overall_path.write_text(
        json.dumps(
            {
                "data": {
                    "vc": {"indel": 1, "INDEL": 1},
                    "msc": {},
                    "cs": {},
                    "ancestry": {},
                },
                "pvals": {},
            }
        )
    )
    variant_index_path.write_text(
        json.dumps(
            [
                {
                    "variant_id": "rs1",
                    "gene_id": "GENE1",
                    "dataset_type": "CVD",
                    "phenotype_path": ["cardiomyopathies", "cardiomyopathy"],
                    "disease": ["cardiomyopathies", "cardiomyopathy"],
                    "variation_type": "INDEL",
                }
            ]
        )
    )

    unit = module.GeneWorkUnit(dataset_type="CVD", gene_id="GENE1", point_rows=1)
    with pytest.raises(ValueError, match="non-canonical category"):
        module._validate_stage_output(
            stage_root=stage_root,
            unit=unit,
            disable_rollup=True,
            logger=logging.getLogger("test"),
        )


def test_preflight_stage_validation_accepts_shard_stage_outputs(tmp_path: Path) -> None:
    module = _load_publish_module()

    stage_root = tmp_path / "stage"
    association_dir = stage_root / "association" / "final" / "association" / "CVD"
    overall_dir = stage_root / "association" / "final" / "overall" / "CVD"
    variant_index_dir = stage_root / "association" / "final" / "variant_index" / "CVD"
    association_dir.mkdir(parents=True, exist_ok=True)
    overall_dir.mkdir(parents=True, exist_ok=True)
    variant_index_dir.mkdir(parents=True, exist_ok=True)

    for gene in ("ANK2", "TTN"):
        (association_dir / f"{gene}.json").write_text(
            json.dumps(
                [
                    {
                        "disease": ["cardiomyopathies", "cardiomyopathy"],
                        "vc": [{"name": "SNP", "value": 1}],
                        "msc": [{"name": "missense variant", "value": 1}],
                        "cs": [{"name": "likely benign", "value": 1}],
                        "ancestry": [],
                    }
                ]
            )
        )
        (overall_dir / f"{gene}.json").write_text(
            json.dumps(
                {
                    "data": {
                        "vc": {"SNP": 1},
                        "msc": {"missense variant": 1},
                        "cs": {"likely benign": 1},
                        "ancestry": {},
                    },
                    "pvals": {},
                }
            )
        )
        (variant_index_dir / f"{gene}.json").write_text(
            json.dumps(
                [
                    {
                        "variant_id": "rs1",
                        "gene_id": gene,
                        "dataset_type": "CVD",
                        "phenotype_path": ["cardiomyopathies", "cardiomyopathy"],
                        "disease": ["cardiomyopathies", "cardiomyopathy"],
                        "variation_type": "SNP",
                        "most_severe_consequence": "missense variant",
                        "clinical_significance": "likely benign",
                    }
                ]
            )
        )

    unit = module.GeneWorkUnit(dataset_type="CVD", gene_id="shard_0000", point_rows=2, shard_id=0)
    module._validate_stage_output(
        stage_root=stage_root,
        unit=unit,
        disable_rollup=True,
        logger=logging.getLogger("test"),
    )


def test_preflight_stage_validation_accepts_variant_index_only(tmp_path: Path) -> None:
    module = _load_publish_module()

    stage_root = tmp_path / "stage"
    variant_index_path = stage_root / "association" / "final" / "variant_index" / "CVD" / "GENE1.json"
    variant_index_path.parent.mkdir(parents=True, exist_ok=True)
    variant_index_path.write_text(
        json.dumps(
            [
                {
                    "variant_id": "rs1",
                    "gene_id": "GENE1",
                    "dataset_type": "CVD",
                    "phenotype_path": ["cardiomyopathies", "cardiomyopathy"],
                    "disease": ["cardiomyopathies", "cardiomyopathy"],
                    "variation_type": "SNP",
                }
            ]
        )
    )

    unit = module.GeneWorkUnit(dataset_type="CVD", gene_id="GENE1", point_rows=1)
    module._validate_stage_output(
        stage_root=stage_root,
        unit=unit,
        disable_rollup=True,
        publisher_mode="variant-index-only",
        logger=logging.getLogger("test"),
    )


def test_build_stage_publishers_supports_variant_index_only(tmp_path: Path) -> None:
    module = _load_publish_module()

    publishers = module._build_stage_publishers(
        output_root=tmp_path,
        disable_rollup=False,
        disable_variant_index=False,
        publisher_mode="variant-index-only",
        rollup_tree_json=None,
        ancestry_precision=6,
        json_compression="gzip",
        json_gzip_level=6,
        json_indent=None,
        export_runtime=None,
    )

    assert [type(publisher).__name__ for publisher in publishers] == ["VariantIndexPublisher"]


def test_grouped_variant_index_cursor_deduplicates_without_window(tmp_path: Path) -> None:
    module = _load_publish_module()

    db_path = tmp_path / "points.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE mvp_association_points (
            dataset_id VARCHAR,
            dataset_type VARCHAR,
            source VARCHAR,
            gene_id VARCHAR,
            variant_id VARCHAR,
            phenotype VARCHAR,
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
            source_file VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO mvp_association_points VALUES
        ('d1', 'CVD', 'legacy_cvd_raw', 'TTN', 'rs1', 'cardiomyopathy', 'cardiomyopathies', 'SNP', 'benign', 'missense_variant', 1e-8, 'African', 0.1, 'AFR', 'African', 'phe1', '/tmp/a.csv', '2024-01-02'::TIMESTAMP),
        ('d2', 'CVD', 'million_veteran_program', 'TTN', 'rs1', 'cardiomyopathy', 'cardiomyopathies', 'indel', 'pathogenic', 'intron_variant', 1e-9, 'African', 0.2, 'AFR', 'African', 'phe1', '/tmp/b.csv', '2024-01-03'::TIMESTAMP),
        ('d1', 'CVD', 'legacy_cvd_raw', 'TTN', 'rs1', 'cardiomyopathy', 'cardiomyopathies', 'SNP', 'benign', 'missense_variant', 1e-8, 'European', 0.3, 'EUR', 'European', 'phe1', '/tmp/a.csv', '2024-01-02'::TIMESTAMP)
        """
    )

    cursor = module._open_grouped_unit_cursor_from_source(
        con,
        source_table="mvp_association_points",
        dataset_types={"CVD"},
        unit=module.GeneWorkUnit(dataset_type="CVD", gene_id="TTN", point_rows=3),
        per_gene_shards=1,
        source_priority=[("legacy_cvd_raw", 1), ("million_veteran_program", 2)],
    )
    rows = cursor.fetchall()
    con.close()

    assert len(rows) == 2
    assert {row[module.ROW_ANCESTRY]: row[module.ROW_ANCESTRY_AF] for row in rows} == {
        "African": 0.1,
        "European": 0.3,
    }
    first_row = rows[0]
    assert first_row[module.ROW_DATASET_ID] == "d1"
    assert first_row[module.ROW_SOURCE] == "legacy_cvd_raw"
    assert first_row[module.ROW_VARIATION_TYPE] == "SNP"


def test_working_table_excludes_numeric_gene_identifiers(tmp_path: Path) -> None:
    module = _load_publish_module()

    db_path = tmp_path / "points.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE mvp_association_points (
            dataset_id VARCHAR,
            dataset_type VARCHAR,
            source VARCHAR,
            gene_id VARCHAR,
            variant_id VARCHAR,
            phenotype VARCHAR,
            disease_category VARCHAR,
            variation_type VARCHAR,
            clinical_significance VARCHAR,
            most_severe_consequence VARCHAR,
            p_value DOUBLE,
            ancestry VARCHAR,
            ancestry_af DOUBLE,
            phenotype_key VARCHAR,
            source_file VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO mvp_association_points VALUES
        ('d1', 'CVD', 'legacy_cvd_raw', 'TTN', 'rs1', 'cardiomyopathy', 'cardiomyopathies', 'SNP', NULL, NULL, 1e-8, 'African', 0.1, NULL, '/tmp/a.csv', now()),
        ('d1', 'CVD', 'legacy_cvd_raw', '0.799091', 'rs2', 'cardiomyopathy', 'cardiomyopathies', 'SNP', NULL, NULL, 2e-8, 'African', 0.2, NULL, '/tmp/b.csv', now())
        """
    )

    logger = logging.getLogger("test")
    row_count, gene_count = module._build_working_table(
        con,
        source_table="mvp_association_points",
        working_table="__working_points",
        dataset_types={"CVD"},
        source_priority=[("legacy_cvd_raw", 1)],
        logger=logger,
    )

    genes = con.execute("SELECT gene_id FROM __working_points ORDER BY gene_id").fetchall()
    con.close()

    assert row_count == 1
    assert gene_count == 1
    assert genes == [("TTN",)]
