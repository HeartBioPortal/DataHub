import importlib.util
import json
import logging
import sys
from pathlib import Path

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
    association_path.parent.mkdir(parents=True, exist_ok=True)
    overall_path.parent.mkdir(parents=True, exist_ok=True)

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
    association_path.parent.mkdir(parents=True, exist_ok=True)
    overall_path.parent.mkdir(parents=True, exist_ok=True)

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
    association_dir.mkdir(parents=True, exist_ok=True)
    overall_dir.mkdir(parents=True, exist_ok=True)

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

    unit = module.GeneWorkUnit(dataset_type="CVD", gene_id="shard_0000", point_rows=2, shard_id=0)
    module._validate_stage_output(
        stage_root=stage_root,
        unit=unit,
        disable_rollup=True,
        logger=logging.getLogger("test"),
    )
