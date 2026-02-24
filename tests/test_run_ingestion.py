import csv
import json
import subprocess
from pathlib import Path


def _write_gwas_csv(path: Path) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["MarkerID", "Phenotype", "gene", "pval", "PMID"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "MarkerID": "rs777-A",
                "Phenotype": "cardiomyopathy",
                "gene": "MYBPC3",
                "pval": "1e-10",
                "PMID": "998877",
            }
        )


def test_run_ingestion_script_executes_pipeline(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gwas_csv = tmp_path / "gwas.csv"
    output_root = tmp_path / "output"
    config_path = tmp_path / "ingestion.json"

    _write_gwas_csv(gwas_csv)

    config_path.write_text(
        json.dumps(
            {
                "profile": "association",
                "adapters": [
                    {
                        "name": "gwas_association",
                        "params": {
                            "input_paths": str(gwas_csv),
                            "dataset_id": "test_gwas",
                        },
                    }
                ],
                "publishers": [
                    {
                        "name": "legacy_association",
                        "params": {"output_root": str(output_root)},
                    }
                ],
            }
        )
    )

    result = subprocess.run(
        ["scripts/run_ingestion.py", "--config", str(config_path)],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["validated_records"] == 1

    cvd_files = list((output_root / "association" / "final" / "association" / "CVD").glob("*.json"))
    assert cvd_files


def test_run_ingestion_script_executes_with_source_config(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gwas_csv = tmp_path / "gwas.csv"
    output_root = tmp_path / "output"
    config_path = tmp_path / "ingestion_sources.json"

    _write_gwas_csv(gwas_csv)

    config_path.write_text(
        json.dumps(
            {
                "profile": "association",
                "sources": [
                    {
                        "id": "gwas_catalog",
                        "params": {
                            "input_paths": str(gwas_csv),
                            "dataset_id": "source_gwas",
                        },
                    }
                ],
                "publishers": [
                    {
                        "name": "legacy_association",
                        "params": {"output_root": str(output_root)},
                    }
                ],
            }
        )
    )

    result = subprocess.run(
        ["scripts/run_ingestion.py", "--config", str(config_path)],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["validated_records"] == 1

    cvd_files = list((output_root / "association" / "final" / "association" / "CVD").glob("*.json"))
    assert cvd_files


def test_run_ingestion_supports_rollup_publisher(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    gwas_csv = tmp_path / "gwas.csv"
    output_root = tmp_path / "output"
    config_path = tmp_path / "ingestion_rollup.json"

    _write_gwas_csv(gwas_csv)

    config_path.write_text(
        json.dumps(
            {
                "profile": "association",
                "adapters": [
                    {
                        "name": "gwas_association",
                        "params": {
                            "input_paths": str(gwas_csv),
                            "dataset_id": "test_gwas",
                        },
                    }
                ],
                "publishers": [
                    {
                        "name": "legacy_association",
                        "params": {"output_root": str(output_root)},
                    },
                    {
                        "name": "phenotype_rollup",
                        "params": {"output_root": str(output_root)},
                    },
                ],
            }
        )
    )

    subprocess.run(
        ["scripts/run_ingestion.py", "--config", str(config_path)],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )

    rollup_files = list(
        (output_root / "association" / "final" / "association_rollup" / "CVD").glob("*.json")
    )
    assert rollup_files
