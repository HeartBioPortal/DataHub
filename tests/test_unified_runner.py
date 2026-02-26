import json
import os
import subprocess
from pathlib import Path


def _write_profiles_json(path: Path) -> None:
    payload = {
        "version": 1,
        "profiles": {
            "test_local": {
                "scheduler": "local",
                "paths": {
                    "mvp_input_path": "/tmp/in/mvp/*.csv.gz",
                    "legacy_cvd_input_path": "/tmp/in/legacy/cvd/*.txt",
                    "legacy_trait_input_path": "/tmp/in/legacy/trait/*.txt",
                    "db_path": "$DATAHUB_TEST_ROOT/mvp.duckdb",
                    "table_name": "mvp_association_points",
                    "output_root": "$DATAHUB_TEST_ROOT/out",
                    "temp_directory": "$DATAHUB_TEST_ROOT/tmp",
                    "rollup_tree_json": "$DATAHUB_TEST_ROOT/tree.json",
                    "state_root": "$DATAHUB_TEST_ROOT/state",
                },
                "defaults": {
                    "source_priority": "legacy_cvd_raw,legacy_trait_raw,million_veteran_program",
                    "dataset_types": "CVD,TRAIT",
                },
                "mvp_ingest": {"threads": 2, "memory_limit": "2GB"},
                "legacy_ingest": {"threads": 2, "memory_limit": "2GB"},
                "publish": {
                    "threads": 2,
                    "memory_limit": "2GB",
                    "per_gene_shards": 128,
                    "json_compression": "gzip",
                    "json_indent": 0,
                },
            },
            "test_slurm": {
                "scheduler": "slurm",
                "paths": {
                    "mvp_input_path": "/tmp/in/mvp/*.csv.gz",
                    "legacy_cvd_input_path": "/tmp/in/legacy/cvd/*.txt",
                    "legacy_trait_input_path": "/tmp/in/legacy/trait/*.txt",
                    "db_path": "/tmp/hbp/mvp.duckdb",
                    "table_name": "mvp_association_points",
                    "output_root": "/tmp/hbp/out",
                    "temp_directory": "/tmp/hbp/tmp",
                    "rollup_tree_json": "/tmp/hbp/tree.json",
                    "state_root": "/tmp/hbp/state",
                },
                "defaults": {
                    "source_priority": "legacy_cvd_raw,legacy_trait_raw,million_veteran_program",
                    "dataset_types": "CVD,TRAIT",
                },
                "mvp_ingest": {"threads": 4, "memory_limit": "8GB"},
                "legacy_ingest": {"threads": 4, "memory_limit": "8GB"},
                "publish": {
                    "threads": 4,
                    "memory_limit": "8GB",
                    "per_gene_shards": 256,
                    "json_compression": "gzip",
                    "json_indent": 0,
                },
                "slurm": {
                    "account": "r01806",
                    "time": "02:00:00",
                    "cpus_per_task": 8,
                    "mem": "32G",
                    "log_dir": "/tmp/hbp/logs",
                },
            },
        },
    }
    path.write_text(json.dumps(payload, indent=2))


def test_unified_runner_dry_run_resolves_profile_and_overrides(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    profiles_path = tmp_path / "profiles.json"
    _write_profiles_json(profiles_path)

    env = dict(os.environ)
    env["DATAHUB_TEST_ROOT"] = str(tmp_path)

    result = subprocess.run(
        [
            "python3",
            "scripts/dataset_specific_scripts/unified/run_unified_pipeline.py",
            "--profiles-json",
            str(profiles_path),
            "--profile",
            "test_local",
            "--step",
            "publish",
            "--dry-run",
            "--set",
            "publish.per_gene_shards=777",
            "--log-level",
            "WARNING",
        ],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
        env=env,
    )
    payload = json.loads(result.stdout)

    assert payload["mode"] == "local"
    assert payload["steps"] == ["publish"]
    command = payload["commands"]["publish"]
    assert "--per-gene-shards 777" in command
    assert str(tmp_path / "mvp.duckdb") in command


def test_unified_runner_slurm_plan_without_submission(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    profiles_path = tmp_path / "profiles.json"
    _write_profiles_json(profiles_path)

    result = subprocess.run(
        [
            "python3",
            "scripts/dataset_specific_scripts/unified/run_unified_pipeline.py",
            "--profiles-json",
            str(profiles_path),
            "--profile",
            "test_slurm",
            "--step",
            "mvp_ingest",
            "--step",
            "publish",
            "--mode",
            "slurm",
            "--log-level",
            "WARNING",
        ],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    planned = payload["slurm"]["planned"]

    assert payload["mode"] == "slurm"
    assert len(planned) == 2
    assert "--account r01806" in planned[0]["sbatch"]
    assert "--wrap" in planned[1]["sbatch"]
