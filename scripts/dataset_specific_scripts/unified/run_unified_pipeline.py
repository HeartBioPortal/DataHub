#!/usr/bin/env python3
"""Profile-driven orchestration for unified MVP + legacy pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shlex
import subprocess
import sys
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]

_ALL_STEPS = ("mvp_ingest", "legacy_ingest", "publish")
_UNRESOLVED_ENV_PATTERN = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[^}]+\})")


@dataclass(frozen=True)
class PipelineStep:
    """A runnable pipeline step with command payload."""

    name: str
    command: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run or submit the unified DataHub pipeline (MVP ingest + legacy ingest + "
            "unified publish) using runtime execution profiles."
        )
    )
    parser.add_argument(
        "--profile",
        default="local_laptop",
        help="Profile name in runtime profiles JSON.",
    )
    parser.add_argument(
        "--profiles-json",
        default=str(REPO_ROOT / "config" / "runtime_profiles" / "unified_pipeline_profiles.json"),
        help="Path to runtime profile catalog JSON.",
    )
    parser.add_argument(
        "--step",
        action="append",
        choices=[*list(_ALL_STEPS), "all"],
        help="Step to execute. Repeat to run multiple steps. Defaults to all.",
    )
    parser.add_argument(
        "--mode",
        default="auto",
        choices=["auto", "local", "slurm"],
        help="Execution mode. auto uses profile scheduler.",
    )
    parser.add_argument(
        "--python-executable",
        default=None,
        help=(
            "Python executable used inside generated step commands. "
            "Defaults to profile value or current interpreter."
        ),
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Override profile values using dotted keys. "
            "Example: --set publish.per_gene_shards=2048"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved commands and exit without executing.",
    )
    parser.add_argument(
        "--submit-slurm",
        action="store_true",
        help="Submit jobs with sbatch when mode resolves to slurm.",
    )
    parser.add_argument(
        "--publish-redis",
        action="store_true",
        help="Enable Redis publish in publish step.",
    )
    parser.add_argument(
        "--slurm-setup-command",
        action="append",
        default=[],
        dest="slurm_setup_commands",
        help=(
            "Command to run before each Slurm step command (for example: "
            "'module load python/3.11'). Repeat for multiple commands."
        ),
    )
    parser.add_argument(
        "--reset-mvp-checkpoint",
        action="store_true",
        help="Pass --reset-checkpoint to MVP ingest step.",
    )
    parser.add_argument(
        "--reset-legacy-checkpoint",
        action="store_true",
        help="Pass --reset-checkpoint to legacy ingest step.",
    )
    parser.add_argument(
        "--reset-publish-checkpoint",
        action="store_true",
        help="Pass --reset-checkpoint to unified publish step.",
    )
    parser.add_argument(
        "--reset-publish-output",
        action="store_true",
        help="Pass --reset-output to unified publish step.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Runner log level.",
    )
    return parser.parse_args()


def _parse_override_value(raw: str) -> Any:
    lowered = raw.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _apply_dot_override(target: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [item for item in dotted_key.split(".") if item]
    if not parts:
        raise ValueError(f"Invalid override key: {dotted_key}")
    cursor: dict[str, Any] = target
    for part in parts[:-1]:
        existing = cursor.get(part)
        if existing is None:
            cursor[part] = {}
            existing = cursor[part]
        if not isinstance(existing, dict):
            raise ValueError(f"Cannot descend into non-object key: {part}")
        cursor = existing
    cursor[parts[-1]] = value


def _expand_env_paths(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: _expand_env_paths(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_expand_env_paths(item) for item in payload]
    if isinstance(payload, str):
        expanded = os.path.expandvars(os.path.expanduser(payload))
        return expanded
    return payload


def _load_profile(args: argparse.Namespace) -> tuple[dict[str, Any], Path]:
    profiles_path = Path(args.profiles_json).expanduser().resolve()
    if not profiles_path.exists():
        raise FileNotFoundError(f"Profile catalog not found: {profiles_path}")

    payload = json.loads(profiles_path.read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("profiles"), dict):
        raise ValueError("Profile catalog must contain a top-level 'profiles' object.")

    profiles = payload["profiles"]
    if args.profile not in profiles:
        raise ValueError(
            f"Profile '{args.profile}' not found in {profiles_path}. "
            f"Available: {sorted(profiles.keys())}"
        )

    profile = deepcopy(profiles[args.profile])
    if not isinstance(profile, dict):
        raise ValueError(f"Profile '{args.profile}' is not a JSON object.")

    for entry in args.set:
        if "=" not in entry:
            raise ValueError(f"Override must use KEY=VALUE format: {entry}")
        key, raw_value = entry.split("=", 1)
        _apply_dot_override(profile, key.strip(), _parse_override_value(raw_value.strip()))

    profile = _expand_env_paths(profile)
    return profile, profiles_path


def _normalize_steps(raw_steps: list[str] | None) -> list[str]:
    if not raw_steps or "all" in raw_steps:
        return list(_ALL_STEPS)
    ordered: list[str] = []
    for step in _ALL_STEPS:
        if step in raw_steps:
            ordered.append(step)
    return ordered


def _require_path(config: dict[str, Any], key: str) -> str:
    value = config.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required profile path key: paths.{key}")
    return str(value)


def _bool_flag(
    command: list[str],
    *,
    true_flag: str,
    false_flag: str | None,
    value: Any,
) -> None:
    if value is True:
        command.append(true_flag)
    elif value is False and false_flag:
        command.append(false_flag)


def _mvp_checkpoint_path(paths: dict[str, Any]) -> str:
    state_root = Path(_require_path(paths, "state_root"))
    return str(state_root / "mvp_fast_checkpoint.json")


def _legacy_checkpoint_path(paths: dict[str, Any]) -> str:
    state_root = Path(_require_path(paths, "state_root"))
    return str(state_root / "legacy_raw_checkpoint.json")


def _publish_checkpoint_path(paths: dict[str, Any]) -> str:
    state_root = Path(_require_path(paths, "state_root"))
    return str(state_root / "unified_publish_checkpoint.json")


def _publish_state_dir(paths: dict[str, Any]) -> str:
    state_root = Path(_require_path(paths, "state_root"))
    return str(state_root / "unified_publish_state")


def _build_mvp_ingest_command(profile: dict[str, Any], args: argparse.Namespace) -> list[str]:
    paths = profile.get("paths", {})
    defaults = profile.get("defaults", {})
    cfg = profile.get("mvp_ingest", {})
    python_executable = str(
        args.python_executable
        or profile.get("python_executable")
        or sys.executable
        or "python3"
    )

    command = [
        python_executable,
        str(REPO_ROOT / "scripts" / "dataset_specific_scripts" / "mvp" / "ingest_mvp_duckdb_fast.py"),
        "--input-path",
        _require_path(paths, "mvp_input_path"),
        "--db-path",
        _require_path(paths, "db_path"),
        "--table-name",
        str(paths.get("table_name", "mvp_association_points")),
        "--checkpoint-path",
        str(cfg.get("checkpoint_path", _mvp_checkpoint_path(paths))),
        "--threads",
        str(cfg.get("threads", 0)),
        "--memory-limit",
        str(cfg.get("memory_limit", "8GB")),
        "--temp-directory",
        str(paths.get("temp_directory", "/tmp")),
        "--log-level",
        args.log_level,
    ]

    dataset_id = cfg.get("dataset_id")
    if dataset_id:
        command.extend(["--dataset-id", str(dataset_id)])
    source = cfg.get("source", "million_veteran_program")
    command.extend(["--source", str(source)])

    dataset_types = cfg.get("dataset_types", defaults.get("dataset_types"))
    if dataset_types:
        command.extend(["--dataset-types", str(dataset_types)])

    phenotype_map_json = cfg.get("phenotype_map_json")
    if phenotype_map_json:
        command.extend(["--phenotype-map-json", str(phenotype_map_json)])

    fallback_dataset_type = cfg.get("fallback_dataset_type")
    if fallback_dataset_type:
        command.extend(["--fallback-dataset-type", str(fallback_dataset_type)])

    _bool_flag(
        command,
        true_flag="--duckdb-progress-bar",
        false_flag="--no-duckdb-progress-bar",
        value=cfg.get("duckdb_progress_bar"),
    )
    _bool_flag(
        command,
        true_flag="--csv-ignore-errors",
        false_flag="--no-csv-ignore-errors",
        value=cfg.get("csv_ignore_errors"),
    )
    _bool_flag(
        command,
        true_flag="--csv-null-padding",
        false_flag="--no-csv-null-padding",
        value=cfg.get("csv_null_padding"),
    )
    if cfg.get("csv_strict_mode") is True:
        command.append("--csv-strict-mode")
    if cfg.get("preserve_insertion_order") is True:
        command.append("--preserve-insertion-order")

    if args.reset_mvp_checkpoint:
        command.append("--reset-checkpoint")
    return command


def _build_legacy_ingest_command(profile: dict[str, Any], args: argparse.Namespace) -> list[str]:
    paths = profile.get("paths", {})
    cfg = profile.get("legacy_ingest", {})
    python_executable = str(
        args.python_executable
        or profile.get("python_executable")
        or sys.executable
        or "python3"
    )

    command = [
        python_executable,
        str(
            REPO_ROOT
            / "scripts"
            / "dataset_specific_scripts"
            / "unified"
            / "ingest_legacy_raw_duckdb.py"
        ),
        "--cvd-input-path",
        _require_path(paths, "legacy_cvd_input_path"),
        "--trait-input-path",
        _require_path(paths, "legacy_trait_input_path"),
        "--db-path",
        _require_path(paths, "db_path"),
        "--table-name",
        str(paths.get("table_name", "mvp_association_points")),
        "--checkpoint-path",
        str(cfg.get("checkpoint_path", _legacy_checkpoint_path(paths))),
        "--threads",
        str(cfg.get("threads", 0)),
        "--memory-limit",
        str(cfg.get("memory_limit", "8GB")),
        "--temp-directory",
        str(paths.get("temp_directory", "/tmp")),
        "--cvd-delimiter",
        str(cfg.get("cvd_delimiter", "auto")),
        "--trait-delimiter",
        str(cfg.get("trait_delimiter", "auto")),
        "--log-level",
        args.log_level,
    ]

    command.extend(["--source-cvd", str(cfg.get("source_cvd", "legacy_cvd_raw"))])
    command.extend(["--source-trait", str(cfg.get("source_trait", "legacy_trait_raw"))])

    if cfg.get("dataset_id_cvd"):
        command.extend(["--dataset-id-cvd", str(cfg.get("dataset_id_cvd"))])
    if cfg.get("dataset_id_trait"):
        command.extend(["--dataset-id-trait", str(cfg.get("dataset_id_trait"))])

    phenotype_map_json = cfg.get("phenotype_map_json")
    if phenotype_map_json:
        command.extend(["--phenotype-map-json", str(phenotype_map_json)])
    fallback_dataset_type = cfg.get("fallback_dataset_type")
    if fallback_dataset_type:
        command.extend(["--fallback-dataset-type", str(fallback_dataset_type)])

    _bool_flag(
        command,
        true_flag="--duckdb-progress-bar",
        false_flag="--no-duckdb-progress-bar",
        value=cfg.get("duckdb_progress_bar"),
    )
    _bool_flag(
        command,
        true_flag="--csv-ignore-errors",
        false_flag="--no-csv-ignore-errors",
        value=cfg.get("csv_ignore_errors"),
    )
    _bool_flag(
        command,
        true_flag="--csv-null-padding",
        false_flag="--no-csv-null-padding",
        value=cfg.get("csv_null_padding"),
    )
    if cfg.get("csv_strict_mode") is True:
        command.append("--csv-strict-mode")
    if cfg.get("preserve_insertion_order") is True:
        command.append("--preserve-insertion-order")

    if args.reset_legacy_checkpoint:
        command.append("--reset-checkpoint")
    return command


def _build_publish_command(profile: dict[str, Any], args: argparse.Namespace) -> list[str]:
    paths = profile.get("paths", {})
    defaults = profile.get("defaults", {})
    cfg = profile.get("publish", {})
    python_executable = str(
        args.python_executable
        or profile.get("python_executable")
        or sys.executable
        or "python3"
    )

    command = [
        python_executable,
        str(
            REPO_ROOT
            / "scripts"
            / "dataset_specific_scripts"
            / "unified"
            / "publish_unified_from_duckdb.py"
        ),
        "--db-path",
        _require_path(paths, "db_path"),
        "--source-table",
        str(paths.get("table_name", "mvp_association_points")),
        "--working-table",
        str(cfg.get("working_table", "association_points_unified")),
        "--dedup-mode",
        str(cfg.get("dedup_mode", "per_gene")),
        "--per-gene-shards",
        str(cfg.get("per_gene_shards", 512)),
        "--output-root",
        _require_path(paths, "output_root"),
        "--source-priority",
        str(cfg.get("source_priority", defaults.get("source_priority", ""))),
        "--dataset-types",
        str(cfg.get("dataset_types", defaults.get("dataset_types", "CVD,TRAIT"))),
        "--json-compression",
        str(cfg.get("json_compression", "gzip")),
        "--json-gzip-level",
        str(cfg.get("json_gzip_level", 6)),
        "--json-indent",
        str(cfg.get("json_indent", 0)),
        "--ancestry-precision",
        str(cfg.get("ancestry_precision", 6)),
        "--publish-batch-size",
        str(cfg.get("publish_batch_size", 50_000)),
        "--query-chunk-rows",
        str(cfg.get("query_chunk_rows", 200_000)),
        "--unit-partitions",
        str(cfg.get("unit_partitions", 1)),
        "--unit-partition-index",
        str(cfg.get("unit_partition_index", 0)),
        "--checkpoint-path",
        str(cfg.get("checkpoint_path", _publish_checkpoint_path(paths))),
        "--state-dir",
        str(cfg.get("state_dir", _publish_state_dir(paths))),
        "--threads",
        str(cfg.get("threads", 0)),
        "--memory-limit",
        str(cfg.get("memory_limit", "8GB")),
        "--temp-directory",
        str(paths.get("temp_directory", "/tmp")),
        "--log-level",
        args.log_level,
    ]

    max_temp_directory_size = cfg.get("max_temp_directory_size")
    if max_temp_directory_size:
        command.extend(["--max-temp-directory-size", str(max_temp_directory_size)])
    resume_seed_checkpoint = cfg.get("resume_seed_checkpoint")
    if resume_seed_checkpoint:
        command.extend(["--resume-seed-checkpoint", str(resume_seed_checkpoint)])

    rollup_tree_json = cfg.get("rollup_tree_json", paths.get("rollup_tree_json"))
    if rollup_tree_json:
        command.extend(["--rollup-tree-json", str(rollup_tree_json)])

    if cfg.get("disable_rollup") is True:
        command.append("--disable-rollup")

    _bool_flag(
        command,
        true_flag="--duckdb-progress-bar",
        false_flag="--no-duckdb-progress-bar",
        value=cfg.get("duckdb_progress_bar"),
    )
    if cfg.get("preserve_insertion_order") is True:
        command.append("--preserve-insertion-order")

    if args.reset_publish_checkpoint:
        command.append("--reset-checkpoint")
    if args.reset_publish_output:
        command.append("--reset-output")
    if args.publish_redis:
        command.append("--publish-redis")
    return command


def _build_steps(profile: dict[str, Any], args: argparse.Namespace, step_names: list[str]) -> list[PipelineStep]:
    builders = {
        "mvp_ingest": _build_mvp_ingest_command,
        "legacy_ingest": _build_legacy_ingest_command,
        "publish": _build_publish_command,
    }
    steps: list[PipelineStep] = []
    for step_name in step_names:
        command = builders[step_name](profile, args)
        steps.append(PipelineStep(name=step_name, command=command))
    return steps


def _collect_unresolved_env_tokens(steps: list[PipelineStep]) -> dict[str, list[str]]:
    unresolved: dict[str, list[str]] = {}
    for step in steps:
        tokens: set[str] = set()
        for arg in step.command:
            tokens.update(_UNRESOLVED_ENV_PATTERN.findall(arg))
        if tokens:
            unresolved[step.name] = sorted(tokens)
    return unresolved


def _run_local(steps: list[PipelineStep], logger: logging.Logger) -> None:
    for index, step in enumerate(steps, start=1):
        logger.info(
            "Executing local step %d/%d: %s",
            index,
            len(steps),
            step.name,
        )
        logger.info("Command: %s", shlex.join(step.command))
        subprocess.run(step.command, cwd=REPO_ROOT, check=True)


def _apply_slurm_option(command: list[str], flag: str, value: Any) -> None:
    if value is None:
        return
    text = str(value).strip()
    if text == "":
        return
    command.extend([flag, text])


def _build_sbatch_command(
    *,
    slurm_cfg: dict[str, Any],
    step: PipelineStep,
    dependency_job_id: str | None,
) -> list[str]:
    sbatch_cmd = ["sbatch", "--parsable", "--job-name", f"datahub_{step.name}"]

    _apply_slurm_option(sbatch_cmd, "--account", slurm_cfg.get("account"))
    _apply_slurm_option(sbatch_cmd, "--partition", slurm_cfg.get("partition"))
    _apply_slurm_option(sbatch_cmd, "--qos", slurm_cfg.get("qos"))
    _apply_slurm_option(sbatch_cmd, "--time", slurm_cfg.get("time"))
    _apply_slurm_option(sbatch_cmd, "--cpus-per-task", slurm_cfg.get("cpus_per_task"))
    _apply_slurm_option(sbatch_cmd, "--mem", slurm_cfg.get("mem"))
    _apply_slurm_option(sbatch_cmd, "--export", slurm_cfg.get("export"))
    _apply_slurm_option(sbatch_cmd, "--mail-type", slurm_cfg.get("mail_type"))
    _apply_slurm_option(sbatch_cmd, "--mail-user", slurm_cfg.get("mail_user"))

    log_dir_raw = slurm_cfg.get("log_dir")
    if log_dir_raw:
        log_dir = Path(str(log_dir_raw))
        sbatch_cmd.extend(
            [
                "--output",
                str(log_dir / f"{step.name}_%j.out"),
                "--error",
                str(log_dir / f"{step.name}_%j.err"),
            ]
        )

    if dependency_job_id:
        sbatch_cmd.extend(["--dependency", f"afterok:{dependency_job_id}"])

    setup_raw = slurm_cfg.get("setup_commands", [])
    if isinstance(setup_raw, str):
        setup_commands = [setup_raw] if setup_raw.strip() else []
    elif isinstance(setup_raw, list):
        setup_commands = [str(item).strip() for item in setup_raw if str(item).strip()]
    else:
        raise ValueError("slurm.setup_commands must be a string or list of strings.")

    wrapped_segments: list[str] = []
    wrapped_segments.extend(setup_commands)
    wrapped_segments.append(f"cd {shlex.quote(str(REPO_ROOT))}")
    wrapped_segments.append(shlex.join(step.command))
    wrapped = " && ".join(wrapped_segments)
    sbatch_cmd.extend(["--wrap", wrapped])
    return sbatch_cmd


def _run_or_plan_slurm(
    *,
    profile: dict[str, Any],
    steps: list[PipelineStep],
    submit: bool,
    logger: logging.Logger,
) -> dict[str, Any]:
    slurm_cfg = profile.get("slurm", {})
    if not isinstance(slurm_cfg, dict):
        raise ValueError("Profile slurm configuration must be an object.")

    log_dir_raw = slurm_cfg.get("log_dir")
    if submit and log_dir_raw:
        Path(str(log_dir_raw)).mkdir(parents=True, exist_ok=True)

    submitted: list[dict[str, Any]] = []
    planned: list[dict[str, Any]] = []
    dependency: str | None = None

    for step in steps:
        sbatch_cmd = _build_sbatch_command(
            slurm_cfg=slurm_cfg,
            step=step,
            dependency_job_id=dependency,
        )
        planned.append({"step": step.name, "sbatch": shlex.join(sbatch_cmd)})

        if not submit:
            continue

        logger.info("Submitting slurm step: %s", step.name)
        logger.info("sbatch command: %s", shlex.join(sbatch_cmd))
        result = subprocess.run(
            sbatch_cmd,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        raw_job_id = result.stdout.strip()
        if not raw_job_id:
            raise RuntimeError(f"Empty sbatch response for step {step.name}")
        dependency = raw_job_id.split(";")[0].strip()
        submitted.append(
            {
                "step": step.name,
                "job_id": dependency,
                "sbatch_stdout": raw_job_id,
            }
        )

    return {"planned": planned, "submitted": submitted}


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("datahub.unified.runner")
    started = time.perf_counter()

    profile, profiles_path = _load_profile(args)
    profile_name = args.profile
    resolved_scheduler = str(profile.get("scheduler", "local")).strip().lower()
    if args.slurm_setup_commands:
        slurm_cfg = profile.setdefault("slurm", {})
        if not isinstance(slurm_cfg, dict):
            raise ValueError("Profile slurm config must be an object when using --slurm-setup-command.")
        existing = slurm_cfg.get("setup_commands", [])
        if isinstance(existing, str):
            base_commands = [existing] if existing.strip() else []
        elif isinstance(existing, list):
            base_commands = [str(item).strip() for item in existing if str(item).strip()]
        else:
            raise ValueError("Profile slurm.setup_commands must be a string or list.")
        base_commands.extend(args.slurm_setup_commands)
        slurm_cfg["setup_commands"] = base_commands
    mode = args.mode
    if mode == "auto":
        mode = "slurm" if resolved_scheduler == "slurm" else "local"

    step_names = _normalize_steps(args.step)
    steps = _build_steps(profile, args, step_names)
    unresolved_tokens = _collect_unresolved_env_tokens(steps)
    if unresolved_tokens and not args.dry_run:
        raise ValueError(
            "Unresolved environment variables in command arguments: "
            f"{unresolved_tokens}. Export these variables or override paths via --set."
        )

    logger.info(
        "Unified runner start: profile=%s mode=%s steps=%s",
        profile_name,
        mode,
        step_names,
    )
    logger.info("Profile catalog: %s", profiles_path)

    execution_summary: dict[str, Any] = {
        "profile": profile_name,
        "profiles_json": str(profiles_path),
        "mode": mode,
        "steps": [step.name for step in steps],
        "commands": {
            step.name: shlex.join(step.command) for step in steps
        },
        "unresolved_env_tokens": unresolved_tokens,
    }

    if args.dry_run:
        execution_summary["dry_run"] = True
        print(json.dumps(execution_summary, indent=2))
        return 0

    if mode == "local":
        _run_local(steps, logger)
    elif mode == "slurm":
        slurm_result = _run_or_plan_slurm(
            profile=profile,
            steps=steps,
            submit=args.submit_slurm,
            logger=logger,
        )
        execution_summary["slurm"] = slurm_result
        if not args.submit_slurm:
            logger.info("Slurm mode selected without submission; printed planned sbatch commands.")
            print(json.dumps(execution_summary, indent=2))
            return 0
    else:
        raise ValueError(f"Unsupported execution mode: {mode}")

    elapsed = time.perf_counter() - started
    execution_summary["dry_run"] = False
    execution_summary["elapsed_seconds"] = round(elapsed, 2)
    print(json.dumps(execution_summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
