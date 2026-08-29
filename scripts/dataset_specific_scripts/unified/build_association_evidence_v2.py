#!/usr/bin/env python3
"""Build the non-composite association evidence v2 sidecar.

The builder is resumable at source-file and derived-phase boundaries. It writes
only to the explicitly supplied sidecar database, checkpoint, manifest, and log
paths. Provider-unavailable sources are represented by retained source summaries
without reconstructing source studies.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.association_evidence_v2.builder import EvidenceV2Builder  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cvd-root", required=True, type=Path)
    parser.add_argument("--trait-root", required=True, type=Path)
    parser.add_argument("--variant-index-root", required=True, type=Path)
    parser.add_argument("--output-db", required=True, type=Path)
    parser.add_argument("--checkpoint-path", required=True, type=Path)
    parser.add_argument("--manifest-path", required=True, type=Path)
    parser.add_argument(
        "--phenotype-tree",
        type=Path,
        default=REPO_ROOT / "config" / "phenotype_tree.json",
    )
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--progress-interval", type=int, default=1)
    parser.add_argument(
        "--provider-chunk-rows",
        type=int,
        default=250_000,
        help=(
            "Provider rows per bounded transaction; 0 uses direct inserts up to 2 GB "
            "and 1,000,000-row one-pass chunks for larger sources."
        ),
    )
    parser.add_argument(
        "--smoke-files",
        type=int,
        help="Process at most this many source files per dataset type.",
    )
    parser.add_argument(
        "--smoke-rows-per-file",
        type=int,
        help="Process at most this many rows from each selected source file.",
    )
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--log-path", type=Path)
    parser.add_argument(
        "--checksum-inputs",
        action="store_true",
        help="SHA-256 source files after the build. This performs a second full read.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate paths and print the planned configuration without writing.",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_sha256_sidecar(path: Path, digest: str) -> Path:
    """Write the checksum contract consumed by the serving publisher."""
    checksum_path = path.with_suffix(path.suffix + ".sha256")
    checksum_path.write_text(f"{digest}  {path.name}\n")
    return checksum_path


def git_commit() -> str | None:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() or None


def git_dirty() -> bool:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.stdout.strip())


def transformation_files() -> list[Path]:
    package_root = REPO_ROOT / "src" / "datahub" / "association_evidence_v2"
    return [
        Path(__file__).resolve(),
        *sorted(package_root.glob("*.py")),
    ]


def source_files(root: Path, limit: int | None) -> list[Path]:
    files = sorted(path for path in root.iterdir() if path.is_file())
    return files[:limit] if limit is not None else files


def configure_logging(path: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


def main() -> int:
    args = parse_args()
    configure_logging(args.log_path)
    logger = logging.getLogger("datahub.association_evidence_v2.cli")
    for path in (args.cvd_root, args.trait_root, args.variant_index_root, args.phenotype_tree):
        if not path.exists():
            raise FileNotFoundError(path)
    command = " ".join(shlex.quote(item) for item in sys.argv)
    datahub_commit = git_commit()
    plan = {
        "release_id": args.release_id,
        "cvd_root": str(args.cvd_root.resolve()),
        "trait_root": str(args.trait_root.resolve()),
        "variant_index_root": str(args.variant_index_root.resolve()),
        "output_db": str(args.output_db.resolve()),
        "checkpoint_path": str(args.checkpoint_path.resolve()),
        "manifest_path": str(args.manifest_path.resolve()),
        "phenotype_tree": str(args.phenotype_tree.resolve()),
        "source_files": {
            "CVD": len(source_files(args.cvd_root, args.smoke_files)),
            "TRAIT": len(source_files(args.trait_root, args.smoke_files)),
        },
        "smoke_files": args.smoke_files,
        "smoke_rows_per_file": args.smoke_rows_per_file,
        "threads": args.threads,
        "memory_limit": args.memory_limit,
        "provider_chunk_rows": args.provider_chunk_rows,
        "temp_directory": str(args.temp_directory.resolve()) if args.temp_directory else None,
        "command": command,
        "datahub_git_commit": datahub_commit,
        "datahub_git_dirty": git_dirty(),
    }
    logger.info("Evidence v2 build plan: %s", json.dumps(plan, sort_keys=True))
    if args.dry_run:
        print(json.dumps({"mode": "dry_run", **plan}, indent=2, sort_keys=True))
        return 0

    builder = EvidenceV2Builder(
        cvd_root=args.cvd_root,
        trait_root=args.trait_root,
        variant_index_root=args.variant_index_root,
        output_db=args.output_db,
        checkpoint_path=args.checkpoint_path,
        phenotype_tree=args.phenotype_tree,
        release_id=args.release_id,
        command=command,
        builder_git_commit=datahub_commit,
        threads=args.threads,
        memory_limit=args.memory_limit,
        temp_directory=args.temp_directory,
        smoke_files=args.smoke_files,
        smoke_rows_per_file=args.smoke_rows_per_file,
        provider_chunk_rows=args.provider_chunk_rows,
        progress_interval=args.progress_interval,
    )
    summary = builder.run(resume=not args.no_resume)
    inputs: list[dict[str, Any]] = []
    for dataset_type, root in (("CVD", args.cvd_root), ("TRAIT", args.trait_root)):
        for path in source_files(root, args.smoke_files):
            item = {
                "dataset_type": dataset_type,
                "path": str(path.resolve()),
                "size_bytes": path.stat().st_size,
                "sha256": None,
                "checksum_status": "not_requested",
            }
            if args.checksum_inputs:
                item["sha256"] = sha256_file(path)
                item["checksum_status"] = "verified"
            inputs.append(item)
    variant_index_files = sorted(
        path
        for path in args.variant_index_root.rglob("*")
        if path.is_file() and (path.name.endswith(".json") or path.name.endswith(".json.gz"))
    )
    transformation_inventory = [
        {
            "path": str(path.relative_to(REPO_ROOT)),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in transformation_files()
    ]
    output_sha256 = sha256_file(args.output_db)
    output_checksum_path = write_sha256_sidecar(args.output_db, output_sha256)
    manifest = {
        **summary,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datahub_git_commit": datahub_commit,
        "datahub_git_dirty": git_dirty(),
        "builder_command": command,
        "transformation_files": transformation_inventory,
        "phenotype_tree": {
            "path": str(args.phenotype_tree.resolve()),
            "size_bytes": args.phenotype_tree.stat().st_size,
            "sha256": sha256_file(args.phenotype_tree),
        },
        "variant_index_input": {
            "path": str(args.variant_index_root.resolve()),
            "file_count": len(variant_index_files),
            "size_bytes": sum(path.stat().st_size for path in variant_index_files),
            "checksum_status": "not_requested_to_avoid_second_full_read",
        },
        "inputs": inputs,
        "output": {
            "path": str(args.output_db.resolve()),
            "size_bytes": args.output_db.stat().st_size,
            "sha256": output_sha256,
            "checksum_path": str(output_checksum_path.resolve()),
            "checksum_file_sha256": sha256_file(output_checksum_path),
        },
        "checkpoint_sha256": sha256_file(args.checkpoint_path),
    }
    args.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    args.manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
