#!/usr/bin/env python3
"""Build the rsID-preserving Protein Consequence Viewer serving layer."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from datahub.protein_consequence_rsid import (
    BuildInputs,
    ProteinConsequenceRsidBuilder,
    sha256_file,
)


def configure_logging(log_path: Path | None, verbose: bool) -> logging.Logger:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )
    return logging.getLogger("datahub.protein_consequence_rsid")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--verbose", action="store_true")
    sub = root.add_subparsers(dest="command", required=True)

    index = sub.add_parser("index-vep", help="Import all transcript-level VEP annotations once")
    index.add_argument("--annotations-csv", type=Path, required=True)
    index.add_argument("--output-db", type=Path, required=True)
    index.add_argument("--threads", type=int, default=4)
    index.add_argument("--memory-limit", default="16GB")
    index.add_argument("--temp-directory", type=Path)
    index.add_argument("--reset", action="store_true")
    index.add_argument("--log-path", type=Path)

    build = sub.add_parser("build", help="Build checkpointed per-gene viewer payloads")
    build.add_argument("--variant-index-root", type=Path, required=True)
    build.add_argument("--vep-index", type=Path, required=True)
    build.add_argument("--output-root", type=Path, required=True)
    build.add_argument("--genes", nargs="*")
    build.add_argument(
        "--gene-profile-index",
        type=Path,
        help="HGNC gene-profile JSONL; limits the build to its approved symbol field",
    )
    build.add_argument("--max-genes", type=int)
    build.add_argument("--workers", type=int, default=1)
    build.add_argument("--progress-interval", type=int, default=25)
    build.add_argument("--reset", action="store_true")
    build.add_argument("--log-path", type=Path)
    return root


def main() -> int:
    args = parser().parse_args()
    logger = configure_logging(args.log_path, args.verbose)
    if args.command == "index-vep":
        logger.info("Calculating VEP source checksum")
        source_sha256 = sha256_file(args.annotations_csv)
        result = ProteinConsequenceRsidBuilder.create_vep_index(
            args.annotations_csv,
            args.output_db,
            reset=args.reset,
            threads=args.threads,
            memory_limit=args.memory_limit,
            temp_directory=args.temp_directory,
            source_sha256=source_sha256,
            logger=logger,
        )
        logger.info("Calculating finalized VEP index checksum")
        result["output_sha256"] = sha256_file(args.output_db)
        manifest_path = args.output_db.parent / f"{args.output_db.name}.manifest.json"
        manifest_path.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        args.output_db.with_suffix(args.output_db.suffix + ".sha256").write_text(
            f"{result['output_sha256']}  {args.output_db.name}\n",
            encoding="utf-8",
        )
    else:
        genes = args.genes
        if args.gene_profile_index:
            if genes:
                raise ValueError("Use either --genes or --gene-profile-index, not both")
            genes = []
            with args.gene_profile_index.open("rt", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    symbol = str(row.get("symbol") or "").strip()
                    if not symbol:
                        raise ValueError(
                            f"Missing symbol at {args.gene_profile_index}:{line_number}"
                        )
                    genes.append(symbol)
        builder = ProteinConsequenceRsidBuilder(
            BuildInputs(
                variant_index_root=args.variant_index_root,
                vep_index_path=args.vep_index,
                output_root=args.output_root,
            ),
            logger=logger,
            progress_interval=args.progress_interval,
            workers=args.workers,
        )
        result = builder.run(genes, reset=args.reset, max_genes=args.max_genes)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
