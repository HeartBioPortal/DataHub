#!/usr/bin/env python3
"""Export MVP inputs into standardized prepared-raw association CSV."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.adapters import MVPAssociationAdapter, PhenotypeMapper  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert MVP long-form rows into one prepared raw CSV per variant/phenotype."
    )
    parser.add_argument(
        "--input-path",
        action="append",
        dest="input_paths",
        required=True,
        help="Input file, directory, or glob. Repeat for multiple paths.",
    )
    parser.add_argument("--output-csv", required=True, help="Prepared output CSV.")
    parser.add_argument(
        "--dataset-id",
        default="hbp_mvp_association",
        help="Dataset identifier for adapter records.",
    )
    parser.add_argument(
        "--phenotype-map-json",
        default=None,
        help="Optional JSON map for phenotype->dataset/category overrides.",
    )
    parser.add_argument(
        "--fallback-dataset-type",
        default="CVD",
        help="Dataset type used when phenotype is not mapped.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="CSV chunk size for MVP ingest.",
    )
    return parser.parse_args()


def _load_mapper(args: argparse.Namespace) -> PhenotypeMapper:
    if args.phenotype_map_json:
        return PhenotypeMapper.from_json(
            args.phenotype_map_json,
            fallback_dataset_type=args.fallback_dataset_type,
        )
    try:
        return PhenotypeMapper.from_hbp_backend(
            fallback_dataset_type=args.fallback_dataset_type
        )
    except Exception:
        return PhenotypeMapper(mapping={}, fallback_dataset_type=args.fallback_dataset_type)


def _to_ancestry_payload(record: Any) -> str:
    allele = ""
    if isinstance(record.metadata, dict):
        allele = str(record.metadata.get("allele_string", "") or "")

    payload = {
        population: {
            "change": allele,
            "MAF": value,
        }
        for population, value in sorted(record.ancestry.items(), key=lambda item: item[0])
    }
    return json.dumps(payload, sort_keys=True)


def main() -> int:
    args = parse_args()
    mapper = _load_mapper(args)

    adapter = MVPAssociationAdapter(
        input_paths=args.input_paths,
        dataset_id=args.dataset_id,
        phenotype_mapper=mapper,
        chunksize=args.chunksize,
    )

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        "rsid",
        "pval",
        "gene",
        "phenotype",
        "functional_class",
        "var_class",
        "clinical_significance",
        "most_severe_consequence",
        "allele_string",
        "protein_start",
        "protein_end",
        "ancestry_data",
        "mvp_phenotype_key",
        "mvp_parent_phenotype",
        "mvp_is_sub_phenotype",
        "mvp_best_or",
        "mvp_best_ci",
        "mvp_best_q_pval",
        "mvp_best_r2",
        "mvp_best_i2",
        "mvp_best_direction",
        "mvp_best_num_samples",
        "mvp_best_num_cases",
        "mvp_best_num_controls",
        "mvp_best_case_af",
        "mvp_best_control_af",
        "source",
    ]

    row_count = 0
    with output_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()

        for record in adapter.read():
            metadata = record.metadata if isinstance(record.metadata, dict) else {}
            writer.writerow(
                {
                    "rsid": record.variant_id,
                    "pval": record.p_value if record.p_value is not None else "",
                    "gene": record.gene_id,
                    "phenotype": record.phenotype,
                    "functional_class": "",
                    "var_class": record.variation_type or "",
                    "clinical_significance": record.clinical_significance or "",
                    "most_severe_consequence": record.most_severe_consequence or "",
                    "allele_string": metadata.get("allele_string", ""),
                    "protein_start": "",
                    "protein_end": "",
                    "ancestry_data": _to_ancestry_payload(record),
                    "mvp_phenotype_key": metadata.get("phenotype_key", ""),
                    "mvp_parent_phenotype": metadata.get("parent_phenotype", ""),
                    "mvp_is_sub_phenotype": metadata.get("is_sub_phenotype", ""),
                    "mvp_best_or": metadata.get("best_or", ""),
                    "mvp_best_ci": json.dumps(metadata.get("best_ci", {}), sort_keys=True)
                    if metadata.get("best_ci")
                    else "",
                    "mvp_best_q_pval": metadata.get("best_q_pval", ""),
                    "mvp_best_r2": metadata.get("best_r2", ""),
                    "mvp_best_i2": metadata.get("best_i2", ""),
                    "mvp_best_direction": metadata.get("best_direction", ""),
                    "mvp_best_num_samples": metadata.get("best_num_samples", ""),
                    "mvp_best_num_cases": metadata.get("best_num_cases", ""),
                    "mvp_best_num_controls": metadata.get("best_num_controls", ""),
                    "mvp_best_case_af": metadata.get("best_case_af", ""),
                    "mvp_best_control_af": metadata.get("best_control_af", ""),
                    "source": record.source,
                }
            )
            row_count += 1

    print(json.dumps({"output_csv": str(output_path), "rows": row_count}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
