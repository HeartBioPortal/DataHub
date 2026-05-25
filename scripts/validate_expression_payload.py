#!/usr/bin/env python3
"""Validate HeartBioPortal expression summary payloads.

This script is intentionally lightweight: it validates the legacy compact
`expression.json` payload today, and can compare it with a future row-level
source CSV when one is available.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_REGULATION_KEYS = {"upregulated", "downregulated"}
SOURCE_GENE_COLUMNS = ("Gene.symbol", "gene_symbol", "gene_id")
SOURCE_DISEASE_COLUMNS = ("disease", "diseases_associated", "disease_name", "phenotype_label_normalized")
SOURCE_REGULATION_COLUMNS = ("regulation", "direction")
SOURCE_PROVENANCE_COLUMNS = {
    "gse_id",
    "study_accession",
    "source_database",
    "source_url",
    "pmid",
    "platform",
    "tissue",
    "assay_type",
    "n_case",
    "n_control",
}


def load_json(path: Path) -> Any:
    with path.open() as handle:
        return json.load(handle)


def normalize_regulation(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"up", "upregulated", "up-regulated"}:
        return "upregulated"
    if text in {"down", "downregulated", "down-regulated"}:
        return "downregulated"
    if text in {"not_significant", "not significant", "unchanged"}:
        return "not_significant"
    return text


def validate_payload_shape(payload: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "gene_count": 0,
        "disease_context_count": 0,
        "regulation_keys": {},
        "problems": [],
        "example_genes": [],
    }

    if not isinstance(payload, dict):
        summary["problems"].append("Payload root is not an object.")
        return summary

    regulation_keys: Counter[str] = Counter()
    disease_contexts: set[str] = set()
    example_genes: list[str] = []

    for gene, disease_map in payload.items():
        if len(example_genes) < 5:
            example_genes.append(str(gene))
        if not isinstance(gene, str) or not gene.strip():
            summary["problems"].append("Found blank or non-string gene key.")
        if "," in str(gene):
            summary["problems"].append(f"Gene key contains comma and may be skipped by serving builders: {gene}")
        if not isinstance(disease_map, dict):
            summary["problems"].append(f"Gene {gene} does not map to a disease object.")
            continue
        for disease, regulation_map in disease_map.items():
            disease_contexts.add(str(disease))
            if not isinstance(regulation_map, dict):
                summary["problems"].append(f"Gene {gene} disease {disease} does not map to a regulation object.")
                continue
            for key, value in regulation_map.items():
                regulation_keys[str(key)] += 1
                if str(key) not in DEFAULT_REGULATION_KEYS:
                    summary["problems"].append(
                        f"Unexpected regulation key {key!r} for gene {gene} disease {disease}."
                    )
                if not isinstance(value, (int, float)) or value < 0:
                    summary["problems"].append(
                        f"Invalid count for gene {gene} disease {disease} regulation {key}: {value!r}."
                    )

    summary["gene_count"] = len(payload)
    summary["disease_context_count"] = len(disease_contexts)
    summary["regulation_keys"] = dict(regulation_keys)
    summary["example_genes"] = example_genes
    return summary


def load_source_counts(path: Path) -> tuple[dict[tuple[str, str], Counter[str]], dict[str, Any]]:
    counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    metadata = {
        "row_count": 0,
        "columns": [],
        "missing_required_columns": [],
        "available_provenance_columns": [],
        "missing_provenance_columns": [],
        "rows_missing_accession": 0,
    }

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        metadata["columns"] = columns
        gene_column = next((col for col in SOURCE_GENE_COLUMNS if col in columns), None)
        disease_column = next((col for col in SOURCE_DISEASE_COLUMNS if col in columns), None)
        regulation_column = next((col for col in SOURCE_REGULATION_COLUMNS if col in columns), None)
        missing_required = [
            label
            for label, selected in (
                ("gene", gene_column),
                ("disease", disease_column),
                ("regulation_or_direction", regulation_column),
            )
            if selected is None
        ]
        metadata["missing_required_columns"] = missing_required
        metadata["selected_columns"] = {
            "gene": gene_column,
            "disease": disease_column,
            "regulation": regulation_column,
        }
        metadata["available_provenance_columns"] = sorted(SOURCE_PROVENANCE_COLUMNS & set(columns))
        metadata["missing_provenance_columns"] = sorted(SOURCE_PROVENANCE_COLUMNS - set(columns))
        if missing_required:
            return counts, metadata

        accession_columns = [col for col in ("gse_id", "study_accession") if col in columns]
        for row in reader:
            metadata["row_count"] += 1
            gene = str(row.get(gene_column or "") or "").strip()
            disease = str(row.get(disease_column or "") or "").strip()
            regulation = normalize_regulation(row.get(regulation_column or "") or "")
            if not gene or not disease or not regulation:
                continue
            if accession_columns and not any(str(row.get(col) or "").strip() for col in accession_columns):
                metadata["rows_missing_accession"] += 1
            counts[(gene, disease)][regulation] += 1

    return counts, metadata


def compare_payload_to_source(
    payload: dict[str, Any],
    source_counts: dict[tuple[str, str], Counter[str]],
    *,
    max_examples: int = 25,
) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    compared = 0

    for gene, disease_map in payload.items():
        if not isinstance(disease_map, dict):
            continue
        for disease, regulation_map in disease_map.items():
            if not isinstance(regulation_map, dict):
                continue
            compared += 1
            expected = Counter({normalize_regulation(k): int(v or 0) for k, v in regulation_map.items()})
            observed = source_counts.get((str(gene), str(disease)), Counter())
            for key in sorted(set(expected) | set(observed)):
                if expected.get(key, 0) != observed.get(key, 0):
                    if len(mismatches) < max_examples:
                        mismatches.append(
                            {
                                "gene": gene,
                                "disease": disease,
                                "regulation": key,
                                "payload_count": expected.get(key, 0),
                                "source_count": observed.get(key, 0),
                            }
                        )
                    break

    return {
        "compared_gene_disease_pairs": compared,
        "mismatch_count_capped": len(mismatches),
        "mismatch_examples": mismatches,
    }


def compare_expression_payloads(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    max_examples: int = 25,
) -> dict[str, Any]:
    examples: list[dict[str, Any]] = []
    baseline_pairs = {
        (str(gene), str(disease))
        for gene, disease_map in baseline.items()
        if isinstance(disease_map, dict)
        for disease in disease_map
    }
    candidate_pairs = {
        (str(gene), str(disease))
        for gene, disease_map in candidate.items()
        if isinstance(disease_map, dict)
        for disease in disease_map
    }

    for gene, disease in sorted(baseline_pairs | candidate_pairs):
        base_counts = baseline.get(gene, {}).get(disease, {}) if isinstance(baseline.get(gene), dict) else {}
        cand_counts = candidate.get(gene, {}).get(disease, {}) if isinstance(candidate.get(gene), dict) else {}
        normalized_base = {normalize_regulation(k): int(v or 0) for k, v in base_counts.items()}
        normalized_candidate = {normalize_regulation(k): int(v or 0) for k, v in cand_counts.items()}
        if normalized_base != normalized_candidate and len(examples) < max_examples:
            examples.append(
                {
                    "gene": gene,
                    "disease": disease,
                    "baseline": normalized_base,
                    "candidate": normalized_candidate,
                }
            )

    return {
        "baseline_gene_count": len(baseline),
        "candidate_gene_count": len(candidate),
        "baseline_gene_disease_pairs": len(baseline_pairs),
        "candidate_gene_disease_pairs": len(candidate_pairs),
        "shared_gene_disease_pairs": len(baseline_pairs & candidate_pairs),
        "baseline_only_gene_disease_pairs": len(baseline_pairs - candidate_pairs),
        "candidate_only_gene_disease_pairs": len(candidate_pairs - baseline_pairs),
        "difference_examples": examples,
    }


def build_report(
    expression_json: Path,
    source_rows: Path | None,
    candidate_expression_json: Path | None = None,
) -> dict[str, Any]:
    payload = load_json(expression_json)
    report = {
        "expression_json": str(expression_json),
        "payload": validate_payload_shape(payload),
        "traceability": {
            "source_rows": str(source_rows) if source_rows else None,
            "status": "UNKNOWN",
            "problems": [],
        },
    }

    if source_rows is None:
        report["traceability"]["problems"].append(
            "No row-level source file was provided, so displayed counts cannot be traced to studies."
        )
        if candidate_expression_json is not None and candidate_expression_json.exists():
            report["candidate_comparison"] = compare_expression_payloads(
                payload,
                load_json(candidate_expression_json),
            )
        return report

    if not source_rows.exists():
        report["traceability"]["problems"].append(f"Source rows file does not exist: {source_rows}")
        return report

    source_counts, source_metadata = load_source_counts(source_rows)
    report["traceability"]["source_metadata"] = source_metadata
    if source_metadata["missing_required_columns"]:
        report["traceability"]["problems"].append(
            "Source rows are missing required columns: "
            + ", ".join(source_metadata["missing_required_columns"])
        )
        return report

    if source_metadata["rows_missing_accession"]:
        report["traceability"]["problems"].append(
            f"{source_metadata['rows_missing_accession']} source rows lack a study accession."
        )

    report["traceability"]["comparison"] = compare_payload_to_source(payload, source_counts)
    report["traceability"]["status"] = (
        "PASS" if not report["traceability"]["problems"] else "WARN"
    )
    if candidate_expression_json is not None and candidate_expression_json.exists():
        report["candidate_comparison"] = compare_expression_payloads(
            payload,
            load_json(candidate_expression_json),
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expression-json",
        required=True,
        type=Path,
        help="Path to legacy expression.json payload.",
    )
    parser.add_argument(
        "--source-rows",
        type=Path,
        help="Optional row-level source CSV used to recreate the payload.",
    )
    parser.add_argument(
        "--candidate-expression-json",
        type=Path,
        help="Optional rebuilt expression summary JSON to compare against the baseline payload.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON report path. Defaults to stdout.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when shape or traceability problems are found.",
    )
    args = parser.parse_args()

    report = build_report(
        args.expression_json,
        args.source_rows,
        args.candidate_expression_json,
    )
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(text + "\n")
    else:
        print(text)

    has_problems = bool(report["payload"]["problems"] or report["traceability"]["problems"])
    return 1 if args.strict and has_problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
