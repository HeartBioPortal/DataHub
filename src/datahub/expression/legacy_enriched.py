"""Build a legacy expression payload with CardioQuilt provenance summaries."""

from __future__ import annotations

import csv
import gzip
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any


def normalize_legacy_key(value: object) -> str:
    return str(value or "").strip().lower().replace("/", " ").replace("-", " ").replace(" ", "_")


def _safe_float(value: object) -> float | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"na", "nan", "none"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _direction(value: object, logfc: float | None = None) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"up", "upregulated", "up_regulated"}:
        return "up"
    if text in {"down", "downregulated", "down_regulated"}:
        return "down"
    if logfc is not None:
        if logfc > 0:
            return "up"
        if logfc < 0:
            return "down"
    return "not_significant"


def _count_value(regulation: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = regulation.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _source_url(accession: str) -> str:
    return f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}"


def _summarize_cardioquilt_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_direction: dict[str, set[str]] = {"up": set(), "down": set()}
    logfc_values: list[float] = []
    adjusted_values: list[float] = []
    study_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        accession = str(row.get("study_accession") or "").strip()
        direction = str(row.get("direction") or "")
        if direction in by_direction and accession:
            by_direction[direction].add(accession)
        logfc = row.get("log2_fold_change")
        adjusted = row.get("adjusted_p_value")
        if isinstance(logfc, (int, float)):
            logfc_values.append(float(logfc))
        if isinstance(adjusted, (int, float)):
            adjusted_values.append(float(adjusted))
        if accession and accession not in study_rows:
            study_rows[accession] = {
                "study_accession": accession,
                "source_url": row.get("source_url") or _source_url(accession),
                "pmid": row.get("pmid") or "",
            }

    studies = sorted(study_rows)
    return {
        "source_database": "GEO/CREEDS/CardioQuilt",
        "source_row_count": len(rows),
        "source_study_count": len(studies),
        "source_studies": studies,
        "source_study_links": [study_rows[study] for study in studies],
        "up_study_count": len(by_direction["up"]),
        "down_study_count": len(by_direction["down"]),
        "minimum_adjusted_p_value": min(adjusted_values) if adjusted_values else None,
        "median_log2_fold_change": median(logfc_values) if logfc_values else None,
    }


def read_cardioquilt_detail_index(
    path: str | Path,
    *,
    adjusted_p_value_threshold: float = 0.05,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    with Path(path).open(newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            gene = str(raw.get("Gene.symbol") or "").strip()
            disease = str(raw.get("diseases_associated") or "").strip()
            accession = str(raw.get("gse_id") or "").strip()
            if not gene or not disease or not accession:
                continue
            adjusted = _safe_float(raw.get("adj.P.Val"))
            if adjusted is not None and adjusted >= adjusted_p_value_threshold:
                continue
            logfc = _safe_float(raw.get("logFC"))
            direction = _direction(raw.get("regulation"), logfc)
            index[(gene.upper(), normalize_legacy_key(disease))].append(
                {
                    "study_accession": accession,
                    "source_url": _source_url(accession),
                    "pmid": str(raw.get("pmid") or "").strip(),
                    "disease_name": disease,
                    "phenotype_label_normalized": normalize_legacy_key(disease),
                    "direction": direction,
                    "log2_fold_change": logfc,
                    "adjusted_p_value": adjusted,
                }
            )
    return dict(index)


def build_enriched_legacy_expression_payload(
    *,
    expression_json_path: str | Path,
    cardioquilt_csv_path: str | Path,
    include_cardioquilt_only: bool = True,
    adjusted_p_value_threshold: float = 0.05,
) -> dict[str, dict[str, dict[str, Any]]]:
    expression_path = Path(expression_json_path)
    if expression_path.suffix == ".gz":
        with gzip.open(expression_path, "rt", encoding="utf-8") as handle:
            current_payload = json.load(handle)
    else:
        current_payload = json.loads(expression_path.read_text())
    cardioquilt_index = read_cardioquilt_detail_index(
        cardioquilt_csv_path,
        adjusted_p_value_threshold=adjusted_p_value_threshold,
    )

    enriched: dict[str, dict[str, dict[str, Any]]] = {}
    seen_keys: set[tuple[str, str]] = set()
    for gene, disease_payload in current_payload.items():
        if not isinstance(disease_payload, dict):
            continue
        gene_key = str(gene).upper()
        enriched[str(gene)] = {}
        for disease, regulation in disease_payload.items():
            if not isinstance(regulation, dict):
                continue
            disease_key = normalize_legacy_key(disease)
            rows = cardioquilt_index.get((gene_key, disease_key), [])
            up = _count_value(regulation, "up", "upregulated")
            down = _count_value(regulation, "down", "downregulated")
            details = _summarize_cardioquilt_rows(rows) if rows else {
                "source_database": "legacy_expression_json",
                "source_row_count": 0,
                "source_study_count": 0,
                "source_studies": [],
                "source_study_links": [],
                "up_study_count": 0,
                "down_study_count": 0,
                "minimum_adjusted_p_value": None,
                "median_log2_fold_change": None,
            }
            enriched[str(gene)][str(disease)] = {
                "up": up,
                "down": down,
                "upregulated": up,
                "downregulated": down,
                "total": up + down,
                "provenance_status": "matched_cardioquilt" if rows else "summary_only",
                **details,
            }
            seen_keys.add((gene_key, disease_key))

    if include_cardioquilt_only:
        for (gene, disease_key), rows in sorted(cardioquilt_index.items()):
            if (gene, disease_key) in seen_keys:
                continue
            details = _summarize_cardioquilt_rows(rows)
            up = int(details["up_study_count"])
            down = int(details["down_study_count"])
            enriched.setdefault(gene, {})[disease_key] = {
                "up": up,
                "down": down,
                "upregulated": up,
                "downregulated": down,
                "total": up + down,
                "provenance_status": "cardioquilt_reconstruction_only",
                **details,
            }

    return {gene: enriched[gene] for gene in sorted(enriched)}


def write_enriched_legacy_expression_payload(
    *,
    expression_json_path: str | Path,
    cardioquilt_csv_path: str | Path,
    output_json: str | Path,
    include_cardioquilt_only: bool = True,
    adjusted_p_value_threshold: float = 0.05,
) -> dict[str, Any]:
    payload = build_enriched_legacy_expression_payload(
        expression_json_path=expression_json_path,
        cardioquilt_csv_path=cardioquilt_csv_path,
        include_cardioquilt_only=include_cardioquilt_only,
        adjusted_p_value_threshold=adjusted_p_value_threshold,
    )
    output = Path(output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix == ".gz":
        with gzip.open(output, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
    else:
        output.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")
    phenotype_pairs = sum(len(value) for value in payload.values())
    matched_pairs = sum(
        1
        for disease_payload in payload.values()
        for value in disease_payload.values()
        if value.get("provenance_status") == "matched_cardioquilt"
    )
    return {
        "output_json": str(output),
        "gene_count": len(payload),
        "gene_phenotype_count": phenotype_pairs,
        "matched_cardioquilt_pairs": matched_pairs,
        "include_cardioquilt_only": include_cardioquilt_only,
    }
