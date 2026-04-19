"""Summary payload shaping for association serving DuckDB artifacts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def _copy_if_mapping(value: Any) -> Any:
    if isinstance(value, dict):
        return deepcopy(value)
    return value


def shape_overall_summary(payload: Any) -> Any:
    """Return the lightweight overall payload needed by search summary APIs."""

    if not isinstance(payload, dict):
        return payload

    result: dict[str, Any] = {}
    data = payload.get("data")
    if isinstance(data, dict):
        result["data"] = {
            key: deepcopy(data[key])
            for key in ("vc", "msc", "cs")
            if key in data
        }
    if "pvals" in payload:
        result["pvals"] = _copy_if_mapping(payload.get("pvals"))
    if "_datahub" in payload:
        result["_datahub"] = _copy_if_mapping(payload.get("_datahub"))
    return result


def shape_association_summary(records: Any, *, name_key: str) -> Any:
    """Return the lightweight association records needed by search summary APIs."""

    if not isinstance(records, list):
        return records

    shaped = []
    for record in records:
        if not isinstance(record, dict):
            continue
        item: dict[str, Any] = {}
        if name_key in record:
            item[name_key] = deepcopy(record[name_key])
        for key in ("vc", "msc", "cs", "pvals"):
            if key in record:
                item[key] = deepcopy(record[key])
        if "_datahub" in record:
            item["_datahub"] = _copy_if_mapping(record.get("_datahub"))
        shaped.append(item)
    return shaped


def shape_association_summary_for_dataset(records: Any, *, dataset_type: str) -> Any:
    normalized_dataset_type = str(dataset_type or "").strip().upper()
    name_key = "trait" if normalized_dataset_type == "TRAIT" else "disease"
    return shape_association_summary(records, name_key=name_key)


def shape_summary_for_table(
    payload: Any,
    *,
    source_table: str,
    dataset_type: str,
) -> Any:
    """Shape a full serving payload into its API-summary equivalent."""

    if source_table == "association_gene_payloads":
        return shape_association_summary_for_dataset(payload, dataset_type=dataset_type)
    if source_table == "overall_gene_payloads":
        return shape_overall_summary(payload)
    raise ValueError(f"Unsupported source table for summary shaping: {source_table}")
