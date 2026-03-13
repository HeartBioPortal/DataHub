"""Phenotype hierarchy path resolution utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datahub.adapters.phenotypes import PhenotypeMapper


def _normalize_dataset_type(value: str | None) -> str:
    return str(value or "").strip().upper()


def _normalize_path_segments(segments: Any) -> tuple[str, ...]:
    if not isinstance(segments, (list, tuple)):
        return ()

    normalized = [
        PhenotypeMapper.normalize(segment)
        for segment in segments
        if segment is not None and str(segment).strip()
    ]
    return tuple(segment for segment in normalized if segment)


def _walk_tree(
    *,
    dataset_type: str,
    node: Any,
    current_path: tuple[str, ...],
    node_paths: dict[tuple[str, str], tuple[str, ...]],
    leaf_paths: dict[tuple[str, str], tuple[str, ...]],
) -> None:
    if isinstance(node, dict):
        for label, child in node.items():
            normalized_label = PhenotypeMapper.normalize(str(label))
            if not normalized_label:
                continue

            next_path = current_path + (normalized_label,)
            node_paths[(dataset_type, normalized_label)] = next_path
            _walk_tree(
                dataset_type=dataset_type,
                node=child,
                current_path=next_path,
                node_paths=node_paths,
                leaf_paths=leaf_paths,
            )
        return

    if isinstance(node, list):
        for item in node:
            if isinstance(item, dict):
                _walk_tree(
                    dataset_type=dataset_type,
                    node=item,
                    current_path=current_path,
                    node_paths=node_paths,
                    leaf_paths=leaf_paths,
                )
                continue

            leaf = PhenotypeMapper.normalize(str(item))
            if not leaf:
                continue

            leaf_paths.setdefault((dataset_type, leaf), current_path + (leaf,))
        return


@dataclass(frozen=True)
class PhenotypePathResolver:
    """Resolve phenotype/category labels into canonical path arrays."""

    leaf_paths: dict[tuple[str, str], tuple[str, ...]]
    node_paths: dict[tuple[str, str], tuple[str, ...]]

    @classmethod
    def from_tree(cls, tree: dict[str, Any]) -> "PhenotypePathResolver":
        leaf_paths: dict[tuple[str, str], tuple[str, ...]] = {}
        node_paths: dict[tuple[str, str], tuple[str, ...]] = {}

        for dataset_type, node in tree.items():
            normalized_dtype = _normalize_dataset_type(str(dataset_type))
            if not normalized_dtype:
                continue

            _walk_tree(
                dataset_type=normalized_dtype,
                node=node,
                current_path=(),
                node_paths=node_paths,
                leaf_paths=leaf_paths,
            )

        return cls(leaf_paths=leaf_paths, node_paths=node_paths)

    @classmethod
    def from_tree_json(cls, tree_json_path: str | Path) -> "PhenotypePathResolver":
        payload = json.loads(Path(tree_json_path).read_text())
        if not isinstance(payload, dict):
            raise ValueError("phenotype tree payload must be a JSON object")
        return cls.from_tree(payload)

    def resolve_leaf_path(
        self,
        *,
        dataset_type: str,
        phenotype: str | None,
        fallback_category: str | None = None,
        fallback_path: Any = None,
    ) -> tuple[str, ...]:
        normalized_dtype = _normalize_dataset_type(dataset_type)
        normalized_phenotype = PhenotypeMapper.normalize(phenotype)
        if normalized_dtype and normalized_phenotype:
            mapped = self.leaf_paths.get((normalized_dtype, normalized_phenotype))
            if mapped:
                return mapped

        normalized_fallback_path = _normalize_path_segments(fallback_path)
        if normalized_fallback_path:
            if normalized_phenotype and normalized_fallback_path[-1] != normalized_phenotype:
                return normalized_fallback_path + (normalized_phenotype,)
            return normalized_fallback_path

        normalized_category = PhenotypeMapper.normalize(fallback_category)
        if normalized_category and normalized_phenotype and normalized_category != normalized_phenotype:
            return (normalized_category, normalized_phenotype)
        if normalized_phenotype:
            return (normalized_phenotype,)
        if normalized_category:
            return (normalized_category,)
        return ()

    def resolve_node_path(
        self,
        *,
        dataset_type: str,
        node_label: str | None,
        fallback_path: Any = None,
    ) -> tuple[str, ...]:
        normalized_dtype = _normalize_dataset_type(dataset_type)
        normalized_label = PhenotypeMapper.normalize(node_label)
        if normalized_dtype and normalized_label:
            mapped = self.node_paths.get((normalized_dtype, normalized_label))
            if mapped:
                return mapped

        normalized_fallback_path = _normalize_path_segments(fallback_path)
        if normalized_fallback_path:
            return normalized_fallback_path
        if normalized_label:
            return (normalized_label,)
        return ()
