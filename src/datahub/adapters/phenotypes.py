"""Phenotype normalization and dataset-type resolution utilities."""

from __future__ import annotations

import importlib.util
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class PhenotypeMapper:
    """Resolve a phenotype string into DataHub dataset-type and category."""

    mapping: dict[str, tuple[str, str]]
    fallback_dataset_type: str = "CVD"
    fallback_category: str = ""

    @staticmethod
    def normalize(value: str | None) -> str:
        """Normalize phenotype/category labels to a stable slug form."""

        if value is None:
            return ""

        cleaned = str(value).strip().replace("/", "_")
        cleaned = re.sub(r"\s+", "_", cleaned)
        return cleaned.lower()

    def resolve(self, phenotype: str | None) -> tuple[str, str]:
        """Return ``(dataset_type, category)`` for a phenotype."""

        key = self.normalize(phenotype)
        if not key:
            return self.fallback_dataset_type, self.fallback_category

        return self.mapping.get(key, (self.fallback_dataset_type, self.fallback_category))

    @classmethod
    def from_hbp_backend(
        cls,
        general_py_path: str | Path | None = None,
        *,
        fallback_dataset_type: str = "CVD",
    ) -> "PhenotypeMapper":
        """Build mapping from ``analyzer/config/general.py`` PHENOTYPE_TREE.

        This keeps DataHub compatible with the existing HBP phenotype routing
        while still allowing alternate mappings in open-source deployments.
        """

        if general_py_path is None:
            # .../analyzer/DataHub/src/datahub/adapters/phenotypes.py -> .../analyzer
            general_py_path = Path(__file__).resolve().parents[4] / "config" / "general.py"

        module = _load_module(Path(general_py_path))
        tree: dict[str, dict[str, list[str]]] = getattr(module, "PHENOTYPE_TREE", {})

        mapping: dict[str, tuple[str, str]] = {}
        for dataset_type, categories in tree.items():
            for category, phenotypes in categories.items():
                category_slug = cls.normalize(category)
                for phenotype in phenotypes:
                    mapping[cls.normalize(phenotype)] = (dataset_type.upper(), category_slug)

        return cls(mapping=mapping, fallback_dataset_type=fallback_dataset_type)

    @classmethod
    def from_json(
        cls,
        json_path: str | Path,
        *,
        fallback_dataset_type: str = "CVD",
    ) -> "PhenotypeMapper":
        """Build mapping from a JSON file.

        Expected format:
        ``{"phenotype_slug": {"dataset_type": "CVD", "category": "..."}}``.
        """

        path = Path(json_path)
        payload = json.loads(path.read_text())
        mapping: dict[str, tuple[str, str]] = {}

        for phenotype, details in payload.items():
            mapping[cls.normalize(phenotype)] = (
                str(details["dataset_type"]).upper(),
                cls.normalize(details.get("category", "")),
            )

        return cls(mapping=mapping, fallback_dataset_type=fallback_dataset_type)


def _load_module(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location("hbp_general", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
