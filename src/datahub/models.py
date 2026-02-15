"""Canonical in-memory data models used by DataHub."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CanonicalRecord:
    """Single normalized variant association record.

    Fields are intentionally broad to support both current association views and
    future modalities that may attach additional payload under ``metadata``.
    """

    dataset_id: str
    dataset_type: str
    source: str
    gene_id: str
    variant_id: str
    phenotype: str
    disease_category: str | None = None
    variation_type: str | None = None
    clinical_significance: str | None = None
    most_severe_consequence: str | None = None
    p_value: float | None = None
    pmid: str | None = None
    ancestry: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def key(self) -> tuple[str, str, str, str]:
        """Stable key for deduplicating/updating records during enrichment."""

        return (self.dataset_type, self.gene_id, self.variant_id, self.phenotype)

    def to_row(self) -> dict[str, Any]:
        """Serialize into a plain dict for storage backends."""

        return {
            "dataset_id": self.dataset_id,
            "dataset_type": self.dataset_type,
            "source": self.source,
            "gene_id": self.gene_id,
            "variant_id": self.variant_id,
            "phenotype": self.phenotype,
            "disease_category": self.disease_category,
            "variation_type": self.variation_type,
            "clinical_significance": self.clinical_significance,
            "most_severe_consequence": self.most_severe_consequence,
            "p_value": self.p_value,
            "pmid": self.pmid,
            "ancestry": self.ancestry,
            "metadata": self.metadata,
        }
