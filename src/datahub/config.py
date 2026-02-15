"""Configuration contracts for DataHub pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class MissingFieldStrategy(str, Enum):
    """How DataHub should handle missing values for a field."""

    EXCLUDE = "exclude"
    UNKNOWN = "unknown"
    ALLOW = "allow"


@dataclass(frozen=True)
class FieldPolicy:
    """Validation and missing-value policy for a canonical field."""

    required: bool = False
    missing_strategy: MissingFieldStrategy = MissingFieldStrategy.EXCLUDE
    unknown_value: str = "Unknown"


@dataclass(frozen=True)
class DatasetContract:
    """Field-level contract for a specific dataset modality."""

    dataset_type: str
    field_policies: Mapping[str, FieldPolicy] = field(default_factory=dict)

    def policy_for(self, field_name: str) -> FieldPolicy:
        """Return the policy for a field, defaulting to optional/exclude behavior."""

        return self.field_policies.get(field_name, FieldPolicy())


@dataclass(frozen=True)
class SourcePriority:
    """Preferred source order per field for enrichment conflict resolution."""

    priorities: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def for_field(self, field_name: str) -> tuple[str, ...]:
        """Return source precedence for a field."""

        return self.priorities.get(field_name, ())


ASSOCIATION_CORE_FIELDS: tuple[str, ...] = (
    "gene_id",
    "variant_id",
    "phenotype",
)

ASSOCIATION_AXIS_FIELDS: tuple[str, ...] = (
    "variation_type",
    "clinical_significance",
    "most_severe_consequence",
)


def build_association_contract(
    *,
    dataset_type: str,
    required_fields: set[str] | None = None,
    axis_missing_strategy: MissingFieldStrategy = MissingFieldStrategy.EXCLUDE,
) -> DatasetContract:
    """Construct a contract for association-style datasets.

    ``required_fields`` can be tuned per dataset type, which keeps community
    submission constraints configurable while enforcing a stable baseline.
    """

    required = set(required_fields or ASSOCIATION_CORE_FIELDS)
    policies: dict[str, FieldPolicy] = {}

    for field_name in ASSOCIATION_CORE_FIELDS:
        policies[field_name] = FieldPolicy(
            required=field_name in required,
            missing_strategy=MissingFieldStrategy.EXCLUDE,
        )

    for field_name in ASSOCIATION_AXIS_FIELDS:
        policies[field_name] = FieldPolicy(
            required=field_name in required,
            missing_strategy=axis_missing_strategy,
            unknown_value="Unknown",
        )

    return DatasetContract(dataset_type=dataset_type, field_policies=policies)
