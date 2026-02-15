"""Core DataHub pipeline primitives.

This package provides the reusable building blocks for DataHub ingestion,
validation, enrichment, storage and publication.
"""

from .config import (
    ASSOCIATION_AXIS_FIELDS,
    ASSOCIATION_CORE_FIELDS,
    DatasetContract,
    FieldPolicy,
    MissingFieldStrategy,
    SourcePriority,
    build_association_contract,
)
from .models import CanonicalRecord
from .pipeline import DataHubPipeline, DataHubRunReport
from .profiles import DatasetProfile, DatasetProfileLoader

__all__ = [
    "CanonicalRecord",
    "ASSOCIATION_AXIS_FIELDS",
    "ASSOCIATION_CORE_FIELDS",
    "DatasetContract",
    "DataHubPipeline",
    "DataHubRunReport",
    "DatasetProfile",
    "DatasetProfileLoader",
    "FieldPolicy",
    "MissingFieldStrategy",
    "SourcePriority",
    "build_association_contract",
]
