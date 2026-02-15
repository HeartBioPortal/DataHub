"""Base interface for all DataHub ingestion adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from datahub.models import CanonicalRecord


class DataAdapter(ABC):
    """Adapter that converts a source dataset into canonical records."""

    name: str

    @abstractmethod
    def read(self) -> Iterable[CanonicalRecord]:
        """Yield canonical records from the adapter source."""
