"""Base class for canonical record storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from datahub.models import CanonicalRecord


class CanonicalStorage(ABC):
    """Persists normalized records for reproducible pipelines."""

    @abstractmethod
    def persist(self, records: Sequence[CanonicalRecord]) -> None:
        """Persist records in backend-specific format."""
