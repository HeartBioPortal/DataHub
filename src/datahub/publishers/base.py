"""Publisher interface for DataHub outputs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from datahub.models import CanonicalRecord


class Publisher(ABC):
    """Publishes canonical records into consumer-facing artifacts."""

    @abstractmethod
    def publish(self, records: Sequence[CanonicalRecord]) -> None:
        """Publish records into output targets."""
