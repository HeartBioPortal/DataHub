"""Redis publisher that delegates to the existing DataManager exporter implementation."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

from datahub.models import CanonicalRecord
from datahub.publishers.base import Publisher


class LegacyRedisPublisher(Publisher):
    """Load DataHub-produced association JSON into legacy Redis caches.

    This intentionally reuses ``analyzer/DataManager/src/redis/exporter.py`` so
    DataHub can keep DataManager runtime behavior intact during migration.
    """

    def __init__(self, *, output_root: str | Path, strict: bool = False) -> None:
        self.output_root = Path(output_root)
        self.strict = strict

    def publish(self, records: list[CanonicalRecord]) -> None:  # noqa: ARG002
        try:
            asyncio.run(self._publish())
        except Exception as exc:
            if self.strict:
                raise
            print(f"LegacyRedisPublisher warning: {exc}", file=sys.stderr)

    async def _publish(self) -> None:
        exporter_cls = self._load_exporter_class()
        exporter = exporter_cls(
            aggregate_path=str(self.output_root / "association" / "final" / "association"),
            overall_path=str(self.output_root / "association" / "final" / "overall"),
        )
        await exporter.run()

    @staticmethod
    def _load_exporter_class():
        data_manager_src = Path(__file__).resolve().parents[4] / "DataManager" / "src"
        exporter_path = data_manager_src / "redis" / "exporter.py"

        src_path = str(data_manager_src)
        if src_path not in sys.path:
            # Legacy exporter imports sibling modules as top-level packages.
            sys.path.insert(0, src_path)

        spec = importlib.util.spec_from_file_location("legacy_redis_exporter", exporter_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load legacy exporter from {exporter_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.Exporter
