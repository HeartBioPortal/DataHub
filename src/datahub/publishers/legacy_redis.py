"""Redis publisher that delegates to the existing DataManager exporter implementation."""

from __future__ import annotations

import asyncio
import gzip
import importlib.util
import shutil
import sys
import tempfile
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
        aggregate_path = self.output_root / "association" / "final" / "association"
        overall_path = self.output_root / "association" / "final" / "overall"

        temp_dir: tempfile.TemporaryDirectory[str] | None = None
        if self._should_materialize_json_view(aggregate_path, overall_path):
            temp_dir = tempfile.TemporaryDirectory(prefix="datahub_redis_json_view_")
            temp_root = Path(temp_dir.name)
            aggregate_path = temp_root / "association"
            overall_path = temp_root / "overall"
            self._materialize_json_view(
                source_aggregate=self.output_root / "association" / "final" / "association",
                source_overall=self.output_root / "association" / "final" / "overall",
                target_aggregate=aggregate_path,
                target_overall=overall_path,
            )

        exporter = exporter_cls(
            aggregate_path=str(aggregate_path),
            overall_path=str(overall_path),
        )
        try:
            await exporter.run()
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    @staticmethod
    def _should_materialize_json_view(aggregate_path: Path, overall_path: Path) -> bool:
        has_plain_json = any(aggregate_path.rglob("*.json")) or any(overall_path.rglob("*.json"))
        has_gzip_json = any(aggregate_path.rglob("*.json.gz")) or any(overall_path.rglob("*.json.gz"))
        return has_gzip_json and not has_plain_json

    @staticmethod
    def _materialize_json_view(
        *,
        source_aggregate: Path,
        source_overall: Path,
        target_aggregate: Path,
        target_overall: Path,
    ) -> None:
        LegacyRedisPublisher._copy_json_tree(source_aggregate, target_aggregate)
        LegacyRedisPublisher._copy_json_tree(source_overall, target_overall)

    @staticmethod
    def _copy_json_tree(source_root: Path, target_root: Path) -> None:
        for source_file in source_root.rglob("*"):
            if not source_file.is_file():
                continue
            if source_file.name.endswith(".json.gz"):
                relative = source_file.relative_to(source_root)
                relative_json = Path(str(relative)[:-3])  # drop trailing ".gz"
                target_file = target_root / relative_json
                target_file.parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(source_file, "rb") as source_stream:
                    with target_file.open("wb") as target_stream:
                        shutil.copyfileobj(source_stream, target_stream)
                continue

            if source_file.name.endswith(".json"):
                relative = source_file.relative_to(source_root)
                target_file = target_root / relative
                target_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_file, target_file)

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
