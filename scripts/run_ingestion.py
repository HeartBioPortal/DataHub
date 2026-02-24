#!/usr/bin/env python3
"""Run configurable DataHub ingestion pipelines via adapter registry."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub import (  # noqa: E402
    AdapterPluginSpec,
    DataHubPipeline,
    DatasetProfileLoader,
    SourceConnectorPluginSpec,
    SourceManifestLoader,
    SourcePriority,
    build_default_adapter_registry,
    build_default_source_registry,
)
from datahub.enrichment import SourcePriorityEnricher  # noqa: E402
from datahub.publishers import (  # noqa: E402
    LegacyAssociationPublisher,
    LegacyRedisPublisher,
    PhenotypeRollupPublisher,
)
from datahub.storage import DuckDBParquetStorage  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DataHub ingestion from JSON config")
    parser.add_argument("--config", required=True, help="Path to ingestion JSON config")
    return parser.parse_args()


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def build_publishers(config: dict[str, Any]) -> list[Any]:
    publishers: list[Any] = []
    for item in config.get("publishers", []):
        name = str(item["name"]).strip().lower()
        params = dict(item.get("params", {}))

        if name == "legacy_association":
            publishers.append(LegacyAssociationPublisher(**params))
        elif name == "legacy_redis":
            publishers.append(LegacyRedisPublisher(**params))
        elif name == "phenotype_rollup":
            publishers.append(PhenotypeRollupPublisher(**params))
        else:
            raise ValueError(f"Unknown publisher: {name}")

    return publishers


def build_adapters(config: dict[str, Any]) -> list[Any]:
    """Build adapters from legacy ``adapters`` config and new ``sources`` config."""

    adapter_registry = build_default_adapter_registry()
    for plugin_raw in config.get("plugins", []):
        adapter_registry.register_plugin(
            AdapterPluginSpec(
                name=plugin_raw["name"],
                module=plugin_raw["module"],
                class_name=plugin_raw["class_name"],
            )
        )

    adapters = []
    for adapter_raw in config.get("adapters", []):
        adapter_name = adapter_raw["name"]
        adapter_params = dict(adapter_raw.get("params", {}))
        adapters.append(adapter_registry.create(adapter_name, **adapter_params))

    source_loader = SourceManifestLoader(config.get("sources_dir"))
    source_registry = build_default_source_registry(source_loader)
    for plugin_raw in config.get("source_plugins", []):
        source_registry.register_plugin(
            SourceConnectorPluginSpec(
                module=plugin_raw["module"],
                class_name=plugin_raw["class_name"],
                source_id=plugin_raw["source_id"],
            ),
            manifest_loader=source_loader,
        )

    for source_raw in config.get("sources", []):
        source_id = source_raw["id"]
        source_params = dict(source_raw.get("params", {}))
        adapters.append(
            source_registry.create_adapter(
                source_id,
                adapter_registry=adapter_registry,
                params=source_params,
            )
        )

    return adapters


def build_storage(config: dict[str, Any]) -> Any:
    storage_config = config.get("storage")
    if not storage_config:
        return None

    storage_type = str(storage_config.get("type", "")).strip().lower()
    params = dict(storage_config.get("params", {}))

    if storage_type == "duckdb_parquet":
        return DuckDBParquetStorage(**params)

    raise ValueError(f"Unknown storage type: {storage_type}")


def build_enricher(config: dict[str, Any]) -> Any:
    enrichment = config.get("enrichment")
    if not enrichment:
        return None

    priority = enrichment.get("source_priority", {})
    if not priority:
        return None

    return SourcePriorityEnricher(SourcePriority(priorities=priority))


def main() -> int:
    args = parse_args()
    config = load_json(args.config)

    profile_loader = DatasetProfileLoader(
        profiles_dir=config.get("profiles_dir")
    )
    if "profile_path" in config:
        profile = profile_loader.load(config["profile_path"])
    else:
        profile = profile_loader.load(config.get("profile", "association"))

    adapters = build_adapters(config)

    if not adapters:
        raise ValueError("No adapters configured. Set adapters[] and/or sources[].")

    report = DataHubPipeline(
        contract=profile.to_contract(),
        adapters=adapters,
        enricher=build_enricher(config),
        storage=build_storage(config),
        publishers=build_publishers(config),
    ).run()

    payload = {
        "profile": profile.name,
        "dataset_type": profile.dataset_type,
        "adapter_count": report.adapter_count,
        "ingested_records": report.ingested_records,
        "validated_records": report.validated_records,
        "dropped_records": report.dropped_records,
        "issues": len(report.issues),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
