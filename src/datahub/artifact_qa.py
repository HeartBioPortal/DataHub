"""Artifact QA summary helpers for DataHub release outputs."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from datahub.sources import SourceManifestLoader


def _sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_final_root(root: str | Path | None) -> Path | None:
    if root is None or not str(root).strip():
        return None
    candidate = Path(root)
    if (candidate / "association" / "final").is_dir():
        return candidate / "association" / "final"
    if candidate.is_dir():
        return candidate
    return None


def summarize_source_catalog(
    *,
    manifests_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Summarize integrated vs catalog-only source manifests."""

    loader = SourceManifestLoader(manifests_dir=manifests_dir)
    manifests = loader.load_all()
    integrated = sorted(
        source_id
        for source_id, manifest in manifests.items()
        if manifest.integration_status == "integrated"
    )
    catalog_only = sorted(
        source_id
        for source_id, manifest in manifests.items()
        if manifest.integration_status == "catalog_only"
    )
    categories: dict[str, int] = {}
    for manifest in manifests.values():
        category = manifest.data_category or "uncategorized"
        categories[category] = categories.get(category, 0) + 1
    return {
        "total": len(manifests),
        "integrated_count": len(integrated),
        "catalog_only_count": len(catalog_only),
        "integrated_sources": integrated,
        "catalog_only_sources": catalog_only,
        "categories": dict(sorted(categories.items(), key=lambda item: item[0])),
    }


def summarize_published_outputs(root: str | Path | None) -> dict[str, Any]:
    """Summarize published association/overall JSON payloads."""

    final_root = _resolve_final_root(root)
    if final_root is None:
        return {"available": False, "payload_count": 0, "groups": {}}

    groups: dict[str, dict[str, Any]] = {}
    payload_count = 0
    total_bytes = 0
    for payload_path in sorted(final_root.glob("*/*/*.json*")):
        if not payload_path.is_file():
            continue
        if not (payload_path.name.endswith(".json") or payload_path.name.endswith(".json.gz")):
            continue
        relative = payload_path.relative_to(final_root)
        if len(relative.parts) < 3:
            continue
        group_key = f"{relative.parts[0]}/{relative.parts[1]}"
        group = groups.setdefault(
            group_key,
            {"payload_count": 0, "total_bytes": 0, "sample_checksums": {}},
        )
        size = payload_path.stat().st_size
        payload_count += 1
        total_bytes += size
        group["payload_count"] += 1
        group["total_bytes"] += size
        if len(group["sample_checksums"]) < 5:
            group["sample_checksums"][relative.as_posix()] = _sha256(payload_path)

    return {
        "available": True,
        "final_root": str(final_root),
        "payload_count": payload_count,
        "total_bytes": total_bytes,
        "groups": dict(sorted(groups.items(), key=lambda item: item[0])),
    }


def summarize_duckdb_tables(db_path: str | Path | None) -> dict[str, Any]:
    """Summarize row counts for tables in a DuckDB database."""

    if db_path is None or not str(db_path).strip():
        return {"available": False, "tables": {}}
    path = Path(db_path)
    if not path.exists():
        return {"available": False, "path": str(path), "tables": {}}

    try:
        import duckdb
    except ImportError:
        return {
            "available": False,
            "path": str(path),
            "tables": {},
            "error": "duckdb is not installed",
        }

    tables: dict[str, int | None] = {}
    connection = duckdb.connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()
        for (table_name,) in rows:
            try:
                count = connection.execute(
                    f'SELECT COUNT(*) FROM "{str(table_name).replace(chr(34), chr(34) + chr(34))}"'
                ).fetchone()[0]
            except Exception:
                count = None
            tables[str(table_name)] = count
    finally:
        connection.close()

    return {
        "available": True,
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "tables": tables,
    }


def build_artifact_qa_report(
    *,
    published_root: str | Path | None = None,
    serving_db_path: str | Path | None = None,
    working_db_path: str | Path | None = None,
    manifests_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Build a release QA report for DataHub artifacts."""

    return {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "source_catalog": summarize_source_catalog(manifests_dir=manifests_dir),
        "published_outputs": summarize_published_outputs(published_root),
        "working_duckdb": summarize_duckdb_tables(working_db_path),
        "serving_duckdb": summarize_duckdb_tables(serving_db_path),
    }


def write_artifact_qa_report(report: dict[str, Any], output_path: str | Path) -> None:
    """Write a QA report as stable JSON."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True))
