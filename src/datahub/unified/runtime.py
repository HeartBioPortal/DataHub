"""DuckDB runtime helpers shared by unified operational scripts."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_TEMP_SUBDIR = "_duckdb_tmp"


@dataclass(frozen=True)
class DuckDBRuntimeSettings:
    """Resolved DuckDB runtime settings applied to a connection."""

    threads: int
    memory_limit: str | None
    preserve_insertion_order: bool
    temp_directory: Path | None
    max_temp_directory_size: str | None
    progress_bar: bool


def resolve_duckdb_temp_directory(
    value: str | Path | None,
    *,
    anchor_path: str | Path,
) -> Path | None:
    """Resolve a safe DuckDB temp directory.

    Direct CLI invocations default to a sibling of the relevant DB or output
    path. Runtime profiles can still pass an explicit scratch directory for
    production or HPC runs.
    """

    if value is None or str(value).strip() == "":
        return Path(anchor_path).expanduser().resolve().parent / DEFAULT_TEMP_SUBDIR

    normalized = str(value).strip()
    if normalized.lower() in {"none", "default", "auto"}:
        return None
    return Path(normalized).expanduser()


def configure_duckdb_runtime(
    connection: Any,
    *,
    threads: int = 0,
    memory_limit: str | None = "8GB",
    preserve_insertion_order: bool = False,
    temp_directory: str | Path | None = None,
    temp_anchor_path: str | Path,
    max_temp_directory_size: str | None = None,
    progress_bar: bool = False,
    logger: logging.Logger | None = None,
) -> DuckDBRuntimeSettings:
    """Apply common DuckDB runtime settings and return their resolved values."""

    if threads > 0:
        connection.execute(f"PRAGMA threads={int(threads)}")

    resolved_memory_limit: str | None = None
    if memory_limit:
        mem = str(memory_limit).strip().replace("'", "")
        if mem:
            resolved_memory_limit = mem
            connection.execute(f"SET memory_limit='{mem}'")

    if not preserve_insertion_order:
        connection.execute("SET preserve_insertion_order=false")

    resolved_temp = resolve_duckdb_temp_directory(
        temp_directory,
        anchor_path=temp_anchor_path,
    )
    if resolved_temp is not None:
        resolved_temp.mkdir(parents=True, exist_ok=True)
        safe_temp = str(resolved_temp).replace("'", "")
        connection.execute(f"SET temp_directory='{safe_temp}'")

    resolved_max_temp: str | None = None
    if max_temp_directory_size:
        size_value = str(max_temp_directory_size).strip().replace("'", "")
        if size_value:
            resolved_max_temp = size_value
            connection.execute(f"PRAGMA max_temp_directory_size='{size_value}'")

    if progress_bar:
        try:
            connection.execute("SET enable_progress_bar = true")
            try:
                connection.execute("SET enable_progress_bar_print = true")
            except Exception:
                # Older DuckDB versions may not expose this setting.
                pass
            if logger is not None:
                logger.info("DuckDB internal progress bar enabled.")
        except Exception as exc:
            if logger is not None:
                logger.warning("Could not enable DuckDB progress bar: %s", exc)

    return DuckDBRuntimeSettings(
        threads=int(threads),
        memory_limit=resolved_memory_limit,
        preserve_insertion_order=bool(preserve_insertion_order),
        temp_directory=resolved_temp,
        max_temp_directory_size=resolved_max_temp,
        progress_bar=bool(progress_bar),
    )
