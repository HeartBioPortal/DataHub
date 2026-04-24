"""Checkpoint helpers for resumable DataHub workflows."""

from __future__ import annotations

import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def write_json_atomic(path: str | Path, payload: Any, *, indent: int | None = 2, sort_keys: bool = True) -> None:
    """Atomically write JSON payloads to disk.

    Paths ending in ``.zip`` are written as single-member zip archives so the
    artifact remains compatible with :func:`datahub.artifact_io.load_json_artifact`.
    """

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    content = json.dumps(payload, indent=indent, sort_keys=sort_keys)
    if target.suffix.lower() == ".zip":
        member_name = target.name[:-4] if target.name.lower().endswith(".zip") else target.with_suffix("").name
        with zipfile.ZipFile(temp, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(member_name, content)
    else:
        temp.write_text(content)
    temp.replace(target)


class StructuralVariantCheckpoint:
    """Row-level checkpoint state for resumable SV publication."""

    VERSION = 1

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.payload = self._default_payload()

    @classmethod
    def _default_payload(cls) -> dict[str, Any]:
        return {
            "version": cls.VERSION,
            "completed_rows": {},
            "output_path": None,
            "updated_at": None,
        }

    def load(self) -> None:
        if not self.path.exists():
            self.payload = self._default_payload()
            return

        loaded = json.loads(self.path.read_text())
        if not isinstance(loaded, dict):
            self.payload = self._default_payload()
            return
        if int(loaded.get("version", 0)) != self.VERSION:
            self.payload = self._default_payload()
            return
        if not isinstance(loaded.get("completed_rows"), dict):
            loaded["completed_rows"] = {}
        self.payload = loaded

    def save(self) -> None:
        self.payload["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
        write_json_atomic(self.path, self.payload, indent=2, sort_keys=True)

    def reset(self) -> None:
        self.payload = self._default_payload()
        if self.path.exists():
            self.path.unlink()

    def completed_rows(self) -> dict[str, int]:
        completed = self.payload.get("completed_rows", {})
        if not isinstance(completed, dict):
            return {}
        normalized: dict[str, int] = {}
        for file_key, value in completed.items():
            if not isinstance(value, dict):
                continue
            try:
                normalized[str(file_key)] = int(value.get("row_index", 0))
            except (TypeError, ValueError):
                continue
        return normalized

    def output_path(self) -> str | None:
        value = self.payload.get("output_path")
        text = str(value).strip() if value is not None else ""
        return text or None

    def mark_completed_row(
        self,
        *,
        source_file: str,
        row_index: int,
        output_path: str | Path | None = None,
    ) -> None:
        completed = self.payload.setdefault("completed_rows", {})
        completed[str(source_file)] = {
            "row_index": int(row_index),
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        if output_path is not None:
            self.payload["output_path"] = str(output_path)
        self.save()
