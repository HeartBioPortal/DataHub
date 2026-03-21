"""Helpers for reading repository artifacts, including compressed single-file archives."""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, TextIO


def _iter_zip_members(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    return [member for member in archive.infolist() if not member.is_dir()]


def _resolve_zip_member(
    *,
    archive_path: Path,
    archive: zipfile.ZipFile,
    member_name: str | None,
) -> zipfile.ZipInfo:
    members = _iter_zip_members(archive)
    if not members:
        raise FileNotFoundError(f"Zip archive contains no files: {archive_path}")

    if member_name:
        for member in members:
            if member.filename == member_name:
                return member
        raise FileNotFoundError(f"Zip member not found in {archive_path}: {member_name}")

    if len(members) != 1:
        member_names = ", ".join(member.filename for member in members[:5])
        raise ValueError(
            "Zip archive must contain exactly one file unless member_name is provided: "
            f"{archive_path} ({member_names})"
        )
    return members[0]


@contextmanager
def open_text_artifact(
    path: str | Path,
    *,
    encoding: str = "utf-8",
    newline: str | None = None,
    zip_member_name: str | None = None,
) -> Iterator[TextIO]:
    """Open a plain-text, gzip, or single-file zip artifact for reading."""

    artifact_path = Path(path)
    suffixes = tuple(suffix.lower() for suffix in artifact_path.suffixes)

    if suffixes[-1:] == (".gz",):
        with gzip.open(artifact_path, "rt", encoding=encoding, newline=newline) as handle:
            yield handle
        return

    if suffixes[-1:] == (".zip",):
        with zipfile.ZipFile(artifact_path) as archive:
            member = _resolve_zip_member(
                archive_path=artifact_path,
                archive=archive,
                member_name=zip_member_name,
            )
            with archive.open(member, "r") as raw_handle:
                with io.TextIOWrapper(raw_handle, encoding=encoding, newline=newline) as handle:
                    yield handle
        return

    with artifact_path.open("rt", encoding=encoding, newline=newline) as handle:
        yield handle


def load_json_artifact(path: str | Path) -> Any:
    """Load JSON from a plain, gzip, or zip-compressed artifact."""

    with open_text_artifact(path) as handle:
        return json.load(handle)
