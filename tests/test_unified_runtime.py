from pathlib import Path

from datahub.unified.runtime import resolve_duckdb_temp_directory


def test_duckdb_temp_directory_defaults_next_to_anchor(tmp_path: Path) -> None:
    db_path = tmp_path / "work" / "datahub.duckdb"

    resolved = resolve_duckdb_temp_directory(None, anchor_path=db_path)

    assert resolved == db_path.parent.resolve() / "_duckdb_tmp"


def test_duckdb_temp_directory_can_keep_duckdb_default(tmp_path: Path) -> None:
    db_path = tmp_path / "datahub.duckdb"

    assert resolve_duckdb_temp_directory("none", anchor_path=db_path) is None
