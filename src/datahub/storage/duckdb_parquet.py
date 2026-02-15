"""DuckDB + Parquet storage backend for canonical records."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from datahub.models import CanonicalRecord
from datahub.storage.base import CanonicalStorage

try:
    import duckdb
except ImportError:  # pragma: no cover - exercised only when dependency missing
    duckdb = None


_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DuckDBParquetStorage(CanonicalStorage):
    """Persist canonical records in a queryable DB and portable Parquet file."""

    def __init__(
        self,
        *,
        db_path: str | Path,
        parquet_path: str | Path,
        table_name: str = "canonical_records",
    ) -> None:
        if not _TABLE_RE.match(table_name):
            raise ValueError(f"Unsafe table name: {table_name}")

        self.db_path = Path(db_path)
        self.parquet_path = Path(parquet_path)
        self.table_name = table_name

    def persist(self, records: list[CanonicalRecord]) -> None:
        if not records:
            return

        if duckdb is None:
            raise RuntimeError(
                "duckdb is not installed. Add it to requirements before running DataHub storage."
            )

        frame = pd.DataFrame([record.to_row() for record in records])
        for column in ("ancestry", "metadata"):
            frame[column] = frame[column].map(lambda payload: json.dumps(payload or {}, sort_keys=True))

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_path.parent.mkdir(parents=True, exist_ok=True)

        connection = duckdb.connect(str(self.db_path))
        try:
            connection.register("canonical_frame", frame)
            connection.execute(
                f"CREATE OR REPLACE TABLE {self.table_name} AS SELECT * FROM canonical_frame"
            )

            if self.parquet_path.exists():
                self.parquet_path.unlink()

            parquet_target = self.parquet_path.as_posix().replace("'", "''")
            connection.execute(
                f"COPY {self.table_name} TO '{parquet_target}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()
