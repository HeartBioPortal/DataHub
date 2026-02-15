"""Canonical storage backends for DataHub."""

from .base import CanonicalStorage
from .duckdb_parquet import DuckDBParquetStorage

__all__ = ["CanonicalStorage", "DuckDBParquetStorage"]
