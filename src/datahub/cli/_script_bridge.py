"""Bridge package console scripts to repository script entrypoints.

The operational scripts remain the source of truth while DataHub transitions
toward thinner CLI modules. These wrappers make editable installs and CI runs
less dependent on the caller's shell PATH.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def _run_script(relative_path: str) -> int:
    script_path = REPO_ROOT / relative_path
    original_argv = list(sys.argv)
    sys.argv = [str(script_path), *original_argv[1:]]
    try:
        runpy.run_path(str(script_path), run_name="__main__")
    finally:
        sys.argv = original_argv
    return 0


def build_legacy_association() -> int:
    return _run_script("scripts/build_legacy_association.py")


def build_serving_duckdb() -> int:
    return _run_script("scripts/dataset_specific_scripts/unified/build_association_serving_duckdb.py")


def ingest_legacy_raw_duckdb() -> int:
    return _run_script("scripts/dataset_specific_scripts/unified/ingest_legacy_raw_duckdb.py")


def ingest_mvp_duckdb_fast() -> int:
    return _run_script("scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py")


def manage_working_duckdb() -> int:
    return _run_script("scripts/dataset_specific_scripts/unified/manage_working_duckdb.py")


def prepare_association_raw() -> int:
    return _run_script("scripts/prepare_association_raw.py")


def publish_unified_from_duckdb() -> int:
    return _run_script("scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py")


def report_artifact_qa() -> int:
    return _run_script("scripts/report_artifact_qa.py")


def run_ingestion() -> int:
    return _run_script("scripts/run_ingestion.py")


def run_secondary_analyses() -> int:
    return _run_script("scripts/dataset_specific_scripts/unified/run_secondary_analyses.py")


def run_structural_variant_ingestion() -> int:
    return _run_script("scripts/run_structural_variant_ingestion.py")


def run_unified_pipeline() -> int:
    return _run_script("scripts/dataset_specific_scripts/unified/run_unified_pipeline.py")
