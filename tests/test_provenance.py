import os
import time
import subprocess
from pathlib import Path

import pytest


def _find_repo_root(start: Path) -> Path:
    """
    Walk up from `start` to find a directory that contains tools/hbp-validate.
    Fallback to current working directory.
    """
    for p in [start, *start.parents]:
        if (p / "tools" / "hbp-validate").exists():
            return p
    return Path.cwd()


@pytest.mark.integration
def test_provenance_valid():
    """
    Smoke test: the example dataset should validate cleanly.

    Improvements over the original:
    - Resolves paths relative to repo root (works regardless of pytest -k path).
    - Skips cleanly if the validator or dataset are missing.
    - Adds a timeout to avoid hanging CI.
    - Provides a detailed, consolidated failure message (cmd, cwd, stdout, stderr).
    - Allows overriding dataset via HBP_DATASET env var.
    """
    start = Path(__file__).resolve()
    root = _find_repo_root(start)

    tool = root / "tools" / "hbp-validate"
    if not tool.exists():
        pytest.skip(f"Validator not found at {tool}")

    ds_env = os.getenv("HBP_DATASET")
    ds = Path(ds_env) if ds_env else (root / "public" / "example_fh_vcf")
    if not ds.exists():
        pytest.skip(f"Dataset not found at {ds}")

    cmd = [str(tool), str(ds)]
    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            cwd=root,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f"Timed out after 180s\nCommand: {' '.join(cmd)}\nCWD: {root}\n"
            f"Partial stdout:\n{exc.stdout}\nPartial stderr:\n{exc.stderr}"
        )

    elapsed = time.monotonic() - t0
    failure_msg = (
        f"Command: {' '.join(cmd)}\n"
        f"CWD: {root}\n"
        f"Exit code: {result.returncode}\n"
        f"Elapsed: {elapsed:.2f}s\n"
        f"--- stdout ---\n{result.stdout}\n"
        f"--- stderr ---\n{result.stderr}\n"
    )

    assert result.returncode == 0, failure_msg

