import os
import time
import subprocess
from pathlib import Path

import pytest

# -----------------------------------------------------------------------------#
#  Constants                                                                   #
# -----------------------------------------------------------------------------#
EXEC_REL   = Path("tools/hbp-validate")           # validator location (relative)
DATA_REL   = Path("public/example_fh_vcf")        # default dataset
TIMEOUT    = 180                                  # seconds
ROOT_ENV   = "HBP_ROOT"                           # override repo root
DATA_ENV   = "HBP_DATASET"                        # override dataset
# -----------------------------------------------------------------------------#


# -----------------------------------------------------------------------------#
#  Fixtures                                                                    #
# -----------------------------------------------------------------------------#
@pytest.fixture(scope="session")
def repo_root() -> Path:
    """
    Detect repository root by walking up the tree until `tools/hbp-validate`
    is found.  Falls back to $HBP_ROOT or CWD.
    """
    env_root = os.getenv(ROOT_ENV)
    if env_root:
        return Path(env_root).expanduser().resolve()

    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / EXEC_REL).exists():
            return p
    return Path.cwd()


@pytest.fixture(scope="session")
def validator(repo_root: Path) -> Path:
    """Return path to validator; skip test if not present or not executable."""
    tool = repo_root / EXEC_REL
    if not tool.exists() or not os.access(tool, os.X_OK):
        pytest.skip(f"validator missing or not executable: {tool}")
    return tool


@pytest.fixture(scope="session")
def dataset(repo_root: Path) -> Path:
    """Return dataset path, allowing override via $HBP_DATASET."""
    ds = Path(os.getenv(DATA_ENV, repo_root / DATA_REL))
    if not ds.exists():
        pytest.skip(f"dataset not found: {ds}")
    return ds


# -----------------------------------------------------------------------------#
#  Helpers                                                                     #
# -----------------------------------------------------------------------------#
def _run(cmd: list[str], cwd: Path, timeout: int = TIMEOUT):
    """Run *cmd* and return (CompletedProcess, elapsed_seconds)."""
    t0 = time.monotonic()
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    return proc, time.monotonic() - t0


def _format_failure(cmd: list[str], cwd: Path, proc: subprocess.CompletedProcess, elapsed: float) -> str:
    """Build a detailed assertion message."""
    return (
        f"Command : {' '.join(cmd)}\n"
        f"CWD     : {cwd}\n"
        f"Elapsed : {elapsed:.2f}s\n"
        f"Exit    : {proc.returncode}\n"
        f"--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}\n"
    )


# -----------------------------------------------------------------------------#
#  Test                                                                        #
# -----------------------------------------------------------------------------#
@pytest.mark.integration
def test_provenance_valid(repo_root: Path, validator: Path, dataset: Path):
    """
    Smoke-test that the example dataset validates cleanly.

    *   Paths resolved from repo root (works from any `pytest -k` invocation).
    *   Skips gracefully when validator or dataset are absent.
    *   Timeout prevents hung CI jobs.
    *   Detailed assertion output on failure.
    *   `$HBP_ROOT` and `$HBP_DATASET` environment variables allow overrides.
    """
    cmd = [str(validator), str(dataset)]
    proc, elapsed = _run(cmd, repo_root)
    assert proc.returncode == 0, _format_failure(cmd, repo_root, proc, elapsed)
