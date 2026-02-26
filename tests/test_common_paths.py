import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.adapters.common import expand_input_paths


def test_expand_input_paths_supports_environment_variables(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "inputs"
    data_dir.mkdir(parents=True)
    csv_file = data_dir / "sample.csv"
    csv_file.write_text("x,y\n1,2\n")

    monkeypatch.setenv("DATAHUB_PATH_TEST", str(data_dir))

    paths = expand_input_paths("$DATAHUB_PATH_TEST/*.csv")
    assert paths == [csv_file]
