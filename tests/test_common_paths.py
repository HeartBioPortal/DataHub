import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.adapters.common import expand_input_paths


def test_expand_input_paths_supports_environment_variables(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "inputs"
    data_dir.mkdir(parents=True)
    csv_file = data_dir / "sample.csv"
    csv_file.write_text("x,y\n1,2\n")
    zip_path = data_dir / "sample2.csv.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sample2.csv", "x,y\n3,4\n")

    monkeypatch.setenv("DATAHUB_PATH_TEST", str(data_dir))

    csv_paths = expand_input_paths("$DATAHUB_PATH_TEST/*.csv")
    assert csv_paths == [csv_file]

    all_paths = expand_input_paths(data_dir)
    assert all_paths == [csv_file, zip_path]
