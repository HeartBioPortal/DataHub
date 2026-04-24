import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datahub.adapters.common import expand_input_paths
from datahub.artifact_io import load_json_artifact
from datahub.checkpoints import write_json_atomic


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


def test_write_json_atomic_supports_zip_artifacts(tmp_path: Path) -> None:
    zip_path = tmp_path / "structural_variants.json.zip"

    write_json_atomic(zip_path, {"ANK2": {"variants": []}}, indent=2, sort_keys=True)

    assert load_json_artifact(zip_path) == {"ANK2": {"variants": []}}
    with zipfile.ZipFile(zip_path) as archive:
        assert archive.namelist() == ["structural_variants.json"]
