import csv
import json
import subprocess
from pathlib import Path


def _write_mvp_csv(path: Path, *, rsid: str, af: float) -> None:
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "SNP_ID",
                "gene_symbol",
                "phenotype_description",
                "ancestry",
                "af",
                "ref",
                "alt",
                "pval",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "SNP_ID": rsid,
                "gene_symbol": "GENE1",
                "phenotype_description": "arrhythmia",
                "ancestry": "EUR",
                "af": str(af),
                "ref": "A",
                "alt": "G",
                "pval": "0.01",
            }
        )


def _run_mvp_script(repo_root: Path, *, args: list[str]) -> dict[str, object]:
    result = subprocess.run(
        ["python3", "scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_mvp_pipeline_resume_skips_completed_files(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    input_one = tmp_path / "mvp_1.csv"
    input_two = tmp_path / "mvp_2.csv"
    output_root = tmp_path / "out"
    checkpoint_path = tmp_path / "state" / "checkpoint.json"

    _write_mvp_csv(input_one, rsid="rs1", af=0.1)
    _write_mvp_csv(input_two, rsid="rs2", af=0.2)

    common_args = [
        "--output-root",
        str(output_root),
        "--disable-rollup",
        "--chunksize",
        "1",
        "--publish-batch-size",
        "1",
        "--progress-every-rows",
        "1",
        "--checkpoint-path",
        str(checkpoint_path),
        "--log-level",
        "WARNING",
    ]

    first = _run_mvp_script(
        repo_root,
        args=[
            "--input-path",
            str(input_one),
            *common_args,
        ],
    )
    assert first["processed_files"] == 1
    assert first["skipped_files"] == 0
    assert first["validated_records"] == 1

    second = _run_mvp_script(
        repo_root,
        args=[
            "--input-path",
            str(input_one),
            "--input-path",
            str(input_two),
            *common_args,
        ],
    )
    assert second["processed_files"] == 1
    assert second["skipped_files"] == 1
    assert second["validated_records"] == 1

    third = _run_mvp_script(
        repo_root,
        args=[
            "--input-path",
            str(input_one),
            "--input-path",
            str(input_two),
            *common_args,
        ],
    )
    assert third["processed_files"] == 0
    assert third["skipped_files"] == 2
    assert third["validated_records"] == 0

    association_path = (
        output_root / "association" / "final" / "association" / "CVD" / "GENE1.json"
    )
    association_payload = json.loads(association_path.read_text())

    rsids = set()
    for entry in association_payload:
        for ancestry_item in entry.get("ancestry", []):
            for point in ancestry_item.get("data", []):
                rsids.add(point["rsid"])
    assert rsids == {"rs1", "rs2"}

    checkpoint_payload = json.loads(checkpoint_path.read_text())
    completed_files = checkpoint_payload.get("completed_files", {})
    assert str(input_one.resolve()) in completed_files
    assert str(input_two.resolve()) in completed_files
