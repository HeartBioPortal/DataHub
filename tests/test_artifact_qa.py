import json
from pathlib import Path

from datahub.artifact_qa import build_artifact_qa_report, write_artifact_qa_report


def test_artifact_qa_report_summarizes_sources_and_payloads(tmp_path: Path) -> None:
    published_root = tmp_path / "published"
    payload_dir = published_root / "association" / "final" / "association" / "CVD"
    payload_dir.mkdir(parents=True)
    (payload_dir / "ANK2.json").write_text(json.dumps([{"disease": ["arrhythmia"]}]))

    report = build_artifact_qa_report(published_root=published_root)

    assert report["source_catalog"]["integrated_count"] >= 4
    assert report["published_outputs"]["available"] is True
    assert report["published_outputs"]["payload_count"] == 1
    assert report["published_outputs"]["groups"]["association/CVD"]["payload_count"] == 1

    output_path = tmp_path / "qa" / "report.json"
    write_artifact_qa_report(report, output_path)
    loaded = json.loads(output_path.read_text())
    assert loaded["published_outputs"]["payload_count"] == 1
