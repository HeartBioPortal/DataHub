import json
from pathlib import Path

from datahub.config_schemas import (
    format_config_validation_issues,
    validate_config_file,
    validate_default_config_tree,
)


def test_default_config_tree_validates_against_json_schemas() -> None:
    issues = validate_default_config_tree()
    assert issues == [], format_config_validation_issues(issues)


def test_source_manifest_schema_reports_missing_required_field(tmp_path: Path) -> None:
    manifest_path = tmp_path / "bad_source.json"
    manifest_path.write_text(
        json.dumps(
            {
                "source_id": "bad_source",
                "display_name": "Bad source",
                "description": "",
                "adapter_name": "external_source_adapter",
                "dataset_types": ["ASSOCIATION"],
                "modalities": ["association"],
                "access_mode": "download",
                "data_category": "gwas_statistical_genetics",
            }
        )
    )

    issues = validate_config_file(
        manifest_path,
        schema_name="source_manifest.schema.json",
        family="source_manifests",
    )

    assert any("integration_status" in issue.message for issue in issues)
