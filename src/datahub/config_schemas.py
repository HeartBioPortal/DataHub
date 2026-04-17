"""JSON Schema validation for DataHub configuration files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_DIR = REPO_ROOT / "config" / "schemas"


@dataclass(frozen=True)
class ConfigSchemaFamily:
    """A group of config files validated by one schema."""

    name: str
    schema_name: str
    glob_pattern: str


DEFAULT_CONFIG_SCHEMA_FAMILIES: tuple[ConfigSchemaFamily, ...] = (
    ConfigSchemaFamily("source_manifests", "source_manifest.schema.json", "config/sources/*.json"),
    ConfigSchemaFamily("dataset_profiles", "dataset_profile.schema.json", "config/profiles/*.json"),
    ConfigSchemaFamily("prep_profiles", "prep_profile.schema.json", "config/prep_profiles/*.json"),
    ConfigSchemaFamily("runtime_profiles", "runtime_profiles.schema.json", "config/runtime_profiles/*.json"),
    ConfigSchemaFamily("export_manifests", "export_manifest.schema.json", "config/export_manifests/**/*.json"),
    ConfigSchemaFamily("output_contracts", "output_contract.schema.json", "config/output_contracts/*.json"),
    ConfigSchemaFamily("secondary_analyses", "secondary_analysis.schema.json", "config/secondary_analyses/*.json"),
)


@dataclass(frozen=True)
class ConfigValidationIssue:
    """One JSON Schema validation issue."""

    family: str
    path: Path
    message: str
    json_path: str


def load_json_schema(schema_name: str, *, schema_dir: str | Path | None = None) -> dict[str, Any]:
    """Load a DataHub JSON Schema by filename."""

    root = Path(schema_dir) if schema_dir else DEFAULT_SCHEMA_DIR
    return json.loads((root / schema_name).read_text())


def validate_config_file(
    path: str | Path,
    *,
    schema_name: str,
    schema_dir: str | Path | None = None,
    family: str = "config",
) -> list[ConfigValidationIssue]:
    """Validate one config file and return all issues."""

    config_path = Path(path)
    payload = json.loads(config_path.read_text())
    validator = Draft202012Validator(load_json_schema(schema_name, schema_dir=schema_dir))
    issues: list[ConfigValidationIssue] = []
    for error in sorted(validator.iter_errors(payload), key=lambda item: list(item.path)):
        json_path = "$"
        if error.path:
            json_path += "." + ".".join(str(part) for part in error.path)
        issues.append(
            ConfigValidationIssue(
                family=family,
                path=config_path,
                message=error.message,
                json_path=json_path,
            )
        )
    return issues


def validate_default_config_tree(
    *,
    repo_root: str | Path | None = None,
    schema_dir: str | Path | None = None,
    families: tuple[ConfigSchemaFamily, ...] = DEFAULT_CONFIG_SCHEMA_FAMILIES,
) -> list[ConfigValidationIssue]:
    """Validate the default DataHub config tree."""

    root = Path(repo_root) if repo_root else REPO_ROOT
    issues: list[ConfigValidationIssue] = []
    for family in families:
        for path in sorted(root.glob(family.glob_pattern)):
            issues.extend(
                validate_config_file(
                    path,
                    schema_name=family.schema_name,
                    schema_dir=schema_dir,
                    family=family.name,
                )
            )
    return issues


def format_config_validation_issues(issues: list[ConfigValidationIssue]) -> str:
    """Format validation issues for CLI errors."""

    return "\n".join(
        f"{issue.family}: {issue.path}: {issue.json_path}: {issue.message}"
        for issue in issues
    )
