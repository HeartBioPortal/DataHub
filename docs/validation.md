# Validation Guide

Validating datasets before submission keeps the DataHub consistent and prevents broken metadata from entering production. This guide explains the available validation tooling, common error messages, and best practices for resolving issues.

## When to run validation

- **During dataset preparation** – run validation locally as you populate `metadata.json` and `provenance.json` to catch schema violations early.
- **Before opening a pull request** – include the validation output in your PR description so reviewers can confirm the dataset passes automated checks.
- **After rebasing or merging** – rerun validation if your branch picks up changes to the schemas or validation tests.

## Validation tooling

### `tools/hbp-validate`

This is the quickest way to check a single dataset directory against the repository schemas and integrity rules.

```bash
tools/hbp-validate public/example_fh_vcf
```

Key features:

- Loads the dataset metadata and provenance files and validates them against the JSON Schemas under `schemas/`.
- Ensures required files are present and checksums (where provided) match the accompanying data.
- Provides clear error messages with JSON pointer paths to help locate problematic fields.

> **Tip:** Use the `--fix` flag (when available) to auto-format JSON files with consistent ordering.

### `make validate`

Running `make validate` executes the full `pytest` suite defined in the repository. This includes schema validation plus any regression or integration tests implemented under `tests/`.

```bash
make validate
```

Use this command before submitting a pull request or when working on validation tooling to ensure nothing regresses.

### Docker-based validation

For teams that prefer containerised tooling, the repository includes a Docker Compose service that mirrors the local workflow:

```bash
docker compose up validation
```

This is helpful when contributors do not want to manage Python dependencies on their host machine. The validation logs will stream to the terminal; exit the service with `Ctrl+C` when complete.

## Troubleshooting common issues

| Error message | Meaning | Resolution |
| --- | --- | --- |
| `is not valid under any of the given schemas` | The JSON file does not match the schema definition. | Inspect the pointer provided in the error message and cross-reference with `schemas/metadata.schema.json` or `schemas/prov.schema.json`. |
| `file not found` | A file referenced in your metadata is missing from the dataset folder. | Ensure all declared artefacts are committed with Git LFS (for large files) and paths are correct. |
| `checksum mismatch` | The provided checksum does not match the file contents. | Recalculate the checksum (e.g., `shasum -a 256 <file>`) and update the metadata. |
| `Additional properties are not allowed` | Extra fields are present in the JSON document. | Remove the unrecognised properties or propose schema updates in a separate pull request. |

If a failure message is unclear, run `pytest -k <dataset-name> -vv` to see the full traceback and context.

## Best practices

- **Commit JSON files in UTF-8 without trailing whitespace** to avoid formatting churn.
- **Store large binary artefacts with Git LFS** and confirm the `.gitattributes` entry is correct.
- **Document preprocessing steps in `provenance.json`**, including tool versions and command-line arguments.
- **Automate validation in CI** by reusing the `make validate` target in your preferred pipeline.

## Need help?

Open a GitHub issue with the validation command you ran, the error output, and a link to your branch. Maintainers are happy to help diagnose stubborn issues or extend the validation tooling.
