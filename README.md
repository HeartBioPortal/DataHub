# HeartBioPortal DataHub

HeartBioPortal DataHub is the canonical, version-controlled collection of cardiovascular omics datasets used across the HeartBioPortal ecosystem. Each contribution is reviewed, validated, and stored alongside machine-readable metadata so that downstream analyses can be reproduced, cited, and continuously improved.

## Key features

- **Curated data catalogue** – each dataset is stored in its own folder with descriptive metadata and provenance information.
- **Schema-driven validation** – JSON Schemas under `schemas/` ensure that metadata and provenance files follow a consistent structure.
- **Reproducible tooling** – helper scripts, automated tests, and MkDocs documentation make it easy to validate and explore datasets locally.
- **Open collaboration** – submissions are tracked in git, with Git LFS support for large binary artefacts and a transparent review process.

## Repository layout

| Path | Description |
| --- | --- |
| `public/` | Open datasets available to all users. |
| `private/` | Embargoed submissions that require restricted access. |
| `schemas/` | JSON Schemas describing dataset metadata and provenance formats. |
| `docs/` | MkDocs documentation source, including schema reference pages. |
| `tools/` | Command-line utilities for listing, validating, and processing datasets. |
| `tests/` | Automated validation tests executed via `pytest`. |

## Prerequisites

1. Install [Git LFS](https://git-lfs.github.com/) before cloning the repository:
   ```bash
   git lfs install
   ```
2. Ensure you have Python 3.9+ available for running the validation tooling.
3. Optional: install Docker if you prefer containerised validation.

## Quick start

```bash
git clone <repo-url>
cd DataHub
pip install -r requirements.txt
make validate          # run automated tests locally

# or using docker
docker compose up validation
```

Once dependencies are installed, run `tools/list_datasets.py` to explore the catalogue of available datasets.

## Working with datasets

Every dataset lives under either `public/` or `private/` and follows a consistent layout:

```
<dataset>/
  metadata.json      # descriptive metadata aligned with schemas/metadata.schema.json
  provenance.json    # processing provenance aligned with schemas/prov.schema.json
  data/ or other files...
```

Use the [`public/example_fh_vcf`](public/example_fh_vcf) folder as a template when creating a new submission. The schemas referenced above are also rendered into human-readable documentation (see [Documentation](#documentation)).

### Submission workflow

1. Fork the repository and create a feature branch for your dataset.
2. Add your dataset folder under `public/` or `private/` with the required JSON files and supporting artefacts.
3. Validate the dataset locally (see [Validation](#validation-and-quality-assurance)).
4. Open a pull request describing the dataset, methodologies, and any licensing considerations. A reviewer will inspect the metadata, provenance, and validation output before merge.

## Validation and quality assurance

Use the helper CLI to validate a dataset before submitting a pull request:

```bash
tools/hbp-validate public/example_fh_vcf
```

Running `make validate` executes the full `pytest` suite, including schema validation and regression checks. For complex workflows or very large files, consult [docs/validation.md](docs/validation.md) for detailed guidance on troubleshooting failures, working with Git LFS assets, and interpreting validation output.

## Documentation

The documentation site is built with [MkDocs](https://www.mkdocs.org/). Generate the schema reference pages and build the site locally with:

```bash
make docs
```

This command runs `python scripts/gen_schema_docs.py` to create Markdown documentation from the JSON Schemas before compiling the MkDocs site in `site/`. Published documentation includes the submission workflow, schema reference, large dataset processing tips, FAQ, and validation guides.

## Helpful tooling

- `tools/list_datasets.py` – list registered datasets and their metadata summary.
- `tools/hbp-validate` – validate a dataset directory against the schemas and integrity checks.
- `tools/large_dataset_processor.py` – example pipeline that demonstrates analysing >500 GB VCF files using Dask.

## Contributing and support

We welcome new datasets, bug fixes, and tooling improvements. Review [CONTRIBUTING.md](CONTRIBUTING.md) for the full submission checklist and coding standards. Questions can be raised via GitHub issues or by contacting the HeartBioPortal maintainers.

## License

HeartBioPortal DataHub is released under the terms of the [MIT License](LICENSE).
