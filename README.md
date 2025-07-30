# HeartBioPortal DataHub

HeartBioPortal DataHub is a version-controlled collection of cardiovascular omics datasets. Each dataset includes standardised metadata and provenance information so that analyses can be reproduced and referenced.

## Quick Start

```bash
git clone <repo-url>
cd DataHub
pip install -r requirements.txt
make validate
# or using docker
docker compose up validation
```

## Git Large File Storage

This repository uses [Git LFS](https://git-lfs.github.com/) for storing large binary datasets. Install Git LFS before cloning:

```bash
git lfs install
```

## Dataset Layout

Datasets are organised under `public/` for open data or `private/` for embargoed submissions. A typical dataset directory contains:

```
<dataset>/
  metadata.json      # descriptive metadata
  provenance.json    # processing provenance
  data files...
```

The JSON schemas that describe these files live under `schemas/` and are also rendered in the documentation.

You can list available datasets using:

```bash
tools/list_datasets.py
```

## Validation

Use the helper script to check a dataset before opening a pull request:

```bash
tools/hbp-validate public/example_fh_vcf
```

or run all tests with `make validate`.

## Contributing

We welcome new datasets and improvements. See [CONTRIBUTING.md](CONTRIBUTING.md) for a walkthrough of the submission process and consult the files in the `docs/` directory for more details.

## Processing Very Large Datasets

See `tools/large_dataset_processor.py` for an example using Dask to analyse VCF files over 500 GB. Run `pip install -r requirements.txt` and execute:

```bash
python tools/large_dataset_processor.py <your.vcf>
```
