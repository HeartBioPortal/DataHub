# HeartBioPortal DataHub

A version-controlled repository of validated cardiovascular omics datasets, harmonised to HeartBioPortal formats and FAIR4RS standards.

## Quick Start

```bash
git clone <repo-url>
cd DataHub
make validate
# or using docker
docker compose up validation
```

## Git Large File Storage

This repository uses [Git LFS](https://git-lfs.github.com/) for storing large
binary datasets. Install Git LFS before cloning:

```bash
git lfs install
```
