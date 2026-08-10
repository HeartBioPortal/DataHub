# HeartBioPortal DataHub

![Abstract overview of source data becoming canonical records and published artifacts](assets/hero/overview-datahub.png){ .overview-hero }

HeartBioPortal DataHub is the data engineering, scientific integration, and
artifact-publication layer behind HeartBioPortal. Biomedical sources describe
related biology with different identifiers, schemas, units, population labels,
quality guarantees, and access constraints. DataHub preserves those source
distinctions while producing reproducible contracts that the portal can query
and display.

## What DataHub is responsible for

DataHub owns five responsibilities:

1. **Preparation**: bring irregular raw files into stable, auditable intermediate shapes.
2. **Canonicalization**: map heterogeneous sources into one reusable record model.
3. **Publication**: convert unified records into analyzed outputs that downstream systems can serve.
4. **Serving preparation**: build query-oriented DuckDBs, indexes, and compact per-gene artifacts without changing their scientific semantics.
5. **Orchestration**: run the same logical pipeline on a laptop, a cloud VM, or an HPC cluster with observable, restartable jobs.

## What DataHub is not

DataHub is not the web application or the account/content database. The HBP
backend consumes DataHub artifacts and may cache or shape responses, but
scientific aggregation, normalization, and provenance rules belong in DataHub.
DataHub also catalogs sources that are not yet integrated; a source manifest is
not proof that its data are present in a production release.

## Reader map

Use the documentation based on what you need:

- New contributor: start with [Getting Started](getting-started.md) and [Repository Tour](repository-tour.md)
- Data engineer: read [System Overview](architecture/system-overview.md), [Configuration Surfaces](architecture/configuration.md), and [Unified DuckDB Pipeline](pipelines/unified.md)
- Scientist trying to understand the artifact model: read [Data Sources](reference/data-sources.md), [Data Model](architecture/data-model.md), [Association Pipeline](pipelines/association.md), and [Serving Artifacts](pipelines/serving.md)
- Someone extending the platform: read [New Source Onboarding](extending/new-source.md) and [Export Manifest Framework](extending/export-manifests.md)
- Someone preparing a run: use [Local Smoke Test](runbooks/local-smoke-test.md) or [Release Checklist](runbooks/release-checklist.md)

## Core design commitments

- **One canonical model, many source adapters**
- **Publish once, serve many times**
- **Use explicit config over hidden source-specific conditionals**
- **Keep provenance attached as long as possible**
- **Keep legacy compatibility without freezing the architecture**

## High-level flow

```text
raw files / source APIs
  -> preparation profiles
  -> adapters
  -> source-normalized and analysis-ready records
  -> analyzed publication (.json/.json.gz/Parquet)
  -> serving DuckDBs and indexed artifacts
  -> backend / web application
```

## Current major pipeline families

- **Legacy-compatible association build**: direct publish from prepared/legacy inputs
- **MVP dataset-specific pipeline**: canonical ingest plus legacy-compatible publish
- **Unified DuckDB-first pipeline**: merged MVP + legacy points, source-priority dedup, publish from DuckDB, optional serving artifact build
- **Secondary and companion evidence**: legacy expression packaging, curated
  expression v3, SGA, protein context, and gene-profile artifacts. Only the
  standardized per-gene secondary artifacts are attached through the generic
  secondary-analysis serving updater.
- **Population-frequency indexing**: dbSNP archive and legacy frequency rows
  normalized into Parquet handoff artifacts and a provenance-preserving DuckDB
  index. Population observations do not contain disease associations; the
  portal joins them to association-selected rsIDs at query time.
- **Structural-variant publication**: dbVar records normalized into the legacy
  gene-centered SV contract, with optional exon enrichment.

The [Data Sources](reference/data-sources.md) inventory lists sources that are
demonstrably represented in current artifacts or active pipelines. The broader
developer registry also contains `catalog_only` roadmap entries; those entries
do not imply production inclusion.

## Documentation website

This repository is configured as a MkDocs site published by GitHub Pages from
the GitHub Actions workflow in `.github/workflows/docs.yml`. For local preview
commands, see [Getting Started](getting-started.md).
