# Gene Dossier Summary Store

The gene dossier summary store is a precomputed SQLite artifact that serves the
unfiltered `POST /api/search_summary/` payload for a fixed set of genes. The
backend builds it from the serving data it already reads. The store is a
performance layer, not a source of truth: every payload in it is exactly what
the backend would compute for that gene with no filters.

| | |
|---|---|
| Artifact | `/data/DataHub/datamart/gene_dossier_summary.sqlite3` |
| Manifest | `/data/DataHub/datamart/gene_dossier_summary.sqlite3.manifest.json` |
| Backend setting | `HBP_GENE_DOSSIER_SUMMARY_PATH` |
| Builder | backend `manage.py build_gene_dossier_summary` (`analyzer/management/commands/build_gene_dossier_summary.py`) |
| Reader | backend `analyzer/dossier_summary_store.py` (`GeneDossierSummaryStore`) |
| Schema | `hbp_gene_dossier_summary_v1`; manifest `hbp_gene_dossier_summary_manifest_v1` |

The artifact is not committed to git. We record each build here and keep its
manifest beside the artifact.

## How the backend uses it

For an unfiltered summary request, the backend reads the gene's row first. It
then falls back to the bounded disk cache (`HBP_SEARCH_SUMMARY_CACHE_DIR`), and
finally builds the payload. The application log records which path served each
request: `search_summary query=<GENE> source=precomputed|...`.

A row is used only when the store's `source_fingerprint` equals the backend's
current fingerprint. The fingerprint is a digest of:

- `cache_version`: the backend `SEARCH_SUMMARY_CORE_CACHE_VERSION`;
- `association_provider`: path, mtime and size of the association serving DuckDB;
- `association_evidence_v2`: path, mtime and size of the evidence v2 sidecar;
- `association_evidence_v2_serving_manifest`: path, mtime and size of the
  serving manifest.

When any of these changes (a backend release that bumps the cache version, or a
new serving artifact), the store stops matching and requests fall back to the
disk cache or a fresh build. Nothing fails, but those genes lose the fast path
until the store is rebuilt with the new code and data.

The reader opens a new read-only connection per request, so an atomic file
replace takes effect without a backend restart.

## Contents

One row per gene in `gene_summaries`: `gene`, `payload_zlib` (zlib-compressed
JSON payload), `payload_sha256`, `uncompressed_bytes`, `built_at`. The
`metadata` table carries `schema_version`, `source_fingerprint`,
`source_metadata`, `status`, `gene_count` and timing fields.

The gene set is the 56 genes of the original 2026-08-31 build. When we rebuild,
we read the list back from the live store's `gene_summaries` table.

## Rebuild

Run the rebuild after the backend release that changes the fingerprint is live.
It must run from the deployed production checkout with the production
environment, so that the fingerprint it writes matches the running service.
It reads serving data only and does not run DataHub pipelines.

1. Report free space on `/` and `/data`.
2. Write the gene list from the live store:

    ```bash
    python3 -c "import sqlite3; c = sqlite3.connect('file:/data/DataHub/datamart/gene_dossier_summary.sqlite3?mode=ro', uri=True); print('\n'.join(r[0] for r in c.execute('select gene from gene_summaries order by gene')))" > genes.txt
    ```

3. Build to the `.next` path with the production environment loaded (the env
   file is root-readable and is never printed):

    ```bash
    manage.py build_gene_dossier_summary \
      --output /data/DataHub/datamart/gene_dossier_summary.next.sqlite3 \
      --genes-file genes.txt --reset --progress-interval 5 --commit-interval 5
    ```

    The command writes `*.building`, then renames it to the output and writes
    `<output>.manifest.json`. A failed gene leaves the working file and a
    `*.failures.json`; `--resume` continues it.

4. Check the new file: `pragma integrity_check` is `ok`, 56 rows, and the
   manifest's `output_sha256` matches the file.
5. Copy the live file and manifest to a backup directory, `os.replace` the
   `.next` file onto the live path, and write the manifest beside it with
   `build_output_path`, `output_path` and `deployed_at`.
6. Request a few store genes and check that the log shows `source=precomputed`.

Rollback is the reverse copy from the backup. A store whose fingerprint no
longer matches the backend is ignored, so rolling the backend back does not
require restoring the store.

## Build record

| Built | Backend | Cache version | Genes | Size | SHA-256 | Wall time |
|---|---|---|---|---|---|---|
| 2026-08-31 | evidence v2 release | 1 | 56 | 491,520 B | `5c327878…e266dd8122` | 5 min 20 s |
| 2026-11-18 | `a2bf029d` (strongest genome-wide signal) | 2 | 56 | 491,520 B | `a96a1d9e…0a9eecdf` | 5 min 40 s |

The 2026-11-18 build:

- Fingerprint: `dd32f25a…bc2811170`.
- Payloads: 347,452 B compressed, 2,815,051 B uncompressed (2,785,662 B in the
  previous build).
- Per gene: 0.7 s (ZFY) to 6.8 s (TTN).
- Peak RSS of the single build process: 5.8 GB. Per-gene build caches are
  cleared between genes, but the process RSS still grows over the run, so we
  run it when the host has that memory free beside the two uWSGI workers.
- Backup of the 2026-08-31 store and the scripts used:
  `/data/DataHub/deployments/strongest-signal/20261118T041140Z/store-rebuild/`.
