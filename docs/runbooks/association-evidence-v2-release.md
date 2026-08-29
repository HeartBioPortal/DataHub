# Association Evidence v2 Release Candidate

Use this runbook only after the staged sidecar, backend, and frontend have passed
scientific, reconciliation, performance, and browser gates.

## Pre-deployment gates

- DataHub manifest and output SHA-256 verify.
- Every DataHub QA check passes.
- The manifest records the exact `--provider-chunk-rows` policy used for oversized sources.
- Source-completeness statuses match the archived inputs.
- The complete gene-keyed unavailable-provider source-summary index, file manifest, and checksums verify; no compact row is promoted to a provider association record.
- The schema 2.8 default-summary rollups, exact phenotype variant-ID sets, file manifest, and checksums verify against the complete source-summary index.
- Five bounded multiplicity fixtures reconcile provider rows, source
  observations, annotations, assertions, summaries, and exports.
- All v1/v2 category differences are classified and reviewed.
- The full comparison audit reuses the verified schema 2.8 distinct-membership rollup; it does not rescan compact source artifacts.
- Summary, detail, selected-rsID drill-down, and ZIP export use the same filter scope.
- Default search and explicit p-value searches pass performance limits.
- Browser tests pass for HMGCR and at least one high-volume gene.
- A timestamped environment/configuration backup and exact v1 rollback command exist.

## Deployment shape

Evidence v2 is an immutable sidecar selected by environment:

```text
HBP_ASSOC_EVIDENCE_MODEL=v2
HBP_ASSOC_EVIDENCE_V2_PATH=/data/DataHub/releases/association-evidence-v2/RELEASE_ID/association_evidence_v2.duckdb
HBP_ASSOC_EVIDENCE_V2_SERVING_MANIFEST=/data/DataHub/releases/association-evidence-v2/RELEASE_ID/serving/serving-manifest.json
HBP_ASSOC_ARTIFACT_ROOT=/data/DataHub/analyzed_data/association_new/final
```

Do not overwrite `association_serving_slim.duckdb`. Keep the prior backend
environment and v1 artifacts available until post-deployment validation completes.
The backend opens the normalized sidecar and serving package read-only. It resolves registered unavailable-provider summaries from the staged gene-keyed index and retains `HBP_ASSOC_ARTIFACT_ROOT` only as a pre-release fallback when that index is absent. Production v2 validation requires the complete index; request-time parsing of large compact JSON artifacts is not an accepted deployment state. The backend retains an explicit v1 fallback. It never reconstructs MVP provider or study records.

Build the release-candidate serving tree with
`--variant-bucket-characters 3` and a checksum-verified
`--coarse-serving-root`. Provider and association records then use
4,096 bounded SHA-256 prefix buckets; the table-specific width is recorded in
the serving manifest and read by the backend. This changes only physical query
partitioning, not normalized evidence, counts, filters, or provenance.

After the complete source-summary index verifies, run **build_association_evidence_v2_source_summary_rollup.py**. Production validation requires **schema_version=2.8.0-rc1**. The backend may use these rollups only for an unfiltered default summary and exact CVD phenotype counting. Any phenotype-path or explicit p-value filter, drill-down, export, or provenance request continues to use the full indexed rows.

## Approved migration sequence

1. Verify the staged SHA-256 manifest and DataHub QA report without opening the file
   for writing.
2. Copy the immutable sidecar, complete serving partition tree, and serving manifest
   into a new release-ID directory; never reuse an existing release path. Preserve or
   dereference hard links deliberately during the copy and verify every copied
   checksum.
3. Save a timestamped copy of the backend service environment and current v1/v2
   selector values. Record the deployed backend/frontend commits and build checksum.
4. Set the two environment variables above and restart only the backend during an
   approved maintenance window. No DuckDB import or schema migration is performed.
5. Require exact HMGCR, PCSK9, LDLR, TTN, and ANK2 summary/detail/export
   reconciliation, source-completeness status, API latency, browser, CAPTCHA export,
   and log-health gates before declaring the release live.
6. Preserve the staged release, copied release, v1 serving DB, previous environment,
   and all validation reports for the rollback window.

## Rollback

Restore the timestamped prior environment or set:

```text
HBP_ASSOC_EVIDENCE_MODEL=v1
```

Then restart only the backend in an approved window and repeat the recorded v1
health/search/download checks. The v2 sidecar can remain on disk because v1 mode does
not open it. No DataHub artifact deletion, reverse transformation, or database
conversion is required. If rollback validation fails, keep the service stopped and
restore the complete timestamped service environment rather than editing data.

The application default remains v1. A deployment enters v2 only when the selector,
normalized sidecar, and serving manifest are all explicitly configured. Missing or
invalid serving partitions fail the v2 validation gate; they are not replaced by a
request-time global scan.
