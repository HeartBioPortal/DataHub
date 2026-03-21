# Analyzed Data Staging

This directory is the checked-in home for curated analyzed artifacts that are useful as small baselines, merge seeds, or reproducible examples.

Use it for:

- legacy-compatible analyzed outputs that downstream tools still need
- compact seed artifacts used to enrich or merge future ingestions
- small reference payloads that make pipeline runs reproducible across machines

Do not use it for:

- large bulk outputs from routine local runs
- temporary scratch artifacts
- files that can be deterministically rebuilt and do not need to be versioned

Recommended layout:

- `analyzed_data/<source_id>/...`

Examples:

- `analyzed_data/dbvar/structural_variants.json.zip`
- `analyzed_data/clinvar/...`

Guidelines:

- prefer compressed archives for versioned JSON artifacts when that keeps repository size reasonable
- treat tracked files here as curated baselines, not an append-only dump of every run
- write fresh local outputs here when convenient, but only commit the ones worth preserving
