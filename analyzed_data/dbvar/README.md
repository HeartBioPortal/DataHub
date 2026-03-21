# dbVar Analyzed Structural Variant Artifacts

This directory holds curated dbVar structural-variant artifacts that are useful as merge seeds, metadata seeds, or stable legacy-contract baselines.

Dataset type:

- DataHub-produced or DataHub-consumable analyzed structural-variant JSON artifacts
- payloads shaped for downstream reuse rather than raw upstream interchange

Naming convention:

- tracked baseline or seed artifacts: `dbvar_structural_variants_<artifact_role>.json.zip`
- local run outputs: `dbvar_structural_variants_<study_id>.json`
- local run reports: `dbvar_structural_variants_<study_id>.report.json`

Current tracked artifact:

- `dbvar_structural_variants_legacy_seed.json.zip`

Notes:

- keep only curated, reusable artifacts under version control
- routine run outputs can live here locally without being committed
