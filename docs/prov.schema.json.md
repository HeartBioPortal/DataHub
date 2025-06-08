# Provenance Schema

The `provenance.json` file captures how the dataset was produced. Required fields are:

- `wasDerivedFrom` – source dataset or processing step
- `etl_pipeline_hash` – git commit hash of the ETL code used
- `validator_version` – version of the validation tool
- `date_processed` – date the dataset was generated

See [`schemas/prov.schema.json`](../schemas/prov.schema.json) for the exact field definitions.
