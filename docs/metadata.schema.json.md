# Metadata Schema

The `metadata.json` file contains descriptive information about each dataset. Required fields include:

- `dataset_id` – unique identifier used in folder names
- `title` – human readable title
- `authors` – list of contributors
- `doi_or_preprint` – DOI or preprint URL
- `reference_genome` – reference build used
- `data_type` – type of data files (e.g. VCF)
- `disease_category` – MONDO disease identifier
- `ancestry` – sample ancestry information
- `license` – data usage license
- `version` – dataset version
- `md5` – checksum of the primary data file

See [`schemas/metadata.schema.json`](../schemas/metadata.schema.json) for the complete JSON schema.
