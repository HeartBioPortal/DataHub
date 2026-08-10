# Gene Profile Schema

Gene profiles provide source-grounded identity, summary, protein, ontology, and
cross-reference evidence for HBP gene dossiers. DataHub builds them from
versioned local snapshots rather than live frontend or backend API calls.

## Artifact layout

The v1 manifest uses `artifact_subdir: gene_profile/v1`:

```text
secondary_analyses/final/gene_profile/v1/
  genes/HGNC_<ID>.json.gz
  gene_profile.index.parquet        # JSONL fallback when Parquet is unavailable
  symbol_to_hgnc.parquet            # JSONL fallback when Parquet is unavailable
  manifest.json
```

Payload filenames are HGNC identifiers with the colon replaced by an underscore;
`gene_id_normalized` remains the approved gene symbol in the serving table.

## Top-level payload

| Object | Meaning |
| --- | --- |
| `schema_version` | Contract identifier, currently `gene_profile.v1`. |
| `identity` | HGNC-controlled gene identity and external IDs. |
| `summary` | Display summary and its explicit fallback/source status. |
| `protein` | UniProtKB-centered protein metadata, with optional protein-context length fallback. |
| `ontology` | Compact GOA display terms and the total available GO annotation count. |
| `display` | Precomputed chips and source badges; these are presentation helpers, not independent evidence. |
| `quality_flags` | Missing-source and ambiguity flags. |
| `provenance` | Field-level source ownership records. |

## Identity fields

`identity` contains `hgnc_id`, approved `symbol`, `approved_name`, HGNC
`status`, `locus_group`, `locus_type`, aliases, previous symbols, cytogenetic
`location`, and `external_ids`. External IDs include Ensembl Gene, Entrez
Gene, RefSeq, and UniProt accessions when supplied by HGNC.

Aliases and previous symbols support lookup and reconciliation; they are not
alternative approved symbols.

## Summary selection

The summary contract preserves both:

- `long_summary`: the full NCBI Gene summary when available
- `one_sentence`: a compact first-sentence projection for cards

The fallback order is:

1. NCBI Gene summary
2. UniProtKB protein name plus function text
3. an HGNC-based identity sentence

`summary_source`, `summary_source_id`, `display_confidence`, and
`quality_flags.ncbi_summary_missing` make the fallback visible. A fallback
sentence is not equivalent to an NCBI-curated gene summary.

## Protein and ontology fields

`protein` contains the recommended protein name, primary UniProt accession,
reviewed status, amino-acid length, compact function summary, GO IDs, and
Reactome IDs. If UniProt length is unavailable, DataHub may use the canonical
isoform length from an existing HBP protein-context artifact.

`ontology.go_terms_display` groups up to five GOA terms per aspect
(`molecular_function`, `biological_process`, and `cellular_component`) and
preserves GO ID, term name, evidence code, reference, and assigning authority.
`go_terms_all_count` records the untruncated annotation count.

## Quality and provenance

Current quality flags are:

- `symbol_conflict`
- `multiple_uniprot_candidates`
- `ncbi_summary_missing`
- `unreviewed_uniprot_only`
- `non_protein_coding`
- `protein_context_missing`

Provenance entries identify which payload sections came from HGNC, NCBI Gene,
UniProtKB, GOA, or HBP protein context. Source snapshot paths and coverage counts
are stored in `manifest.json`.
