# Protein Consequence rsID Index

The `protein_consequence_rsid_v2.0` artifact supplies variant identity to the
Protein Consequence Viewer without inferring an rsID from an amino-acid
position. It adds an identity-preserving annotation layer without discarding the existing
legacy viewer rows. Legacy rows whose source pipeline already removed the rsID
remain explicitly unresolved; genes without a v2 payload continue to use the
legacy data alone.

## Scientific contract

The artifact keeps two separate collections:

1. `association_context_definitions` stores each dataset-type, exact-phenotype-path,
   and source definition once. `association_context_links_by_variant` links each
   displayed rsID to those definitions and retains minimum reported p-value and
   source-summary count for the exact context. This dictionary encoding avoids
   repeating the same phenotype metadata thousands of times.
2. `annotations` records imported protein consequences. Each row retains its
   rsID, gene, allele, genomic position, transcript, protein, HGVS consequence,
   protein coordinates, and annotation provenance.

The VEP index retains every supplied source row. Per-gene viewer payloads retain every
positive protein-coordinate row for an association rsID and record matched
non-protein and unmatched rsIDs separately.

The collections are linked by **rsID and gene**. We do not match an rsID from
amino-acid position, consequence label, or protein change alone. One rsID can
have multiple allele/transcript consequence rows; one displayed residue can
therefore represent multiple rsIDs and multiple annotations. Protein ID remains
part of the coordinate identity, so annotations from different proteins are not
treated as the same exact position merely because their numeric positions match.

## Annotation source

The initial v2 build uses local Ensembl Variant Effect Predictor output:

| Field | Value |
| --- | --- |
| Tool | Ensembl VEP |
| Version | 114 |
| Assembly | GRCh38 |
| dbSNP input | NCBI dbSNP build 157, GRCh38.p14 |
| VEP index selection | Every valid rsID-and-gene row in the supplied VEP annotation CSV is retained |
| Per-gene viewer selection | Exact rsID-and-gene matches with a positive protein coordinate |

Legacy viewer consequences came from SnpEff-derived fields in the legacy
association artifacts. Those CSVs removed rsID before publication and then
deduplicated by phenotype/protein-change fields. A legacy row that cannot be
linked from an identity-preserving source remains unresolved; the v2 build does
not guess an identifier.

## Per-gene payload

```json
{
  "schema_version": "protein_consequence_rsid_v2.0",
  "gene": "PCSK9",
  "annotation": {
    "source": "Ensembl VEP",
    "version": "114",
    "genome_assembly": "GRCh38",
    "representative_transcript_selected": false
  },
  "association_context_definitions": [
    {
      "context_id": "protein-association-context:...",
      "dataset_type": "CVD",
      "phenotype_path": ["vascular_diseases", "coronary_artery_disease"],
      "source": "million_veteran_program"
    }
  ],
  "association_context_links_by_variant": {
    "rs11591147": [[0, 1, 2.0e-8]]
  },
  "annotations": [
    {
      "variant_id": "rs11591147",
      "transcript_id": "ENST...",
      "protein_id": "ENSP...",
      "hgvs_protein": "p.Arg46Leu",
      "protein_position": 46,
      "consequence": "missense_variant",
      "annotation_source": "Ensembl VEP",
      "annotation_version": "114",
      "genome_assembly": "GRCh38"
    }
  ]
}
```

`unresolved_variant_ids` contains association rsIDs for which the supplied VEP
input has no gene-matching protein annotation. This is expected for intronic,
intergenic, regulatory, noncoding, and otherwise protein-unresolved variants.
It is not an error and no protein position is fabricated. `non_protein_variant_ids`
contains rsIDs that have a gene-matching VEP row but no positive protein
coordinate, so they also remain off the lollipop axis.

The release build is limited to approved symbols from the gene-profile v1
index. A gene with no positive protein-coordinate annotation is recorded in
the build checkpoint and manifest, but no empty serving payload is published;
the backend can therefore retain its legacy viewer fallback.

## Serving behavior

The backend always retains matching legacy `variant_viewer` rows. When a gene
payload exists under `HBP_PROTEIN_CONSEQUENCE_V2_PATH`, it appends the exact
rsID-and-gene-matched VEP transcript consequences after applying the same CVD
and trait selection. New rows carry `rsid_resolution=source_preserved`; old rows
whose compact schema lost identity carry
`rsid_resolution=unresolved_legacy_compact`. The chart and CAPTCHA-protected
download use this same hybrid row set. For genes without a v2 payload, the
legacy rows remain available alone.

The frontend groups markers only for display. Selecting a grouped residue lists
the union of its retained rsIDs and every distinct consequence label; selecting
an expanded dot lists the rsID for that exact annotation. If a grouped residue
contains both unresolved legacy rows and source-preserved rsID rows, the marker
label and color use an rsID-bearing row. The existing source-count field and
stable row identity resolve ties among eligible rows. This is deterministic
display precedence, not a consequence-severity rule. Grouping never rewrites the
stored data.

## Heterogeneity audit

The 2026-09-05 source-index audit used three distinct keys:

| Question | Grouping key | Interpretation |
| --- | --- | --- |
| Does one exact variant/protein coordinate carry multiple labels? | `(gene, rsID, protein_id, protein_position_start)` | Exact annotation-label conflict |
| Does one rsID have different labels at the same numeric position across isoforms? | `(gene, rsID, protein_position_start)` | Cross-isoform numeric-position difference |
| Do different rsIDs at one residue have different labels? | `(gene, protein_id, protein_position_start)` | Residue-level variant diversity |

Results for the VEP 114 index:

| Measure | Count |
| --- | ---: |
| Exact variant-protein-position groups | 411,396 |
| Exact groups with multiple consequence labels | 0 |
| Numeric-position groups across isoforms | 364,268 |
| Numeric-position groups with cross-isoform label differences | 85 (0.023334%) |
| Exact protein residues with multiple rsIDs | 11,215 |
| Multi-rsID residues with multiple consequence labels | 6,392 |

The full compact audit is stored with the release staging evidence and contains
the exact SQL, all 85 cross-isoform numeric-position rows, all 6,392 mixed
residues, and SHA-256 checksums. The validation genes ANK2, BMPR2, HMGCR, PCSK9,
and TTN had zero exact variant-protein-position conflicts in this index.
