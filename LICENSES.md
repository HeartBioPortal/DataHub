# License and Redistribution Summary

This file summarizes licensing surfaces for the HBP 3.0 NAR release. It is not legal advice; final release approval should confirm source-specific terms.

## 1. HBP software license

DataHub's repository `LICENSE` states that repository metadata and documentation are licensed under Creative Commons Attribution 4.0 International (CC BY 4.0), while raw data may retain the licenses specified by original sources. Confirm whether code is intended to share the same license statement or needs a separate software license before public release.

## 2. HBP-derived redistributable outputs

HBP-derived metadata, schemas, manifests, documentation, and small examples may be redistributed under the repository license when they do not include restricted third-party data. Derived data artifacts inherit source-license constraints and should not be archived publicly until each source family has been reviewed.

## 3. Third-party open/public-domain or permissive sources

Examples likely include NCBI resources such as dbSNP, ClinVar, dbVar, NCBI Gene, and ALFA, subject to NCBI disclaimer and usage guidelines. These still require attribution/provenance and should preserve source version, access date, and source URL.

## 4. Third-party attribution-required sources

Sources such as UniProtKB, Gene Ontology, Reactome, EMBL-EBI resources, Ensembl, InterPro, GWAS Catalog, and related open scientific resources generally require source attribution and may carry specific terms of use. Preserve source names, URLs, versions, access dates, and license labels in DataHub artifacts.

## 5. Third-party non-commercial or license-restricted sources

DrugBank v5.1.12 is documented for this release as a licensed academic source. Treat it as CC BY-NC 4.0 / non-commercial academic-use terms only after final confirmation from the source page. Raw DrugBank full database files must not be committed or archived unless redistribution permission is explicit. Derived HBP drug records should preserve source/version/license metadata.

Open Targets Platform GraphQL-derived records should document the API endpoint, GraphQL query, variables, access date, source field names, and source terms. The expected endpoint is `https://api.platform.opentargets.org/api/v4/graphql`; confirm the exact license/terms for the release.

TOPMed-derived public frequency resources and dbVar nstd229/TOPMed structural-variant call-set derivatives require final source-term review before bulk redistribution.

## 6. Controlled-access or non-redistributable source data

Controlled individual-level human data are not redistributed through HBP. Restricted cohort data, controlled-access genotype/phenotype data, non-public MVP raw/source files, credentials, API keys, and protected data must not be committed or included in GitHub or Zenodo releases.
