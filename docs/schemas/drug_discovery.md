# Drug Discovery Schema

The HBP Drugs & Compounds layer is a per-gene list of molecule records assembled
from Open Targets Platform and licensed DrugBank data. DataHub currently records
source manifests, licensing metadata, and the imported payload contract; the
upstream merge is not yet implemented as a canonical DataHub adapter/publisher.
Both source manifests therefore remain `catalog_only` in DataHub even though
legacy/imported per-gene payloads are available to HBP.

## Source and license contract

- Open Targets records originate from the Platform GraphQL API. Reproducible
  builds preserve the endpoint, query, variables, access date, release/version
  when available, and original field names.
- DrugBank records originate from the licensed 5.1.12 all-full-database academic
  dataset. Raw DrugBank files are not redistributed. Derived use remains subject
  to the recorded CC BY-NC 4.0/DrugBank access terms and non-commercial limits.

The source metadata lives in `config/sources/open_targets.json` and
`config/sources/drugbank.json`.

## Per-gene payload

Each `analyzed_data/drug_discovery/merged/<GENE>.json` file is an array. Common
fields include:

| Field | Meaning |
| --- | --- |
| `molecule_name`, `molecule_type` | Display identity and molecule class. |
| `source` | `drugbank`, `opentargets`, or `merged`. |
| `target`, `target_class` | Gene/target and target class where supplied. |
| `action_type` | Source action or mechanism category. |
| `drugbank_ids`, `chembl`, `chembl_uri` | Source molecule identifiers. |
| `clinical_trial_phase`, `trial_status`, `has_been_withdrawn` | Open Targets development/status context when available. |
| `efo` | Open Targets disease/indication identifier when available. |
| `indication`, `moa`, `description` | Source narrative fields. |
| `groups`, `state`, `classification`, `pathways` | DrugBank classification/context fields. |
| `pharmacodynamics`, `absorption`, `half_life`, `protein_binding` | DrugBank pharmacology fields when licensed and included. |
| `possible_adverse_events` | Source-derived adverse-event context when present; values require source-specific interpretation. |

Fields are sparse and source-dependent. Absence means “not present in this
record,” not a negative scientific assertion.

## Meaning of `source: merged`

A `merged` record represents a gene-molecule entry for which the import process
combined complementary Open Targets and DrugBank fields. It is not a third data
source, a pooled statistical estimate, or stronger evidence by itself. Source
identifiers and source-specific fields remain necessary to understand what each
resource contributed.

## Interpretation boundary

Drug-target links, indications, trial phases, and mechanism descriptions provide
translational context. They do not establish that a drug is effective or safe
for the queried cardiovascular phenotype. Users must follow the source record,
regulatory status, and trial evidence.
