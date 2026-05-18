# Drug Discovery Schema

Documentation-only schema for HBP 3.0 drug-discovery payloads.

| Field | Description |
| --- | --- |
| `gene` | Target gene symbol. |
| `molecule_name` | Drug or molecule name. |
| `molecule_id` | Source molecule identifier. |
| `molecule_source` | Open Targets, DrugBank, or other source. |
| `target_id` | Source target identifier. |
| `source_action_type` | Mechanism/action type when available. |
| `source_indication` | Disease or indication context. |
| `source_trial_phase_or_status` | Trial phase or approval/status field. |
| `source_license` | Source license or terms. |
| `provenance` | Source and transformation provenance object. |

DrugBank-derived records must preserve DrugBank version and license status and must not expose raw full-database content unless redistribution is permitted.
