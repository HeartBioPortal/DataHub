# New Source Onboarding

## Goal

When adding a new source, the objective is not just “make the data parse.” The objective is:

- preserve source identity
- map it into the canonical model coherently
- keep source-specific behavior isolated
- ensure publication can consume it without ad hoc backend logic

## Normal onboarding path

### 1. Add or update a source manifest

Location:

- `config/sources/`

Define:

- source ID
- adapter name
- modalities / dataset types
- access mode
- source metadata and defaults

### 2. Decide whether a prep profile is needed

If the raw input is structurally messy, add a prep profile first.

Location:

- `config/prep_profiles/`

### 3. Implement or reuse an adapter

Location:

- `src/datahub/adapters/`

An adapter should do source-specific parsing and produce canonical records. It should not contain environment-specific orchestration logic.

### 4. Register the adapter if needed

Location:

- `src/datahub/registry.py`

### 5. Add tests

At minimum, test:

- input parsing
- phenotype routing
- field mapping
- ancestry behavior if relevant
- metadata/provenance preservation if relevant

## When not to add a new canonical field

Do not promote a source-specific field into the global canonical schema just because one source has it.

Promote only when the field is:

- reused across multiple sources, or
- required for querying/filtering/dedup/publication logic

Otherwise keep it in structured metadata.

## Where to put new analyzed fields

If the new field matters for analyzed outputs, define it through the export manifest framework, not as a hidden one-off in a publisher.
