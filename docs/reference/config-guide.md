# Config Guide

## `config/prep_profiles/`

Defines raw-column arbitration for legacy/raw association sources.

Typical contents:

- field candidate columns
- ancestry field mapping
- defaults

## `config/profiles/`

Defines dataset/modality validation policy.

Typical contents:

- required fields
- missing-value strategy
- unknown-value handling

## `config/sources/`

Defines source metadata and adapter defaults.

Typical contents:

- source identity
- adapter name
- modalities and dataset types
- access metadata
- default parameters

## `config/runtime_profiles/`

Defines execution behavior for environments.

Typical contents:

- path defaults
- CPU/memory/thread defaults
- scheduler and Slurm settings
- step-specific command settings

## `config/export_manifests/`

Defines analyzed export behavior.

Typical contents:

- promoted fields
- metadata preservation rules
- helper-driven derived fields
- additive publish fields
- serving preservation rules
- source overrides

## `config/phenotype_tree.json`

Defines canonical phenotype hierarchy.

This file influences:

- disease / trait grouping
- filter hierarchy
- canonical path reconstruction
- rollup logic

## Which config should I change?

- Raw column issue: `prep_profiles`
- Validation issue: `profiles`
- Source identity / defaults issue: `sources`
- Environment execution issue: `runtime_profiles`
- Published analyzed field issue: `export_manifests`
- Hierarchy / grouping issue: `phenotype_tree.json`
