# Contributing

## Contribution standard

The repository should remain understandable to outside contributors. That means changes should be explicit, scoped, and testable.

## When making a change

1. Identify the layer first.
2. Update config if the behavior is declarative.
3. Add or adjust tests at the behavior boundary.
4. Update documentation when the mental model or workflow changes.

## Review expectations

A good contribution should answer these questions clearly:

- What layer is changing?
- Why does the change belong there?
- Does it preserve or intentionally change the analyzed contract?
- Is provenance preserved or improved?
- What tests prove the behavior?

## Preferred extension points

- New source: source manifest + adapter + tests
- New raw-column mapping: prep profile + preparer tests
- New validation rule: dataset profile / contract + validation tests
- New analyzed field: export manifest + helper + publisher/builder tests
- New environment setup: runtime profile + orchestration tests if needed

## What to avoid

- hardcoding source-specific behavior into unrelated modules
- backend-only scientific fixes that should live in DataHub
- adding new fields without deciding whether they belong in schema, metadata, or analyzed output
- mixing runtime orchestration changes with scientific contract changes in one opaque edit

## Documentation rule

If a change affects how contributors understand the repository, update the docs in the same change set.
