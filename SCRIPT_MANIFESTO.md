# DataHub Script Manifesto

This file is the operating contract for DataHub scripts. Read it before adding
or changing any script under `scripts/` or any long-running pipeline entrypoint.

## Required Baseline

Every new operational script must include:

- Progress reporting that is visible during normal terminal runs.
- Structured logging with timestamps, levels, and enough context to diagnose the
  current source file, shard, archive, table, or processing unit.
- A resumable checkpoint or state file unless the script is trivially small.
- Idempotent behavior, or explicit reset flags when idempotency is impossible.
- A dry-run, smoke-test, or limit option when the full dataset is large.
- Clear output paths and no silent writes to unexpected locations.
- Final JSON or machine-readable summary when practical.
- Tests for parsing, checkpoint/resume behavior, and output contracts.

## Progress Rules

Long-running scripts must show progress without requiring the user to inspect
logs manually.

- Use a terminal progress bar or repeated progress log line.
- Include unit counts: files, archives, rows, genes, variants, shards, or bytes.
- Include the current item name where useful.
- Write a progress checkpoint JSON for long jobs so another shell can inspect
  status while the job is running.
- When exact totals are not available cheaply, report a defensible proxy such
  as compressed bytes read, completed shards, or completed archives.

## Resume Rules

Checkpointing must be scientifically and operationally safe.

- Checkpoint only after a unit is fully committed.
- On resume, skip completed units and clean incomplete unit rows before retrying.
- If mid-unit resume is not technically safe, document the unit granularity.
- Provide a reset flag that clearly rebuilds from scratch.
- Validate that checkpoint configuration matches the current run.

## Logging Rules

Logs must explain what the script is doing.

- Use Python `logging`, not bare prints, except for final machine-readable output.
- Include source path, output path, unit ID, row counts, elapsed time, and current
  item when applicable.
- Emit warnings for skipped, malformed, or suspicious records.
- Keep final summaries concise and parseable.

## Scale And HPC Rules

If the expected run is too large for a server or laptop, design for HPC first.

- Prefer DuckDB, Parquet, chunked CSV, or streaming readers over loading whole
  datasets into memory.
- Avoid millions of extracted tiny files when archive streaming is possible.
- Use async or multiprocessing only when it materially improves throughput and
  does not compromise reproducibility or memory safety.
- Provide Slurm/sbatch examples or profile support for jobs that need many CPUs,
  high memory, scratch storage, or long walltime.
- Make temp directories, memory limits, thread counts, and shard counts
  configurable.

## Scientific Data Rules

Pipeline scripts must preserve scientific meaning.

- Preserve source provenance as early as possible.
- Do not collapse source-specific observations into a single aggregate unless
  the aggregation contract is explicit and defensible.
- Keep raw/source labels alongside normalized labels.
- Prefer deduplication keys that reflect the biological entity and source
  contract, not just display labels.
- Never cap or massage results to hide source mismatches.

## Review Checklist

Before merging or pushing a new script, confirm:

- It has progress, logging, checkpoint/resume, reset, and smoke-test controls.
- It documents output tables/files and source assumptions.
- It can be stopped and restarted without silent duplication.
- It has focused tests.
- It avoids committing generated data, raw dumps, database files, or checkpoints.
