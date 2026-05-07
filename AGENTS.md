# DataHub Agent Instructions

Before creating or modifying operational scripts, read `SCRIPT_MANIFESTO.md`.

Scripts that process large biomedical/genomics data must be observable and
restartable by default. Do not add long-running scripts that lack progress
reporting, structured logging, resumable checkpoints, and an HPC/Slurm path
when the expected workload is too large for the current host.
