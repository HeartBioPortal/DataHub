#!/bin/bash
#SBATCH --job-name=hbp_protein_context
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=04:00:00
#SBATCH --array=0-31%4
#SBATCH --output=/N/scratch/%u/hbp/logs/datahub/protein_context_%A_%a.out
#SBATCH --error=/N/scratch/%u/hbp/logs/datahub/protein_context_%A_%a.err

set -euo pipefail

DATAHUB_ROOT="${DATAHUB_ROOT:-$PWD}"
PYTHON="${PYTHON:-.venv/bin/python3}"
OUTPUT_ROOT="${OUTPUT_ROOT:-/N/scratch/$USER/hbp/secondary_analyses}"
VARIANT_VIEWER_ROOT="${VARIANT_VIEWER_ROOT:-$DATAHUB_ROOT/analyzed_data/variant_viewer}"
CACHE_ROOT="${CACHE_ROOT:-/N/scratch/$USER/hbp/cache/datahub}"
CACHE_PATH="${CACHE_PATH:-$CACHE_ROOT/protein_context_api_cache_${SLURM_ARRAY_TASK_ID:-0}.json}"
UNIT_PARTITIONS="${UNIT_PARTITIONS:-32}"
UNIT_PARTITION_INDEX="${UNIT_PARTITION_INDEX:-${SLURM_ARRAY_TASK_ID:-0}}"
MAX_ISOFORMS="${MAX_ISOFORMS:-12}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.2}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-45}"
PROGRESS_EVERY="${PROGRESS_EVERY:-25}"

mkdir -p "/N/scratch/$USER/hbp/logs/datahub"
mkdir -p "$CACHE_ROOT"

cd "$DATAHUB_ROOT"

if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi

extra_args=()
if [[ -n "${INCLUDE_GENES_FILE:-}" ]]; then
  extra_args+=(--include-genes-file "$INCLUDE_GENES_FILE")
fi
if [[ "${SKIP_EBI_PROTEINS:-0}" == "1" ]]; then
  extra_args+=(--protein-context-skip-ebi-proteins)
fi
if [[ "${SKIP_INTERPRO:-0}" == "1" ]]; then
  extra_args+=(--protein-context-skip-interpro)
fi
if [[ -n "${PROTEIN_CONTEXT_LIMIT:-}" ]]; then
  extra_args+=(--protein-context-limit "$PROTEIN_CONTEXT_LIMIT")
fi

"$PYTHON" scripts/dataset_specific_scripts/unified/run_secondary_analyses.py generate \
  --analyses protein_context \
  --output-root "$OUTPUT_ROOT" \
  --variant-viewer-root "$VARIANT_VIEWER_ROOT" \
  --protein-context-cache-path "$CACHE_PATH" \
  --protein-context-timeout-seconds "$TIMEOUT_SECONDS" \
  --protein-context-sleep-seconds "$SLEEP_SECONDS" \
  --protein-context-max-isoforms "$MAX_ISOFORMS" \
  --protein-context-progress-every "$PROGRESS_EVERY" \
  --unit-partitions "$UNIT_PARTITIONS" \
  --unit-partition-index "$UNIT_PARTITION_INDEX" \
  --log-level INFO \
  "${extra_args[@]}"
