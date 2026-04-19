#!/bin/bash
#SBATCH --job-name=hbp_summary_upgrade
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=160G
#SBATCH --time=12:00:00
#SBATCH --output=/N/scratch/%u/hbp/logs/datahub/summary_upgrade_%j.out
#SBATCH --error=/N/scratch/%u/hbp/logs/datahub/summary_upgrade_%j.err

set -euo pipefail

DATAHUB_ROOT="${DATAHUB_ROOT:-$PWD}"
DB_PATH="${DB_PATH:-/N/scratch/$USER/hbp/datamart/association_serving.duckdb}"
PYTHON="${PYTHON:-.venv/bin/python3}"
BATCH_SIZE="${BATCH_SIZE:-1}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-100}"
SLOW_PAYLOAD_SECONDS="${SLOW_PAYLOAD_SECONDS:-30}"
UNIT_PARTITIONS="${UNIT_PARTITIONS:-1}"
PAYLOAD_SOURCE="${PAYLOAD_SOURCE:-auto}"
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-120GB}"
DUCKDB_TEMP_DIRECTORY="${DUCKDB_TEMP_DIRECTORY:-/N/scratch/$USER/hbp/datamart/duckdb_tmp}"

mkdir -p "/N/scratch/$USER/hbp/logs/datahub"
mkdir -p "$DUCKDB_TEMP_DIRECTORY"

cd "$DATAHUB_ROOT"

if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi

for partition_index in $(seq 0 $((UNIT_PARTITIONS - 1))); do
  echo "Starting summary upgrade partition ${partition_index}/${UNIT_PARTITIONS}"

  replace_args=()
  if [[ "${REPLACE_SUMMARY_TABLES:-0}" == "1" && "$partition_index" == "0" ]]; then
    replace_args+=(--replace-summary-tables)
  fi

  "$PYTHON" scripts/dataset_specific_scripts/unified/upgrade_association_serving_duckdb.py \
    --db-path "$DB_PATH" \
    --batch-size "$BATCH_SIZE" \
    --progress-interval "$PROGRESS_INTERVAL" \
    --slow-payload-seconds "$SLOW_PAYLOAD_SECONDS" \
    --payload-source "$PAYLOAD_SOURCE" \
    --unit-partitions "$UNIT_PARTITIONS" \
    --unit-partition-index "$partition_index" \
    --duckdb-threads "${SLURM_CPUS_PER_TASK:-4}" \
    --duckdb-memory-limit "$DUCKDB_MEMORY_LIMIT" \
    --duckdb-temp-directory "$DUCKDB_TEMP_DIRECTORY" \
    --log-level INFO \
    "${replace_args[@]}"
done

echo "Summary upgrade finished."
