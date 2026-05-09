#!/bin/bash
#SBATCH --job-name=dbsnp_parquet
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --output=dbsnp_parquet_%A_%a.out
#SBATCH --error=dbsnp_parquet_%A_%a.err

set -euo pipefail

DATAHUB_ROOT="${DATAHUB_ROOT:-$PWD}"
RAW_ROOT="${RAW_ROOT:-${DATAHUB_ROOT}/raw_data/dbsnp}"
OUTPUT_ROOT="${OUTPUT_ROOT:-${DATAHUB_ROOT}/analyzed_data/dbsnp_frequency}"
BATCH_SIZE="${BATCH_SIZE:-250000}"
PYTHON="${PYTHON:-${DATAHUB_ROOT}/.venv/bin/python}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-60}"
COMPRESSION="${COMPRESSION:-zstd}"

cd "${DATAHUB_ROOT}"

mapfile -t ARCHIVES < <(find "${RAW_ROOT}" -maxdepth 1 -type f -name '*.tar.gz' | sort)
if [[ "${#ARCHIVES[@]}" -eq 0 ]]; then
  echo "No dbSNP frequency archives found under ${RAW_ROOT}" >&2
  exit 1
fi

TASK_INDEX="${SLURM_ARRAY_TASK_ID:-0}"
if [[ "${TASK_INDEX}" -ge "${#ARCHIVES[@]}" ]]; then
  echo "SLURM_ARRAY_TASK_ID=${TASK_INDEX} is outside archive count ${#ARCHIVES[@]}" >&2
  exit 1
fi

ARCHIVE="${ARCHIVES[${TASK_INDEX}]}"
echo "Exporting ${ARCHIVE} to ${OUTPUT_ROOT}"

"${PYTHON}" scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_parquet.py \
  --verbose \
  export-archive \
  --archive "${ARCHIVE}" \
  --output-root "${OUTPUT_ROOT}" \
  --batch-size "${BATCH_SIZE}" \
  --progress-interval "${PROGRESS_INTERVAL}" \
  --compression "${COMPRESSION}"
