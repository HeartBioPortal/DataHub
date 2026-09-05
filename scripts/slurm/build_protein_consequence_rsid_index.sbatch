#!/bin/bash
#SBATCH --job-name=hbp-protein-rsid
#SBATCH --cpus-per-task=20
#SBATCH --mem=64G
#SBATCH --time=72:00:00
#SBATCH --output=logs/protein_rsid_%j.out
#SBATCH --error=logs/protein_rsid_%j.err

set -euo pipefail

: "${DATAHUB_ROOT:?Set DATAHUB_ROOT}"
: "${VEP_ANNOTATIONS:?Set VEP_ANNOTATIONS}"
: "${VARIANT_INDEX_ROOT:?Set VARIANT_INDEX_ROOT}"
: "${OUTPUT_ROOT:?Set OUTPUT_ROOT}"

cd "$DATAHUB_ROOT"
source .venv/bin/activate

VEP_INDEX="${VEP_INDEX:-$OUTPUT_ROOT/vep_annotations_114_grch38.duckdb}"
GENE_PROFILE_INDEX="${GENE_PROFILE_INDEX:-$DATAHUB_ROOT/secondary_analyses/final/gene_profile/v1/gene_profile.index.jsonl}"
mkdir -p "$OUTPUT_ROOT" "${SLURM_TMPDIR:-$OUTPUT_ROOT/tmp}"

python scripts/dataset_specific_scripts/unified/build_protein_consequence_rsid_index.py \
  --verbose index-vep \
  --annotations-csv "$VEP_ANNOTATIONS" \
  --output-db "$VEP_INDEX" \
  --threads "${SLURM_CPUS_PER_TASK:-20}" \
  --memory-limit "${MEMORY_LIMIT:-56GB}" \
  --temp-directory "${SLURM_TMPDIR:-$OUTPUT_ROOT/tmp}" \
  --log-path "$OUTPUT_ROOT/index-vep.log"

python scripts/dataset_specific_scripts/unified/build_protein_consequence_rsid_index.py \
  --verbose build \
  --variant-index-root "$VARIANT_INDEX_ROOT" \
  --vep-index "$VEP_INDEX" \
  --gene-profile-index "$GENE_PROFILE_INDEX" \
  --output-root "$OUTPUT_ROOT/v2" \
  --workers "${SLURM_CPUS_PER_TASK:-20}" \
  --progress-interval "${PROGRESS_INTERVAL:-25}" \
  --log-path "$OUTPUT_ROOT/build.log"
