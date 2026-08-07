#!/usr/bin/env bash
set -euo pipefail

INPUT_VCF="/gpfs/helios/projects/BLUEPRINT/genotypes/glimpse_020323/BLUEPRINT.filtered.vcf.gz"
OUTPUT_VCF="${1:-data/BLUEPRINT.filtered.vcf.gz}"

if ! command -v module >/dev/null 2>&1; then
  if [[ -f /etc/profile.d/modules.sh ]]; then
    # Make Environment Modules available when the script is run non-interactively.
    # shellcheck source=/dev/null
    source /etc/profile.d/modules.sh
  fi
fi

module load bcftools
module load vcftools

mkdir -p "$(dirname "$OUTPUT_VCF")"

echo "Input : $INPUT_VCF"
echo "Output: $OUTPUT_VCF"
echo "Setting VCF IDs to chrCHROM_POS_REF_ALT format..."

bcftools annotate \
  --set-id 'chr%CHROM\_%POS\_%REF\_%ALT' \
  -Oz \
  -o "$OUTPUT_VCF" \
  "$INPUT_VCF"

bcftools index -t -f "$OUTPUT_VCF"

echo "Done."
echo "Created:"
echo "  $OUTPUT_VCF"
echo "  ${OUTPUT_VCF}.tbi"
