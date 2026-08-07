#!/bin/bash

#SBATCH --time=24:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=4G
#SBATCH --job-name="plot_data"

module load any/jdk/1.8.0_265
module load nextflow
module load any/singularity/3.7.3
module load squashfs/4.4
module load tabix

nextflow run main.nf -profile tartu_hpc -resume \
  --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/GTEx_V10_all/majiq/GTEx_V10_input_majiq_kidney_cortex.tsv\
  --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/GTEx_V10_all_majiq
