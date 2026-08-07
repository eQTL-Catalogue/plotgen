#!/bin/bash

#SBATCH --time=72:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=4G
#SBATCH --job-name="plot_data_interval"
#SBATCH --partition=amd

module load any/jdk/1.8.0_265
module load nextflow
module load any/singularity/3.7.3
module load squashfs/4.4
module load tabix

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/INTERVAL_RNA/INTERVAL_RNA_ge_leafcutter_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/INTERVAL_RNA_ge_leafcutter/

nextflow run main.nf -profile tartu_hpc -resume \
  --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/INTERVAL_RNA/INTERVAL_RNA_WGS_ge_input.tsv \
  --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/INTERVAL_RNA_WGS/ge/

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/INTERVAL_RNA/INTERVAL_RNA_WGS_ge_leafcutter_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/INTERVAL_RNA_WGS_ge_leafcutter/
