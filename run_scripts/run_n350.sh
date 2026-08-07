#!/bin/bash

#SBATCH --time=48:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=4G
#SBATCH --job-name="plot_data"
#SBATCH --partition=amd

module load any/jdk/1.8.0_265
module load nextflow
module load any/singularity/3.7.3
module load squashfs/4.4
module load tabix

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_ge_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/ge

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_exon_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/exon


# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_others_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/others

nextflow run main.nf -profile tartu_hpc -resume \
  --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_all_Fusion_input.tsv \
  --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/Fusion_muscle_naive

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_tx_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/tx

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_txrev_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/txrev

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_leafcutter_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/leafcutter

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n350/n350_majiq_input.tsv \
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n350/majiq