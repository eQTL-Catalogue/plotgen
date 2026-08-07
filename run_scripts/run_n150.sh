#!/bin/bash

#SBATCH --time=72:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=4G
#SBATCH --job-name="plot_data"

module load any/jdk/1.8.0_265
module load nextflow
module load any/singularity/3.7.3
module load squashfs/4.4
module load tabix

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n150/n150_ge_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n150/ge

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n150/n150_all_without_ge.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n150/others


# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n150_Bossini/n150_Bossini_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n150/Bossini


nextflow run main.nf -profile tartu_hpc  \
  --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/n150/n150_majiq_input.tsv\
  --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/n150/majiq

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/MacroMap/MacroMap_exon_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/MacroMap/exon


# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/MacroMap/MacroMap_tx_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/MacroMap/tx

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/MacroMap/MacroMap_txrev_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/MacroMap/txrev

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/MacroMap/MacroMap_leafcutter_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/MacroMap/leafcutter

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/MacroMap/MacroMap_leafcutter_input_tx_txrev_lc.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/MacroMap/tx_txrev_lc

# nextflow run main.nf -profile tartu_hpc -resume \
#   --studyFile /gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/MacroMap/MacroMap_majiq_input.tsv\
#   --outdir /gpfs/helios/projects/eQTLCatalogue/coverage_plots/MacroMap/majiq