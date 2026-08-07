# Input TSV Generation Guide

This document explains how to generate study input TSV files for this repository in a consistent, reproducible way.

## Purpose

Input TSV files are consumed by `main.nf` and define all per-dataset inputs required by the plotting pipeline.

Use this guide whenever creating a new study TSV (for any study and quantification method).

## Canonical Schema (15 columns, exact order)

Always use this exact header and order:

`dataset_id	study_id	quant_method	qtl_group	study_name	credible_sets_file	sample_meta	vcf_file	coverage_path	usage_matrix_norm	tpm_matrix	exon_summ_stats_files	all_summ_stats_files	pheno_meta	scaling_factors`

Reference:
- `test_data/GEUVADIS_test_input.tsv`
- `main.nf` (`splitCsv` mapping for `params.studyFile`)

## Core Mapping Rules

- One row = one `qtl_group` (often same as `sample_group` in mapping tables).
- `dataset_id` must match the row quantification method (`quant_method`).
- `study_id` and `study_name` should be constant within a study file unless intentionally mixing studies.
- Prefer absolute paths for production HPC runs.

## Required Source Tables

At minimum, gather:

- `dataset_id_map.tsv`
  - use to map `(study_label, sample_group, quant_method) -> dataset_id, study_id`
- Study normalization table (example: `qtlmap_inputs.tsv`)
  - provides `count_matrix`, `sample_meta`, `vcf`, `tpm_file`, `pheno_meta`
- qtlmap outputs root(s)
  - provides `susie`, `sumstats_batches`, and related per-dataset outputs
- Local sample metadata copy under `/gpfs/helios/home/kerimov/alasoo_lab/SampleArcheology`
  - use this for `sample_meta`, even when normalization/source tables point at another `SampleArcheology` location

## Column-by-Column Guidance

- `dataset_id`: from mapping table for the target `quant_method`.
- `study_id`: from mapping table.
- `quant_method`: one of `ge`, `exon`, `tx`, `txrev`, `leafcutter`, `majiq` (as supported by workflow).
- `qtl_group`: typically `sample_group`.
- `study_name`: study label used in UI/output (for example `MacroMap`).
- `credible_sets_file`: usually `<qtlmap_root>/susie/<dataset_id>/<dataset_id>.credible_sets.parquet`.
- `sample_meta`: sample metadata TSV path from the local `/gpfs/helios/home/kerimov/alasoo_lab/SampleArcheology` copy.
- `vcf_file`: study genotype VCF path from normalization/source table.
- `coverage_path`: per-group coverage parquet path (or agreed placeholder if intentionally missing).
- `usage_matrix_norm`: quant-specific split-normalized matrix (for example `.../normalised/<quant>/qtl_group_split_norm/...`).
- `tpm_matrix`: quant-specific per-million-normalized file (see quant rules below).
- `exon_summ_stats_files`: should point to exon dataset sumstats batches for the same `qtl_group`.
- `all_summ_stats_files`: should point to current-row dataset sumstats batches for the row quant method.
- `pheno_meta`: quant-specific phenotype metadata file.
- `scaling_factors`: usually GE scaling factors per `qtl_group`.

## Quant-Specific `tpm_matrix` Rules

Do not reuse GE median TPM paths for non-GE methods.

Use quant-specific per-million-normalized files:

- `ge`: `.../normalised/ge/qtl_group_median_tpms/...` (or study-established GE convention)
- `exon`: `.../normalised/exon/per_million_normalised/...exon_counts_TPM_norm.tsv.gz`
- `tx`: `.../normalised/tx/per_million_normalised/...transcript_usage_TPM_norm.tsv.gz`
- `txrev`: `.../normalised/txrev/per_million_normalised/...txrevise_TPM_norm.tsv.gz`
- `leafcutter`: `.../normalised/leafcutter/per_million_normalised/...leafcutter_CPM_norm.tsv.gz`
- `majiq`: use MAJIQ PSI quantification output, not GE TPMs:
  - `/gpfs/helios/projects/eQTLCatalogue/r8_run_folders/rnaseq/{study_name}/{qtl_group}/majiq/majiq_quantified_psis/majiq_quantified_psis.tsv.gz`

## Quant-Specific `pheno_meta` Rules

Use metadata matching quant method:

- `ge`: `.../gene_counts_Ensembl_105_phenotype_metadata.tsv.gz`
- `exon`: `.../exon_counts_Ensembl_105_phenotype_metadata.tsv.gz`
- `tx`: `.../transcript_usage_Ensembl_105_phenotype_metadata.tsv.gz`
- `txrev`: `.../txrevise_Ensembl_105_phenotype_metadata.tsv.gz`
- `leafcutter`: quant-specific leafcutter metadata file (often per-group or study file)
- `majiq`: MAJIQ metadata file (for example `.../normalised/majiq/..._majiq_metadata.tsv.gz`)

## Sumstats Mapping Rules (Important)

- `all_summ_stats_files`:
  - use the current row dataset id and quant method.
  - pattern example: `<qtlmap_root_for_quant>/sumstats_batches/<dataset_id>/all`
- `exon_summ_stats_files`:
  - map by same `qtl_group` to the **exon** dataset_id using `dataset_id_map.tsv`.
  - do not copy `all_summ_stats_files` blindly.

## Generation Workflow

1. Select study and quant methods to generate.
2. Build mapping dictionaries from `dataset_id_map.tsv`:
   - `(qtl_group, quant) -> dataset_id`
3. Read normalization inputs table and index by dataset id.
4. Construct rows for each target quant and `qtl_group`.
5. Validate existence of all non-placeholder paths.
6. Write TSV(s) with canonical header order.
7. Re-read written files and re-check critical columns (`tpm_matrix`, sumstats columns).

## Validation Checklist

Before finalizing:

- Header exactly matches canonical 15-column schema and order.
- Row count equals expected number of groups for the study/quant.
- `dataset_id` corresponds to row `quant_method`.
- `sample_meta` points to `/gpfs/helios/home/kerimov/alasoo_lab/SampleArcheology`, not a project-level `SampleArcheology` copy.
- `tpm_matrix` matches quant-specific pattern.
- `exon_summ_stats_files` uses exon dataset IDs, not row dataset IDs (unless row is exon).
- `all_summ_stats_files` uses row dataset ID.
- All required file paths exist, except explicitly agreed placeholders.

## Common Pitfalls

- Using same path for `exon_summ_stats_files` and `all_summ_stats_files` for non-exon rows.
- Copying `sample_meta` directly from `qtlmap_inputs.tsv` when it points outside the local `SampleArcheology` checkout.
- Carrying GE `tpm_matrix` paths into `exon`, `tx`, `txrev`, `leafcutter`, or `majiq` TSVs.
- Mixing `MacroMap_ge` and `MacroMap_other` qtlmap roots incorrectly.
- Forgetting quant-specific `pheno_meta`.
- Changing header order (workflow mapping is positional by column name and expected schema).

## Output Naming Convention (Recommended)

For study `X` and quant `Q`:

- `input/X/X_Q_input.tsv`

Examples:
- `input/MacroMap/MacroMap_ge_input.tsv`
- `input/MacroMap/MacroMap_exon_input.tsv`
- `input/MacroMap/MacroMap_tx_input.tsv`
- `input/MacroMap/MacroMap_txrev_input.tsv`
- `input/MacroMap/MacroMap_leafcutter_input.tsv`

## Minimal Review Command Set (Optional)

After generating files, verify quickly:

- check line counts (`rows + header`)
- inspect first 2 rows
- spot-check one row per quant for `tpm_matrix`, `exon_summ_stats_files`, `all_summ_stats_files`

If any mismatch is found, fix generation logic first and regenerate, rather than editing many rows manually.
