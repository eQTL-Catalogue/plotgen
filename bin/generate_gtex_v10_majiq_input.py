#!/usr/bin/env python3

import csv
from pathlib import Path


OUTPUT = Path(
    "/gpfs/helios/home/kerimov/alasoo_lab/plotgen/input/GTEx_V10_all/"
    "majiq/GTEx_V10_input_majiq_all.tsv"
)
DATASET_METADATA = Path(
    "/gpfs/helios/home/kerimov/alasoo_lab/plotgen/"
    "dataset_metadata_incoming.tsv"
)
QCNORM = Path(
    "/gpfs/helios/projects/eQTLCatalogue/r8_run_folders/qcnorm/"
    "gtex_majiq_210726"
)
PLOTGEN = Path("/gpfs/helios/home/kerimov/alasoo_lab/plotgen")
QTLMAP = Path(
    "/gpfs/helios/projects/eQTLCatalogue/qtlmap/GTEx_v10_Kaur"
)
SAMPLE_META = Path(
    "/gpfs/helios/home/kerimov/alasoo_lab/SampleArcheology/studies/GTEx/"
    "v10_metadata/GTExV10_metadata.tsv"
)

TISSUES = [
    "adipose_subcutaneous",
    "adipose_visceral",
    "adrenal_gland",
    "artery_aorta",
    "artery_coronary",
    "artery_tibial",
    "brain_amygdala",
    "brain_anterior_cingulate_cortex",
    "brain_caudate",
    "brain_cerebellar_hemisphere",
    "brain_cerebellum",
    "brain_cortex",
    "brain_frontal_cortex",
    "brain_hippocampus",
    "brain_hypothalamus",
    "brain_nucleus_accumbens",
    "brain_putamen",
    "brain_spinal_cord",
    "brain_substantia_nigra",
    "breast",
    "fibroblast",
    "LCL",
    "colon_sigmoid",
    "colon_transverse",
    "esophagus_gej",
    "esophagus_mucosa",
    "esophagus_muscularis",
    "heart_atrial_appendage",
    "heart_left_ventricle",
    "kidney_cortex",
    "liver",
    "lung",
    "minor_salivary_gland",
    "muscle",
    "nerve_tibial",
    "ovary",
    "pancreas",
    "pituitary",
    "prostate",
    "skin_not_sun_exposed",
    "skin_sun_exposed",
    "small_intestine",
    "spleen",
    "stomach",
    "testis",
    "thyroid",
    "uterus",
    "vagina",
    "blood",
]

COLUMNS = [
    "dataset_id",
    "study_id",
    "quant_method",
    "qtl_group",
    "study_name",
    "credible_sets_file",
    "sample_meta",
    "vcf_file",
    "coverage_path",
    "usage_matrix_norm",
    "tpm_matrix",
    "exon_summ_stats_files",
    "all_summ_stats_files",
    "pheno_meta",
    "scaling_factors",
]


def load_sample_sizes() -> dict[str, int]:
    with DATASET_METADATA.open(newline="") as metadata:
        rows = csv.DictReader(metadata, delimiter="\t")
        return {
            row["dataset_id"]: int(row["sample_size"])
            for row in rows
            if row["study_id"] == "QTS000015" and row["quant_method"] == "majiq"
        }


def newest_sumstats_path(dataset_id: str) -> Path:
    candidates = [
        path
        for batch in QTLMAP.glob("GTEx_majiq_b*")
        for path in (
            batch / "sumstats" / dataset_id / "all",
            batch / "sumstats_batches" / dataset_id / "all",
        )
        if path.is_dir()
    ]
    if not candidates:
        raise FileNotFoundError(f"No nominal summary statistics found for {dataset_id}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def make_row(index: int, tissue: str) -> dict[str, str]:
    dataset_id = f"QTD{951 + index:06d}"
    exon_dataset_id = f"QTD{117 + 5 * index:06d}"

    return {
        "dataset_id": dataset_id,
        "study_id": "QTS000015",
        "quant_method": "majiq",
        "qtl_group": tissue,
        "study_name": "GTEx",
        "credible_sets_file": (
            "/gpfs/helios/projects/eQTLCatalogue/qtlmap/"
            f"eQTL_Catalogue_r8/susie/QTS000015/{dataset_id}/"
            f"{dataset_id}.credible_sets.parquet"
        ),
        "sample_meta": str(SAMPLE_META),
        "vcf_file": (
            "/gpfs/helios/projects/GTEx/genotypes/GTEx_v10/"
            "GTEx_v9.MAF001.vcf.gz"
        ),
        "coverage_path": (
            "/gpfs/helios/projects/eQTLCatalogue/coverage_parquet/GTEx_v10/"
            f"{tissue}.parquet"
        ),
        "usage_matrix_norm": str(
            QCNORM / tissue / "phenotype_matrix_qtlmap.tsv.gz"
        ),
        "tpm_matrix": str(QCNORM / tissue / "majiq_quantified_psis.tsv.gz"),
        "exon_summ_stats_files": str(
            PLOTGEN
            / "input/GTEx_V10_all/sumstats_files"
            / f"{exon_dataset_id}_sumstats.tsv"
        ),
        "all_summ_stats_files": str(newest_sumstats_path(dataset_id)),
        "pheno_meta": str(QCNORM / tissue / "updated_majiq_metadata.tsv.gz"),
        "scaling_factors": (
            "/gpfs/helios/projects/eQTLCatalogue/qcnorm2/QTS000015/"
            f"{tissue}/QTS000015/normalised/ge/per_million_normalised/"
            f"GTEx.{tissue}.scaling_factors.tsv.gz"
        ),
    }


def main() -> None:
    rows = [make_row(i, tissue) for i, tissue in enumerate(TISSUES)]
    sample_sizes = load_sample_sizes()
    if set(sample_sizes) != {row["dataset_id"] for row in rows}:
        raise ValueError("GTEx MAJIQ sample-size metadata does not match input rows")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as output:
        writer = csv.DictWriter(
            output, fieldnames=COLUMNS, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)

    groups = {
        "small": [row for row in rows if sample_sizes[row["dataset_id"]] < 300],
        "medium": [
            row
            for row in rows
            if 300 <= sample_sizes[row["dataset_id"]] <= 600
        ],
        "big": [row for row in rows if sample_sizes[row["dataset_id"]] > 600],
    }
    for group, group_rows in groups.items():
        group_output = OUTPUT.with_name(f"GTEx_V10_input_majiq_{group}.tsv")
        with group_output.open("w", newline="") as output:
            writer = csv.DictWriter(
                output, fieldnames=COLUMNS, delimiter="\t", lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(group_rows)
        print(f"Wrote {len(group_rows)} rows to {group_output}")

    print(f"Wrote {len(rows)} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
