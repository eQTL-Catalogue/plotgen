#!/usr/bin/env python3
"""Generate n150 qtlmap study input TSVs (canonical 15-column schema)."""

from __future__ import annotations

import argparse
import csv
import errno
import glob
import os
import sys
from pathlib import Path

ROOT = Path("/gpfs/helios/projects/eQTLCatalogue/r8_run_folders/qtlmap")
MAP_PATH = Path(__file__).resolve().parents[1] / "dataset_id_map.tsv"
GENOTYPE_PATH = Path(__file__).resolve().parents[1] / "study_genotype_paths.tsv"
COV_BASE = Path("/gpfs/helios/projects/eQTLCatalogue/coverage_parquet")
OUT_DIR = Path(__file__).resolve().parents[1] / "input" / "n150"

QTLMAP_ROOTS = {
    "ge": ROOT / "n150_ge",
    "exon": ROOT / "n150_exon",
    "tx": ROOT / "n150_other",
    "txrev": ROOT / "n150_other",
    "leafcutter": ROOT / "n150_other",
    "majiq": ROOT / "n150_other",
}

HEADER = (
    "dataset_id\tstudy_id\tquant_method\tqtl_group\tstudy_name\tcredible_sets_file\t"
    "sample_meta\tvcf_file\tcoverage_path\tusage_matrix_norm\ttpm_matrix\t"
    "exon_summ_stats_files\tall_summ_stats_files\tpheno_meta\tscaling_factors\n"
)


def load_qtlmap_merged() -> dict[str, dict[str, str]]:
    files = glob.glob(
        "/gpfs/helios/projects/eQTLCatalogue/r8_run_folders/qcnorm/**/qtlmap_inputs.tsv",
        recursive=True,
    )
    merged: dict[str, dict[str, str]] = {}
    for fp in files:
        with open(fp) as fh:
            for row in csv.DictReader(fh, delimiter="\t"):
                q = row["qtl_subset"].strip()
                merged[q] = row
    return merged


def load_genotype_paths() -> dict[str, str]:
    """study_label -> vcf path from study_genotype_paths.tsv."""
    out: dict[str, str] = {}
    with open(GENOTYPE_PATH) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            label = (row.get("study_label") or "").strip()
            vcf = (row.get("vcf_file") or "").strip()
            if label and vcf:
                out[label] = vcf
    return out


def load_dataset_map() -> tuple[
    dict[str, dict[str, str]],
    dict[tuple[str, str, str], str],
]:
    """dataset_id -> row; (study_label, sample_group, quant) -> dataset_id"""
    by_id: dict[str, dict[str, str]] = {}
    triple: dict[tuple[str, str, str], str] = {}
    with open(MAP_PATH) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            by_id[row["dataset_id"]] = row
            triple[(row["study_label"], row["sample_group"], row["quant_method"])] = row[
                "dataset_id"
            ]
    return by_id, triple


def scaling_path(count_matrix: str, study_id: str, qtl_group: str) -> Path:
    """GE scaling factors next to qtlmap count_matrix root."""
    if f"/{study_id}/normalised/" not in count_matrix:
        raise ValueError(f"Cannot locate study root in count_matrix: {count_matrix}")
    prefix = count_matrix.split(f"/{study_id}/normalised/")[0]
    return Path(prefix) / study_id / "normalised" / "ge" / "per_million_normalised" / (
        f"{study_id}.{qtl_group}.scaling_factors.tsv.gz"
    )


def tpm_matrix_path(quant: str, count_matrix: str, qtlmap_row: dict[str, str]) -> str:
    if quant == "ge":
        return qtlmap_row["tpm_file"]
    p = Path(count_matrix)
    if p.parent.name != "qtl_group_split_norm":
        # majiq (and edge cases): use catalogue tpm_file (GE median TPM)
        return qtlmap_row["tpm_file"]
    stem = p.name.replace(".tsv.gz", "")
    sub = {
        "exon": f"{stem}.exon_counts_TPM_norm.tsv.gz",
        "tx": f"{stem}.transcript_usage_TPM_norm.tsv.gz",
        "txrev": f"{stem}.txrevise_TPM_norm.tsv.gz",
        "leafcutter": f"{stem}.leafcutter_CPM_norm.tsv.gz",
    }
    if quant not in sub:
        return qtlmap_row["tpm_file"]
    return str(p.parent.parent / "per_million_normalised" / sub[quant])


def exon_id_for_group(triple: dict[tuple[str, str, str], str], study: str, group: str) -> str:
    return triple[(study, group, "exon")]


def iter_susie_ids(qroot: Path) -> list[str]:
    sus = qroot / "susie"
    return sorted(p.name for p in sus.iterdir() if p.is_dir())


PATH_COLUMNS = (
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
)


def validate_paths(
    paths: list[str | Path], missing: list[str], warn: list[str]
) -> None:
    for p in paths:
        ps = Path(p)
        try:
            if ps.is_dir():
                if not any(ps.iterdir()):
                    missing.append(f"empty_dir: {ps}")
            elif not ps.is_file():
                missing.append(str(ps))
        except OSError as e:
            warn.append(f"cannot_stat ({e}): {ps}")


def validate_study_tsv(
    tsv_path: Path, *, relaxed: bool = False
) -> tuple[list[str], list[str], list[str]]:
    """Return (missing_or_empty, permission_errors, permission_warnings).

    When ``relaxed`` is True, permission problems are reported as warnings only
    (useful when the generator runs without full ACLs). Missing paths still fail.
    """
    missing: list[str] = []
    denied: list[str] = []
    warn: list[str] = []
    with open(tsv_path) as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for i, row in enumerate(reader, start=2):
            for col in PATH_COLUMNS:
                raw = (row.get(col) or "").strip()
                if not raw:
                    missing.append(f"{tsv_path.name}:{i}:{col}: empty")
                    continue
                p = Path(raw)
                try:
                    if p.is_dir():
                        if not any(p.iterdir()):
                            missing.append(f"{tsv_path.name}:{i}:{col}: empty_dir {p}")
                    elif p.is_file():
                        if not os.access(p, os.R_OK):
                            msg = f"{tsv_path.name}:{i}:{col}: not_readable {p}"
                            (warn if relaxed else denied).append(msg)
                    else:
                        missing.append(f"{tsv_path.name}:{i}:{col}: missing {p}")
                except OSError as e:
                    en = getattr(e, "errno", None)
                    msg = f"{tsv_path.name}:{i}:{col}: {e}: {p}"
                    if en == errno.ENOENT:
                        missing.append(f"{tsv_path.name}:{i}:{col}: missing {p}")
                    elif relaxed and en in (errno.EACCES, errno.EPERM):
                        warn.append(msg)
                    else:
                        denied.append(msg)
    return missing, denied, warn


def row_line(
    *,
    dataset_id: str,
    study_id: str,
    quant: str,
    qtl_group: str,
    study_name: str,
    vcf_file: str,
    qtlmap_row: dict[str, str],
    qroot: Path,
    exon_summ_stats: str,
    all_summ_stats: str,
    ge_count_for_scaling: str,
) -> str:
    cov = COV_BASE / study_name / f"{qtl_group}.parquet"
    tpm = tpm_matrix_path(quant, qtlmap_row["count_matrix"], qtlmap_row)
    scale = scaling_path(ge_count_for_scaling, study_id, qtl_group)
    cred = qroot / "susie" / dataset_id / f"{dataset_id}.credible_sets.parquet"
    fields = [
        dataset_id,
        study_id,
        quant,
        qtl_group,
        study_name,
        str(cred),
        qtlmap_row["sample_meta"],
        vcf_file,
        str(cov),
        qtlmap_row["count_matrix"],
        tpm,
        exon_summ_stats,
        all_summ_stats,
        qtlmap_row["pheno_meta"],
        str(scale),
    ]
    return "\t".join(fields) + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build n150 study input TSVs and validate referenced paths."
    )
    p.add_argument(
        "--relaxed",
        action="store_true",
        help="Only fail on missing paths; permission errors become warnings. "
        "Default is strict: all paths must exist and be readable.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    relaxed = bool(args.relaxed)

    qmap = load_qtlmap_merged()
    by_id, triple = load_dataset_map()
    genotype_paths = load_genotype_paths()
    access_warn: list[str] = []

    ge_root = QTLMAP_ROOTS["ge"]
    exon_root = QTLMAP_ROOTS["exon"]
    other_root = QTLMAP_ROOTS["tx"]

    ge_ids = set(iter_susie_ids(ge_root))
    exon_ids = set(iter_susie_ids(exon_root))
    other_ids = set(iter_susie_ids(other_root))

    # --- ge ---
    ge_rows: list[str] = []
    ge_missing: list[str] = []
    for gid in sorted(ge_ids):
        if gid not in by_id:
            ge_missing.append(f"dataset_id not in dataset_id_map: {gid}")
            continue
        info = by_id[gid]
        if info["quant_method"] != "ge":
            ge_missing.append(f"{gid} is not ge in map: {info['quant_method']}")
            continue
        row = qmap[gid]
        study, group = info["study_label"], info["sample_group"]
        if study not in genotype_paths:
            ge_missing.append(f"no vcf in study_genotype_paths.tsv for study_label={study}")
            continue
        vcf_ref = genotype_paths[study]
        try:
            eid = exon_id_for_group(triple, study, group)
        except KeyError:
            ge_missing.append(f"no exon mapping for {study} {group}")
            continue
        exon_ss = str(exon_root / "sumstats_batches" / eid / "all")
        all_ss = str(ge_root / "sumstats_batches" / gid / "all")
        ge_row = row_line(
            dataset_id=gid,
            study_id=info["study_id"],
            quant="ge",
            qtl_group=group,
            study_name=study,
            vcf_file=vcf_ref,
            qtlmap_row=row,
            qroot=ge_root,
            exon_summ_stats=exon_ss,
            all_summ_stats=all_ss,
            ge_count_for_scaling=row["count_matrix"],
        )
        ge_rows.append(ge_row)
        validate_paths(
            [
                row["sample_meta"],
                vcf_ref,
                COV_BASE / study / f"{group}.parquet",
                row["count_matrix"],
                tpm_matrix_path("ge", row["count_matrix"], row),
                exon_ss,
                all_ss,
                row["pheno_meta"],
                scaling_path(row["count_matrix"], info["study_id"], group),
                ge_root / "susie" / gid / f"{gid}.credible_sets.parquet",
            ],
            ge_missing,
            access_warn,
        )

    # --- exon ---
    exon_rows: list[str] = []
    exon_missing: list[str] = []
    for eid in sorted(exon_ids):
        if eid not in by_id:
            exon_missing.append(f"dataset_id not in dataset_id_map: {eid}")
            continue
        info = by_id[eid]
        if info["quant_method"] != "exon":
            exon_missing.append(f"{eid} is not exon in map")
            continue
        row = qmap[eid]
        study, group = info["study_label"], info["sample_group"]
        if study not in genotype_paths:
            exon_missing.append(f"no vcf in study_genotype_paths.tsv for study_label={study}")
            continue
        vcf_ref = genotype_paths[study]
        exon_ss = str(exon_root / "sumstats_batches" / eid / "all")
        exon_row = row_line(
            dataset_id=eid,
            study_id=info["study_id"],
            quant="exon",
            qtl_group=group,
            study_name=study,
            vcf_file=vcf_ref,
            qtlmap_row=row,
            qroot=exon_root,
            exon_summ_stats=exon_ss,
            all_summ_stats=exon_ss,
            ge_count_for_scaling=qmap[triple[(study, group, "ge")]]["count_matrix"],
        )
        exon_rows.append(exon_row)
        validate_paths(
            [
                row["sample_meta"],
                vcf_ref,
                COV_BASE / study / f"{group}.parquet",
                row["count_matrix"],
                tpm_matrix_path("exon", row["count_matrix"], row),
                exon_ss,
                row["pheno_meta"],
                scaling_path(
                    qmap[triple[(study, group, "ge")]]["count_matrix"],
                    info["study_id"],
                    group,
                ),
                exon_root / "susie" / eid / f"{eid}.credible_sets.parquet",
            ],
            exon_missing,
            access_warn,
        )

    # --- other quants (tx, txrev, leafcutter, majiq) ---
    other_quants = ("tx", "txrev", "leafcutter", "majiq")
    out_by_q: dict[str, list[str]] = {q: [] for q in other_quants}
    miss_by_q: dict[str, list[str]] = {q: [] for q in other_quants}

    orphan_other = [oid for oid in sorted(other_ids) if oid not in by_id]
    for oid in sorted(other_ids):
        if oid not in by_id:
            continue
        info = by_id[oid]
        qm = info["quant_method"]
        if qm not in other_quants:
            continue
        row = qmap[oid]
        study, group = info["study_label"], info["sample_group"]
        if study not in genotype_paths:
            miss_by_q[qm].append(f"no vcf in study_genotype_paths.tsv for study_label={study}")
            continue
        vcf_ref = genotype_paths[study]
        try:
            eid = exon_id_for_group(triple, study, group)
        except KeyError:
            miss_by_q[qm].append(f"no exon mapping for {study} {group} ({oid})")
            continue
        exon_ss = str(exon_root / "sumstats_batches" / eid / "all")
        all_ss = str(other_root / "sumstats_batches" / oid / "all")
        ge_cm = qmap[triple[(study, group, "ge")]]["count_matrix"]
        o_row = row_line(
            dataset_id=oid,
            study_id=info["study_id"],
            quant=qm,
            qtl_group=group,
            study_name=study,
            vcf_file=vcf_ref,
            qtlmap_row=row,
            qroot=other_root,
            exon_summ_stats=exon_ss,
            all_summ_stats=all_ss,
            ge_count_for_scaling=ge_cm,
        )
        out_by_q[qm].append(o_row)
        validate_paths(
            [
                row["sample_meta"],
                vcf_ref,
                COV_BASE / study / f"{group}.parquet",
                row["count_matrix"],
                tpm_matrix_path(qm, row["count_matrix"], row),
                exon_ss,
                all_ss,
                row["pheno_meta"],
                scaling_path(ge_cm, info["study_id"], group),
                other_root / "susie" / oid / f"{oid}.credible_sets.parquet",
            ],
            miss_by_q[qm],
            access_warn,
        )

    # Report missing before any write
    all_missing = (
        [("ge", m) for m in ge_missing]
        + [("exon", m) for m in exon_missing]
        + [(q, m) for q in other_quants for m in miss_by_q[q]]
    )
    if orphan_other:
        print(
            "NOTE: n150_other susie folders not in dataset_id_map.tsv (skipped): "
            + ", ".join(orphan_other),
            file=sys.stderr,
        )
    if all_missing:
        print("MISSING PATHS (aborting)", file=sys.stderr)
        for tag, msg in all_missing[:80]:
            print(f"  [{tag}] {msg}", file=sys.stderr)
        if len(all_missing) > 80:
            print(f"  ... and {len(all_missing) - 80} more", file=sys.stderr)
        return 1
    if access_warn:
        uniq = sorted(set(access_warn))
        print("PRE-WRITE PATH WARNINGS (I/O during build):", file=sys.stderr)
        for w in uniq[:20]:
            print(f"  {w}", file=sys.stderr)
        if len(uniq) > 20:
            print(f"  ... and {len(uniq) - 20} more", file=sys.stderr)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written: list[tuple[str, int]] = []
    for name, rows in [
        ("n150_ge_input.tsv", ge_rows),
        ("n150_exon_input.tsv", exon_rows),
        ("n150_tx_input.tsv", out_by_q["tx"]),
        ("n150_txrev_input.tsv", out_by_q["txrev"]),
        ("n150_leafcutter_input.tsv", out_by_q["leafcutter"]),
        ("n150_majiq_input.tsv", out_by_q["majiq"]),
    ]:
        path = OUT_DIR / name
        with open(path, "w") as fh:
            fh.write(HEADER)
            fh.writelines(rows)
        written.append((name, len(rows)))

    print("Wrote:")
    for name, n in written:
        print(f"  {OUT_DIR / name}: {n} data rows")

    mode = "relaxed (missing only)" if relaxed else "strict (exist + readable)"
    print(f"Validating all path columns in written TSVs [{mode}]...", file=sys.stderr)
    all_miss: list[str] = []
    all_den: list[str] = []
    all_warn: list[str] = []
    for name, _ in written:
        m, d, w = validate_study_tsv(OUT_DIR / name, relaxed=relaxed)
        all_miss.extend(m)
        all_den.extend(d)
        all_warn.extend(w)
    if all_miss:
        print("VALIDATION FAILED — missing or empty paths:", file=sys.stderr)
        for line in all_miss[:60]:
            print(f"  {line}", file=sys.stderr)
        if len(all_miss) > 60:
            print(f"  ... and {len(all_miss) - 60} more", file=sys.stderr)
        return 1
    if all_den:
        print("VALIDATION FAILED — paths not readable:", file=sys.stderr)
        for line in all_den[:60]:
            print(f"  {line}", file=sys.stderr)
        if len(all_den) > 60:
            print(f"  ... and {len(all_den) - 60} more", file=sys.stderr)
        return 1
    if all_warn:
        print("Permission warnings (paths not verified readable here):", file=sys.stderr)
        uniq_w = sorted(set(all_warn))
        for line in uniq_w[:40]:
            print(f"  {line}", file=sys.stderr)
        if len(uniq_w) > 40:
            print(f"  ... and {len(uniq_w) - 40} more", file=sys.stderr)
    if relaxed:
        print(
            "RELAXED OK: no missing paths. Re-run without --relaxed on a host with full ACLs "
            "to require read access on every path.",
            file=sys.stderr,
        )
    else:
        print("All paths exist and are readable.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
