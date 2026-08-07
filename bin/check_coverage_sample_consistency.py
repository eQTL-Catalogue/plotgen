#!/usr/bin/env python3
"""Check that coverage Parquet files contain all QC-passing metadata samples."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pyarrow.parquet as pq


DEFAULT_STUDIES = ("Aygun_2021", "Steinberg_2020", "ROSMAP")
TRUE_VALUES = {"1", "true", "t", "yes", "y"}
REQUIRED_INPUT_COLUMNS = {
    "study_name",
    "qtl_group",
    "sample_meta",
    "coverage_path",
}
REQUIRED_METADATA_COLUMNS = {
    "sample_id",
    "qtl_group",
    "rna_qc_passed",
    "genotype_qc_passed",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare QC-passing sample_id values in study metadata against "
            "sample_id values present in each coverage Parquet file."
        )
    )
    parser.add_argument(
        "-i",
        "--input-tsv",
        default="input/batch_040626/batch_040626_ge_input.tsv",
        help="Pipeline input TSV with study_name, qtl_group, sample_meta, and coverage_path columns.",
    )
    parser.add_argument(
        "--studies",
        nargs="+",
        default=list(DEFAULT_STUDIES),
        help="Study names to check. Defaults to Aygun_2021 Steinberg_2020 ROSMAP.",
    )
    parser.add_argument(
        "--row-groups",
        default="0",
        help=(
            "Comma-separated Parquet row groups to inspect for coverage sample IDs. "
            "Use integers, 'first', 'last', or 'all'. Default: 0."
        ),
    )
    parser.add_argument(
        "-o",
        "--output-tsv",
        help="Optional path to write the tab-separated report.",
    )
    parser.add_argument(
        "--allow-mismatches",
        action="store_true",
        help="Always exit 0, even if metadata samples are missing from coverage.",
    )
    parser.add_argument(
        "--show-samples",
        type=int,
        default=50,
        help="Maximum missing/extra sample IDs to include per row in the report.",
    )
    return parser.parse_args()


def read_input_rows(input_tsv: Path, studies: set[str]) -> list[dict[str, str]]:
    with input_tsv.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing = REQUIRED_INPUT_COLUMNS.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{input_tsv} is missing required columns: {', '.join(sorted(missing))}"
            )

        seen: set[tuple[str, str, str, str]] = set()
        rows: list[dict[str, str]] = []
        for row in reader:
            study_name = row["study_name"].strip()
            if study_name not in studies:
                continue
            key = (
                study_name,
                row["qtl_group"].strip(),
                row["sample_meta"].strip(),
                row["coverage_path"].strip(),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

    return rows


def is_true(value: str) -> bool:
    return str(value).strip().lower() in TRUE_VALUES


def metadata_samples(sample_meta: Path, qtl_group: str) -> list[str]:
    with sample_meta.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing = REQUIRED_METADATA_COLUMNS.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{sample_meta} is missing required columns: {', '.join(sorted(missing))}"
            )

        samples = [
            row["sample_id"].strip()
            for row in reader
            if row["qtl_group"].strip() == qtl_group
            and is_true(row["rna_qc_passed"])
            and is_true(row["genotype_qc_passed"])
        ]

    return samples


def parse_row_groups(spec: str, num_row_groups: int) -> list[int]:
    spec = spec.strip().lower()
    if spec == "all":
        return list(range(num_row_groups))

    row_groups: set[int] = set()
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        if token == "first":
            row_groups.add(0)
        elif token == "last":
            row_groups.add(num_row_groups - 1)
        else:
            row_group = int(token)
            if row_group < 0:
                row_group = num_row_groups + row_group
            if row_group < 0 or row_group >= num_row_groups:
                raise ValueError(
                    f"row group {token!r} is out of range for file with {num_row_groups} row groups"
                )
            row_groups.add(row_group)

    if not row_groups:
        raise ValueError("No row groups selected")
    return sorted(row_groups)


def coverage_samples(coverage_path: Path, row_group_spec: str) -> tuple[set[str], dict[str, int]]:
    parquet_file = pq.ParquetFile(coverage_path)
    if "sample_id" not in parquet_file.schema.names:
        raise ValueError(f"{coverage_path} does not have a sample_id column")

    selected_row_groups = parse_row_groups(row_group_spec, parquet_file.num_row_groups)
    samples: set[str] = set()
    for row_group in selected_row_groups:
        table = parquet_file.read_row_group(row_group, columns=["sample_id"])
        samples.update(value for value in table.column("sample_id").to_pylist() if value)

    info = {
        "num_rows": parquet_file.metadata.num_rows,
        "num_row_groups": parquet_file.num_row_groups,
        "row_groups_checked": len(selected_row_groups),
    }
    return samples, info


def compact_sample_list(samples: list[str], limit: int) -> str:
    if limit <= 0:
        return ""
    shown = samples[:limit]
    suffix = "" if len(samples) <= limit else f";...+{len(samples) - limit}"
    return ",".join(shown) + suffix


def build_report(args: argparse.Namespace) -> tuple[list[dict[str, str]], bool]:
    input_tsv = Path(args.input_tsv)
    rows = read_input_rows(input_tsv, set(args.studies))
    if not rows:
        raise ValueError(
            f"No rows found in {input_tsv} for studies: {', '.join(args.studies)}"
        )

    report_rows: list[dict[str, str]] = []
    has_missing = False
    for row in rows:
        study_name = row["study_name"].strip()
        qtl_group = row["qtl_group"].strip()
        sample_meta = Path(row["sample_meta"].strip())
        coverage_path = Path(row["coverage_path"].strip())

        metadata_sample_ids = metadata_samples(sample_meta, qtl_group)
        coverage_sample_ids, coverage_info = coverage_samples(coverage_path, args.row_groups)

        missing_in_coverage = sorted(set(metadata_sample_ids) - coverage_sample_ids)
        extra_in_coverage = sorted(coverage_sample_ids - set(metadata_sample_ids))
        has_missing = has_missing or bool(missing_in_coverage)

        report_rows.append(
            {
                "status": "FAIL" if missing_in_coverage else "PASS",
                "study_name": study_name,
                "qtl_group": qtl_group,
                "metadata_samples": str(len(set(metadata_sample_ids))),
                "coverage_samples_observed": str(len(coverage_sample_ids)),
                "missing_in_coverage_count": str(len(missing_in_coverage)),
                "extra_in_coverage_count": str(len(extra_in_coverage)),
                "coverage_num_rows": str(coverage_info["num_rows"]),
                "coverage_num_row_groups": str(coverage_info["num_row_groups"]),
                "row_groups_checked": str(coverage_info["row_groups_checked"]),
                "missing_in_coverage": compact_sample_list(
                    missing_in_coverage, args.show_samples
                ),
                "extra_in_coverage": compact_sample_list(extra_in_coverage, args.show_samples),
                "sample_meta": str(sample_meta),
                "coverage_path": str(coverage_path),
            }
        )

    return report_rows, has_missing


def write_report(rows: list[dict[str, str]], output_path: str | None) -> None:
    fieldnames = list(rows[0].keys())
    if output_path:
        handle = open(output_path, "w", newline="")
        close_handle = True
    else:
        handle = sys.stdout
        close_handle = False

    try:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if close_handle:
            handle.close()


def main() -> int:
    args = parse_args()
    report_rows, has_missing = build_report(args)
    write_report(report_rows, args.output_tsv)

    if has_missing and not args.allow_mismatches:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
