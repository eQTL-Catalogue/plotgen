#!/usr/bin/env python3

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Sequence


NON_ID_COLUMNS: Sequence[str] = (
    "study_id",
    "study_label",
    "dataset_id",
    "molecular_trait_id",
    "gene_id",
    "gene_name",
    "variant",
    "rsid",
    "quantification_method",
    "credible_set",
    "credible_set_size",
    "pip",
    "pvalue",
    "beta",
    "se",
    "dataset_label",
    "plot_variant",
)


def log_progress(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [dup_extract] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract duplicate payload rows from each input SQLite into separate output SQLite files. "
            "For each duplicated payload group, output exactly two rows (lowest two ids)."
        )
    )
    parser.add_argument(
        "--input-list",
        required=True,
        help="Text file with one absolute SQLite path per line.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where per-input duplicate SQLite files will be written.",
    )
    return parser.parse_args()


def read_sqlite_paths(list_file: Path) -> List[Path]:
    if not list_file.exists():
        raise ValueError(f"Input list file does not exist: {list_file}")

    paths: List[Path] = []
    with list_file.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            path = Path(line).resolve()
            if not path.is_absolute():
                raise ValueError(f"Expected absolute path in input list, got: {line}")
            if not path.exists():
                raise ValueError(f"Input SQLite file does not exist: {path}")
            paths.append(path)

    if not paths:
        raise ValueError(f"Input list file has no SQLite paths: {list_file}")
    return paths


def output_name_for(source_path: Path, index: int) -> str:
    parent_name = source_path.parent.name
    grandparent_name = source_path.parent.parent.name if source_path.parent.parent else "root"
    return f"{index:02d}_{grandparent_name}__{parent_name}__duplicates.sqlite"


def extract_duplicates_for_file(source_db: Path, output_db: Path) -> int:
    log_progress(f"Preparing output DB: {output_db}")
    if output_db.exists():
        output_db.unlink()

    log_progress(f"Connecting to source DB: {source_db}")
    conn = sqlite3.connect(str(source_db))
    outdb_attached = False
    try:
        log_progress("Attaching output database")
        conn.execute("ATTACH DATABASE ? AS outdb;", (str(output_db),))
        outdb_attached = True
        log_progress("Creating output table schema")
        conn.execute(
            """
            CREATE TABLE outdb.credible_set_table (
                id INTEGER PRIMARY KEY NOT NULL,
                study_id TEXT,
                study_label TEXT,
                dataset_id TEXT,
                molecular_trait_id TEXT,
                gene_id TEXT,
                gene_name TEXT,
                variant TEXT,
                rsid TEXT,
                quantification_method TEXT,
                credible_set TEXT,
                credible_set_size INTEGER,
                pip FLOAT,
                pvalue FLOAT,
                beta FLOAT,
                se FLOAT,
                dataset_label TEXT,
                plot_variant TEXT
            );
            """
        )

        partition_cols = ", ".join(NON_ID_COLUMNS)
        log_progress("Running duplicate extraction query (this can take a while)")
        conn.execute(
            f"""
            INSERT INTO outdb.credible_set_table (
                id,
                study_id,
                study_label,
                dataset_id,
                molecular_trait_id,
                gene_id,
                gene_name,
                variant,
                rsid,
                quantification_method,
                credible_set,
                credible_set_size,
                pip,
                pvalue,
                beta,
                se,
                dataset_label,
                plot_variant
            )
            SELECT
                id,
                study_id,
                study_label,
                dataset_id,
                molecular_trait_id,
                gene_id,
                gene_name,
                variant,
                rsid,
                quantification_method,
                credible_set,
                credible_set_size,
                pip,
                pvalue,
                beta,
                se,
                dataset_label,
                plot_variant
            FROM (
                SELECT
                    id,
                    study_id,
                    study_label,
                    dataset_id,
                    molecular_trait_id,
                    gene_id,
                    gene_name,
                    variant,
                    rsid,
                    quantification_method,
                    credible_set,
                    credible_set_size,
                    pip,
                    pvalue,
                    beta,
                    se,
                    dataset_label,
                    plot_variant,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_cols}
                        ORDER BY id
                    ) AS rn,
                    COUNT(*) OVER (
                        PARTITION BY {partition_cols}
                    ) AS grp_count
                FROM credible_set_table
            ) ranked
            WHERE grp_count > 1
              AND rn <= 2
            ORDER BY id;
            """
        )

        log_progress("Counting extracted rows")
        row_count = conn.execute("SELECT COUNT(*) FROM outdb.credible_set_table;").fetchone()[0]
        log_progress(f"Extracted rows: {row_count}")
        log_progress("Committing transaction before detach")
        conn.commit()
        log_progress("Detaching output database")
        conn.execute("DETACH DATABASE outdb;")
        outdb_attached = False
        return int(row_count)
    finally:
        if outdb_attached:
            try:
                conn.commit()
                conn.execute("DETACH DATABASE outdb;")
            except sqlite3.Error:
                pass
        log_progress("Closing source database connection")
        conn.close()


def main() -> None:
    log_progress("Starting duplicate-row extraction")
    args = parse_args()
    input_list = Path(args.input_list).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    log_progress(f"Reading input list: {input_list}")
    input_paths = read_sqlite_paths(input_list)
    log_progress(f"Input files: {len(input_paths)}")
    log_progress(f"Output directory: {output_dir}")

    total_rows = 0
    for i, source_db in enumerate(input_paths, start=1):
        output_db = output_dir / output_name_for(source_db, i)
        log_progress(f"[{i}/{len(input_paths)}] Processing source: {source_db}")
        rows = extract_duplicates_for_file(source_db, output_db)
        total_rows += rows
        log_progress(f"[{i}/{len(input_paths)}] Wrote: {output_db} (rows={rows})")

    log_progress(f"Completed. Total duplicate rows written across outputs: {total_rows}")


if __name__ == "__main__":
    main()
