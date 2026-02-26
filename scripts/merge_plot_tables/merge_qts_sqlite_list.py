#!/usr/bin/env python3

import argparse
import sqlite3
from pathlib import Path
from typing import List, Sequence, Tuple

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge SQLite files listed in a text file into one deduplicated SQLite DB."
    )
    parser.add_argument(
        "--input-list",
        required=True,
        help="Text file with one absolute SQLite path per line.",
    )
    parser.add_argument(
        "--output-sqlite",
        required=True,
        help="Path to merged output SQLite file.",
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


def create_output_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE credible_set_table (
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
    # Deduplicate exact rows (all non-id columns) while merging.
    dedup_expr = ", ".join(
        f"ifnull(CAST({column} AS TEXT), '__NULL__')" for column in NON_ID_COLUMNS
    )
    conn.execute(
        f"CREATE UNIQUE INDEX uq_full_row_tmp ON credible_set_table({dedup_expr});"
    )


def insert_new_rows(conn: sqlite3.Connection, alias: str) -> None:
    conn.execute(
        f"""
        INSERT OR IGNORE INTO credible_set_table (
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
        FROM {alias}.credible_set_table
        ORDER BY id;
        """
    )


def merge_inputs(conn: sqlite3.Connection, input_paths: Sequence[Path]) -> List[Tuple[Path, int]]:
    per_file_counts: List[Tuple[Path, int]] = []
    for source_order, source_path in enumerate(input_paths):
        alias = f"src_{source_order}"
        conn.execute(f"ATTACH DATABASE ? AS {alias};", (str(source_path),))
        try:
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM {alias}.credible_set_table;"
            ).fetchone()[0]
            per_file_counts.append((source_path, int(row_count)))
            insert_new_rows(conn, alias)
            conn.commit()
        finally:
            conn.execute(f"DETACH DATABASE {alias};")
    return per_file_counts


def create_final_indexes(conn: sqlite3.Connection) -> None:
    conn.execute("DROP INDEX uq_full_row_tmp;")
    indexes = [
        "CREATE INDEX idx_molecular_trait_id ON credible_set_table(molecular_trait_id);",
        "CREATE INDEX idx_gene_name ON credible_set_table(gene_name);",
        "CREATE INDEX idx_credible_set ON credible_set_table(credible_set);",
        "CREATE INDEX idx_variant ON credible_set_table(variant);",
        "CREATE INDEX idx_rsid ON credible_set_table(rsid);",
        "CREATE INDEX idx_dataset ON credible_set_table(study_label);",
        "CREATE INDEX idx_for_plotting ON credible_set_table(dataset_id, gene_id, molecular_trait_id, variant);",
    ]
    for statement in indexes:
        conn.execute(statement)


def main() -> None:
    args = parse_args()
    input_list = Path(args.input_list).resolve()
    output_sqlite = Path(args.output_sqlite).resolve()

    input_paths = read_sqlite_paths(input_list)
    output_sqlite.parent.mkdir(parents=True, exist_ok=True)
    if output_sqlite.exists():
        output_sqlite.unlink()

    conn = sqlite3.connect(str(output_sqlite))
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        create_output_table(conn)
        per_file_counts = merge_inputs(conn, input_paths)
        create_final_indexes(conn)
        merged_rows = conn.execute("SELECT COUNT(*) FROM credible_set_table;").fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    total_input_rows = sum(count for _, count in per_file_counts)
    duplicate_rows_removed = total_input_rows - merged_rows
    print(f"Merged output: {output_sqlite}")
    print(f"Input files: {len(per_file_counts)}")
    print(f"Total input rows: {total_input_rows}")
    print(f"Merged rows: {merged_rows}")
    print(f"Duplicate rows removed: {duplicate_rows_removed}")


if __name__ == "__main__":
    main()
