#!/usr/bin/env python3

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recursively list QTS*.sqlite files and write absolute paths."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory to recursively scan for QTS*.sqlite files.",
    )
    parser.add_argument(
        "--output-list",
        required=True,
        help="Output text file path (one absolute SQLite path per line).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_list = Path(args.output_list).resolve()

    if not input_dir.exists() or not input_dir.is_dir():
        raise ValueError(f"Input directory does not exist or is not a directory: {input_dir}")

    matched = sorted(
        path.resolve()
        for path in input_dir.rglob("QTS*.sqlite")
        if path.is_file()
    )

    output_list.parent.mkdir(parents=True, exist_ok=True)
    with output_list.open("w", encoding="utf-8") as handle:
        for path in matched:
            handle.write(f"{path}\n")

    print(f"Input directory: {input_dir}")
    print(f"Matched files: {len(matched)}")
    print(f"Wrote list file: {output_list}")


if __name__ == "__main__":
    main()
