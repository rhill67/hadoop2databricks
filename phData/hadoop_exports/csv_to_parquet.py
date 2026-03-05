#!/usr/bin/env python3

"""
csv_to_parquet.py

Convert CSV files to Parquet format.

Supports:
  - Single CSV file
  - Directory of CSV files
  - Glob pattern

Examples:

Single file
python csv_to_parquet.py --input data/file.csv --output ./parquet/

Directory
python csv_to_parquet.py --input ./csv_files --output ./parquet/

Glob pattern
python csv_to_parquet.py --input "./csv_files/*.csv" --output ./parquet/
"""

import argparse
import pandas as pd
import glob
import os


def discover_csv_files(input_path):
    """Find CSV files from file, directory, or glob pattern"""

    if os.path.isdir(input_path):
        return glob.glob(os.path.join(input_path, "*.csv"))

    if "*" in input_path:
        return glob.glob(input_path)

    return [input_path]


def convert_csv_to_parquet(csv_file, output_dir):
    """Convert a single CSV file to Parquet"""

    filename = os.path.basename(csv_file).replace(".csv", ".parquet")
    parquet_path = os.path.join(output_dir, filename)

    print(f"Processing: {csv_file}")

    df = pd.read_csv(csv_file)

    df.to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False
    )

    print(f"Written: {parquet_path}")


def main():

    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet")

    parser.add_argument(
        "--input",
        required=True,
        help="CSV file, directory, or glob pattern"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for parquet files"
    )

    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    csv_files = discover_csv_files(args.input)

    print(f"Found {len(csv_files)} CSV file(s)")

    for csv_file in csv_files:
        convert_csv_to_parquet(csv_file, args.output)


if __name__ == "__main__":
    main()
