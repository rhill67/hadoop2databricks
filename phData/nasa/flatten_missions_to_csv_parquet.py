#!/usr/bin/env python3

### Date : 03-04-2026
### Auth : roger hill
### Desc : See below 'Purpose'

"""
flatten_missions_to_csv_parquet.py

Reads one or many mission JSON files, flattens nested people/roles, and outputs:
  - CSV (always)
  - Parquet (optional, requires pyarrow or fastparquet)

Flattened columns:
  mission_identifier, startDate, endDate, institution, role, firstName, lastName

Behavior:
  - Explodes roles[] => 1 row per (person, role). If roles is empty, emits 1 row with role="".
  - Handles missing fields as empty strings.
"""

import argparse
import csv
import glob
import json
import logging
import os
from typing import Dict, Any, Iterable, List


# -----------------------------
# Logging setup
# -----------------------------
def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


# -----------------------------
# Input discovery
# -----------------------------
def discover_json_files(input_path: str) -> List[str]:
    """
    Accepts:
      - single file path
      - directory path (loads *.json inside)
      - glob pattern (e.g., ./data/*.json)
    """
    if os.path.isdir(input_path):
        return sorted(glob.glob(os.path.join(input_path, "*.json")))

    if any(ch in input_path for ch in ["*", "?", "["]):
        return sorted(glob.glob(input_path))

    return [input_path]


# -----------------------------
# Core flattening logic
# -----------------------------
def safe_get_str(obj: Dict[str, Any], key: str) -> str:
    val = obj.get(key, "")
    return "" if val is None else str(val)


def load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


CSV_COLUMNS = [
    "mission_identifier",
    "startDate",
    "endDate",
    "institution",
    "role",
    "firstName",
    "lastName",
]


def flatten_one_mission_json(data: Dict[str, Any], source_file: str = "") -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    # Mission-level fields
    mission_identifier = safe_get_str(data, "identifier")
    start_date = safe_get_str(data, "startDate")
    end_date = safe_get_str(data, "endDate")

    people = data.get("people", [])
    if not isinstance(people, list):
        logging.warning("File %s: expected 'people' list; got %s", source_file, type(people).__name__)
        people = []

    for person_entry in people:
        if not isinstance(person_entry, dict):
            continue

        institution = safe_get_str(person_entry, "institution")

        roles = person_entry.get("roles", [])
        if roles is None:
            roles = []
        if not isinstance(roles, list):
            roles = [str(roles)]

        person_obj = person_entry.get("person", {})
        if not isinstance(person_obj, dict):
            person_obj = {}

        first_name = safe_get_str(person_obj, "firstName")
        last_name = safe_get_str(person_obj, "lastName")

        # Emit one row even if roles is empty
        if not roles:
            rows.append({
                "mission_identifier": mission_identifier,
                "startDate": start_date,
                "endDate": end_date,
                "institution": institution,
                "role": "",
                "firstName": first_name,
                "lastName": last_name,
            })
        else:
            for role in roles:
                rows.append({
                    "mission_identifier": mission_identifier,
                    "startDate": start_date,
                    "endDate": end_date,
                    "institution": institution,
                    "role": "" if role is None else str(role),
                    "firstName": first_name,
                    "lastName": last_name,
                })

    return rows


# -----------------------------
# Outputs
# -----------------------------
def write_csv(rows: Iterable[Dict[str, str]], output_csv: str) -> None:
    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_parquet(rows: List[Dict[str, str]], output_parquet: str) -> None:
    """
    Writes Parquet using pandas + (pyarrow OR fastparquet).
    Databricks reads this natively (Spark can read parquet directly).

    Requires:
      pip install pandas pyarrow
        OR
      pip install pandas fastparquet
    """
    try:
        import pandas as pd
    except ImportError as e:
        raise RuntimeError("pandas is required for Parquet output. Install with: pip install pandas") from e

    # Build DataFrame with stable column ordering
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)

    os.makedirs(os.path.dirname(output_parquet) or ".", exist_ok=True)

    # Let pandas choose the available engine. If none installed, it will error with guidance.
    try:
        df.to_parquet(output_parquet, index=False)
    except Exception as e:
        raise RuntimeError(
            "Failed to write Parquet. Install a Parquet engine:\n"
            "  pip install pyarrow   (recommended)\n"
            "or\n"
            "  pip install fastparquet\n"
            f"Original error: {e}"
        ) from e


def print_preview(rows: List[Dict[str, str]], limit: int) -> None:
    if limit <= 0:
        return
    print(",".join(CSV_COLUMNS))
    for row in rows[:limit]:
        print(",".join(row.get(col, "") for col in CSV_COLUMNS))


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Flatten mission JSON to CSV and Parquet.")
    parser.add_argument("--input", required=True, help="JSON file, directory, or glob (e.g. ./data/*.json)")
    parser.add_argument("--output-csv", required=True, help="Output CSV path (e.g. out/flattened.csv)")
    parser.add_argument("--output-parquet", required=False, help="Output Parquet path (e.g. out/flattened.parquet)")
    parser.add_argument("--preview", type=int, default=10, help="Print first N rows (default 10, 0 disables)")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args()
    setup_logging(args.verbose)

    files = discover_json_files(args.input)
    if not files:
        logging.error("No JSON files found for input: %s", args.input)
        return 2

    logging.info("Found %d JSON file(s).", len(files))

    all_rows: List[Dict[str, str]] = []

    for path in files:
        try:
            data = load_json_file(path)
            rows = flatten_one_mission_json(data, source_file=path)
            all_rows.extend(rows)
            logging.info("Processed %s -> %d row(s)", path, len(rows))
        except json.JSONDecodeError as e:
            logging.error("Invalid JSON in %s: %s", path, e)
        except FileNotFoundError:
            logging.error("File not found: %s", path)
        except Exception as e:
            logging.exception("Unexpected error processing %s: %s", path, e)

    # Always write CSV
    write_csv(all_rows, args.output_csv)
    logging.info("Wrote CSV: %s (%d total row(s))", args.output_csv, len(all_rows))

    # Optionally write Parquet
    if args.output_parquet:
        write_parquet(all_rows, args.output_parquet)
        logging.info("Wrote Parquet: %s", args.output_parquet)

    # Console preview
    if args.preview > 0:
        print_preview(all_rows, args.preview)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
