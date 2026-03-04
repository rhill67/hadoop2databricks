#!/usr/bin/env python3


### Date : 03-04-2026 
### Auth : roger hill 
### Desc : See below 'Purpose' 

"""
flatten_missions.py

Purpose:
  - Read one or many mission JSON files
  - Flatten the nested "people" array into a tabular output (CSV)
  - Output columns:
      mission_identifier, startDate, endDate, institution, role, firstName, lastName

Key behavior:
  - "roles" is an array; this script EXPLODES roles:
      If a person has 2 roles, you get 2 rows for that person (one per role).
  - Missing fields are handled safely as empty strings ("")

Examples:
  python3 flatten_missions.py --input mission_data.json --output flattened.csv
  python3 flatten_missions.py --input ./data --output flattened.csv
  python3 flatten_missions.py --input "./data/*.json" --output flattened.csv
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
    """Configure logging level and format."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s"
    )


# -----------------------------
# Input discovery
# -----------------------------
def discover_json_files(input_path: str) -> List[str]:
    """
    Discover JSON files from:
      - a single file path
      - a directory path (loads *.json inside)
      - a glob pattern (e.g., ./data/*.json)

    Returns a sorted list of file paths.
    """
    # If it's a directory, take all *.json files in it
    if os.path.isdir(input_path):
        files = glob.glob(os.path.join(input_path, "*.json"))
        return sorted(files)

    # If it contains wildcard characters, treat as a glob
    if any(ch in input_path for ch in ["*", "?", "["]):
        return sorted(glob.glob(input_path))

    # Otherwise assume it's a single file path
    return [input_path]


# -----------------------------
# Core flattening logic
# -----------------------------
def safe_get_str(obj: Dict[str, Any], key: str) -> str:
    """Return obj[key] if present and not None; otherwise return empty string."""
    val = obj.get(key, "")
    return "" if val is None else str(val)


def flatten_one_mission_json(data: Dict[str, Any], source_file: str = "") -> List[Dict[str, str]]:
    """
    Convert one mission JSON document into a list of flat rows.

    Each row includes mission-level fields + one person + one role.

    Returns:
      List[Dict[str, str]] where keys match the CSV columns.
    """
    rows: List[Dict[str, str]] = []

    # Mission-level fields (top-level keys)
    mission_identifier = safe_get_str(data, "identifier")
    start_date = safe_get_str(data, "startDate")
    end_date = safe_get_str(data, "endDate")

    # People entries live in a top-level "people" array
    people = data.get("people", [])
    if not isinstance(people, list):
        logging.warning("File %s: expected 'people' to be a list; got %s", source_file, type(people).__name__)
        people = []

    for person_entry in people:
        # Each entry should be a dict with "institution", "roles", and "person"
        if not isinstance(person_entry, dict):
            continue

        institution = safe_get_str(person_entry, "institution")

        # "roles" is an array; we explode it into multiple rows (one per role)
        roles = person_entry.get("roles", [])
        if roles is None:
            roles = []
        if not isinstance(roles, list):
            roles = [str(roles)]

        # "person" is nested under "person"
        person_obj = person_entry.get("person", {})
        if not isinstance(person_obj, dict):
            person_obj = {}

        first_name = safe_get_str(person_obj, "firstName")
        last_name = safe_get_str(person_obj, "lastName")

        # If no roles are provided, still emit one row with empty role
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


def load_json_file(path: str) -> Dict[str, Any]:
    """
    Load a JSON file. Raises exceptions to be handled by the caller
    so we can continue processing other files.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# CSV output + console preview
# -----------------------------
CSV_COLUMNS = [
    "mission_identifier",
    "startDate",
    "endDate",
    "institution",
    "role",
    "firstName",
    "lastName",
]


def write_csv(rows: Iterable[Dict[str, str]], output_csv: str) -> None:
    """Write flat rows to CSV with a consistent column order."""
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def print_preview(rows: List[Dict[str, str]], limit: int) -> None:
    """
    Print a simple CSV-like preview to stdout so you can eyeball results quickly.
    """
    if limit <= 0:
        return

    print(",".join(CSV_COLUMNS))
    for row in rows[:limit]:
        print(",".join(row.get(col, "") for col in CSV_COLUMNS))


# -----------------------------
# Main program
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Flatten mission JSON files into a CSV report.")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to a JSON file, a directory containing JSON files, or a glob pattern like './data/*.json'"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV file path, e.g. flattened.csv"
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=10,
        help="How many flattened rows to print as a preview (default: 10). Use 0 to disable."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging."
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    files = discover_json_files(args.input)
    if not files:
        logging.error("No JSON files found for input: %s", args.input)
        return 2

    logging.info("Found %d JSON file(s) to process.", len(files))

    all_rows: List[Dict[str, str]] = []

    # Process each file independently so a single bad file doesn't kill the run
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

    # Write output CSV
    write_csv(all_rows, args.output)
    logging.info("Wrote %d total row(s) to %s", len(all_rows), args.output)

    # Print preview
    if args.preview > 0:
        print_preview(all_rows, args.preview)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
