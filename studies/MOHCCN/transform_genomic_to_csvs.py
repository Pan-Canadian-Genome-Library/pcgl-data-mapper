#!/usr/bin/env python3
"""
Convert a structured experiments/analyses JSON file into three CSVs:

  - experiment.csv : one row per experiment, with metadata flattened into columns
  - analysis.csv   : one row per file (main + index) in each analysis; samples
                     are concatenated; analysis_id is suffixed with _main / _index
  - file.csv       : one row per file, linking the suffixed analysis_id to the
                     filename and access_method

Usage:
    python json_to_csv.py --input path/to/input.json [--output-dir DIR]

If --output-dir is omitted, CSVs are written to the current working directory.

Written by Claude
"""

import argparse
import csv
import json
import sys
from pathlib import Path

# Order in which file entries are pulled from each analysis. The key is the
# JSON field, the value is the suffix that disambiguates analysis_id.
FILE_ROLES = [("main", "_main"), ("index", "_index")]


def build_experiment_rows(experiments):
    """Return (fieldnames, rows) for experiment.csv.

    Top-level keys come first in a fixed order; metadata keys are appended in
    the order they're first encountered so the column layout is deterministic.
    """
    base_fields = ["program_id", "experiment_id", "submitter_sample_id"]

    metadata_fields = []
    seen_metadata = set()
    for exp in experiments:
        for key in exp.get("metadata", {}).keys():
            if key not in seen_metadata:
                seen_metadata.add(key)
                metadata_fields.append(key)

    fieldnames = base_fields + metadata_fields

    rows = []
    for exp in experiments:
        row = {field: exp.get(field, "") for field in base_fields}
        metadata = exp.get("metadata", {}) or {}
        for field in metadata_fields:
            row[field] = metadata.get(field, "")
        rows.append(row)

    return fieldnames, rows


def build_analysis_and_file_rows(analyses):
    """Return (analysis_fieldnames, analysis_rows, file_fieldnames, file_rows).

    For each analysis we emit one row per file (main, index). The analysis_id
    is suffixed so rows remain unique. Sample fields are joined with ';'.
    Metadata keys are flattened into columns just like the experiment table.
    """
    # Collect metadata keys in first-seen order for deterministic columns.
    metadata_fields = []
    seen_metadata = set()
    for analysis in analyses:
        for key in (analysis.get("metadata") or {}).keys():
            if key not in seen_metadata:
                seen_metadata.add(key)
                metadata_fields.append(key)

    analysis_fieldnames = (
            ["analysis_id", "program_id"]
            + metadata_fields
            + ["analysis_sample_ids", "experiment_ids"]
    )
    file_fieldnames = ["analysis_id", "name", "access_method"]

    analysis_rows = []
    file_rows = []

    for analysis in analyses:
        base_id = analysis.get("analysis_id", "")
        program_id = analysis.get("program_id", "")
        metadata = analysis.get("metadata") or {}
        samples = analysis.get("samples") or []

        analysis_sample_ids = ";".join(
            s.get("analysis_sample_id", "") for s in samples
        )
        experiment_ids = ";".join(s.get("experiment_id", "") for s in samples)

        for role_key, suffix in FILE_ROLES:
            file_entry = analysis.get(role_key) or {}
            suffixed_id = f"{base_id}{suffix}"

            analysis_row = {
                "analysis_id": suffixed_id,
                "program_id": program_id,
                "analysis_sample_ids": analysis_sample_ids,
                "experiment_ids": experiment_ids,
            }
            for field in metadata_fields:
                analysis_row[field] = metadata.get(field, "")
            analysis_rows.append(analysis_row)

            file_rows.append({
                "analysis_id": suffixed_id,
                "name": file_entry.get("name", ""),
                "access_method": file_entry.get("access_method", ""),
            })

    return analysis_fieldnames, analysis_rows, file_fieldnames, file_rows


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Convert experiments/analyses JSON into experiment.csv, "
                    "analysis.csv, and file.csv.",
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        type=Path,
        help="Path to the input JSON file.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Directory to write the CSV files into (default: current directory).",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    input_path = args.input
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    experiments = data.get("experiments", [])
    analyses = data.get("analyses", [])

    exp_fields, exp_rows = build_experiment_rows(experiments)
    ana_fields, ana_rows, file_fields, file_rows = build_analysis_and_file_rows(
        analyses
    )

    write_csv(output_dir / "experiment.csv", exp_fields, exp_rows)
    write_csv(output_dir / "analysis.csv", ana_fields, ana_rows)
    write_csv(output_dir / "file.csv", file_fields, file_rows)

    print(f"Wrote {len(exp_rows)} rows to {output_dir / 'experiment.csv'}")
    print(f"Wrote {len(ana_rows)} rows to {output_dir / 'analysis.csv'}")
    print(f"Wrote {len(file_rows)} rows to {output_dir / 'file.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
