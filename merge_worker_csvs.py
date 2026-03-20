#!/usr/bin/env python3
"""Merge per-worker CSVs into single files, deduplicating rows."""
import os
import glob
import csv

OUTPUT_DIR = "lake_survey_data"

DATASETS = [
    "all_counties_lakes",
    "all_counties_catch_summaries",
    "all_counties_length_distributions",
]

for base in DATASETS:
    pattern = os.path.join(OUTPUT_DIR, f"{base}_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No worker files found for {base}, skipping.")
        continue

    seen = set()
    rows = []
    header = None

    for f in files:
        with open(f, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            file_header = next(reader)
            if header is None:
                header = file_header
            for row in reader:
                key = tuple(row)
                if key not in seen:
                    seen.add(key)
                    rows.append(row)

    out = os.path.join(OUTPUT_DIR, f"{base}.csv")
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"{base}.csv: {len(rows):,} rows (from {len(files)} workers)")
