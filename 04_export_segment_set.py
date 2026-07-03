#!/usr/bin/env python3
"""Stage 4: Export final deduplicated segment set as CSV.

Reads runs_enriched.json from Stage 3, deduplicates by run_uuid, sorts by
log_collected_at, and writes a clean CSV. Prints a summary to stderr.

Usage:
    python 04_export_segment_set.py --input output/runs_enriched.json
    python 04_export_segment_set.py --input output/runs_enriched.json --output custom.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_OUTPUT = OUTPUT_DIR / "segment_set.csv"

# Column order for the CSV
COLUMNS = [
    "run_uuid",
    "custom_id",
    "vehicle_name",
    "log_collected_at",
    "duration_min",
    "rep_lat",
    "rep_lon",
    "gps_points_in_geofence",
    "map_key",
    "s3_path",
    "in_odd",
    "odd_area_name",
    "autonomous_time_in_odd_s",
    "gps_min_lat",
    "gps_max_lat",
    "gps_min_lon",
    "gps_max_lon",
]


def export_segment_set(runs: list[dict], output_path: Path) -> int:
    """Write runs to CSV. Returns number of rows written."""
    # Deduplicate by run_uuid (keep first occurrence)
    seen = set()
    unique = []
    for r in runs:
        uid = r["run_uuid"]
        if uid not in seen:
            seen.add(uid)
            unique.append(r)

    # Sort by log_collected_at
    unique.sort(key=lambda r: r.get("log_collected_at", ""))

    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in unique:
            # Round floats for readability
            row = {}
            for col in COLUMNS:
                val = r.get(col)
                if isinstance(val, float):
                    row[col] = round(val, 6)
                else:
                    row[col] = val
            writer.writerow(row)

    return len(unique)


def print_summary(runs: list[dict], n_written: int) -> None:
    """Print a summary of the segment set to stderr."""
    print("\nSegment Set Export Summary", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print(f"Total segments:        {n_written}", file=sys.stderr)

    # Unique vehicles
    vehicles = sorted(set(r.get("vehicle_name", "") for r in runs))
    print(f"Unique vehicles:       {len(vehicles)} ({', '.join(vehicles[:5])}"
          f"{'...' if len(vehicles) > 5 else ''})", file=sys.stderr)

    # Date range
    dates = [r.get("log_collected_at", "") for r in runs if r.get("log_collected_at")]
    if dates:
        print(f"Date range:            {min(dates)[:10]} to {max(dates)[:10]}",
              file=sys.stderr)

    # ODD coverage
    in_odd = sum(1 for r in runs if r.get("in_odd"))
    total = len(runs)
    pct = (100.0 * in_odd / total) if total > 0 else 0
    print(f"In-ODD:                {in_odd}/{total} ({pct:.0f}%)", file=sys.stderr)

    # ODD areas
    odd_areas = Counter(r.get("odd_area_name", "unknown") for r in runs)
    area_str = ", ".join(f"{k} ({v})" for k, v in odd_areas.most_common())
    print(f"ODD areas:             {area_str}", file=sys.stderr)

    # Map keys
    map_keys = Counter(r.get("map_key", "unknown") for r in runs)
    mk_str = ", ".join(f"{k} ({v})" for k, v in map_keys.most_common())
    print(f"Map keys:              {mk_str}", file=sys.stderr)

    # Total autonomous time
    total_odd_time = sum(r.get("autonomous_time_in_odd_s", 0) or 0 for r in runs)
    print(f"Total autonomous time: {total_odd_time / 3600:.1f} hours in-ODD",
          file=sys.stderr)

    # Total duration
    total_dur = sum(r.get("duration_min", 0) or 0 for r in runs)
    print(f"Total duration:        {total_dur:.1f} min ({total_dur / 60:.1f} hrs)",
          file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Stage 4: Export final deduplicated segment set as CSV."
    )
    parser.add_argument(
        "--input", default=str(OUTPUT_DIR / "runs_enriched.json"),
        help="Input JSON from Stage 3"
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})"
    )
    args = parser.parse_args()

    # Load runs
    with open(args.input) as f:
        runs = json.load(f)
    print(f"Loaded {len(runs)} runs from {args.input}", file=sys.stderr)

    # Export
    n_written = export_segment_set(runs, Path(args.output))

    # Summary
    print_summary(runs, n_written)
    print(f"\nWrote: {args.output} ({n_written} rows, {len(COLUMNS)} columns)",
          file=sys.stderr)


if __name__ == "__main__":
    main()
