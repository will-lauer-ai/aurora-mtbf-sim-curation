#!/usr/bin/env python3
"""Stage 1: Discover candidate runs by date range and optional vehicle filter.

Queries the Ursa operational tables (Postgres, freshest source) for
converted runs within a date range. Outputs a JSON list of run dicts.

Usage:
    python 01_discover_runs.py --start-date 2026-06-01 --end-date 2026-06-30
    python 01_discover_runs.py --start-date 2026-06-01 --end-date 2026-06-30 --vehicle truck-806
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import lake_client

OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_OUTPUT = OUTPUT_DIR / "runs_raw.json"


def discover_runs(
    start_date: str,
    end_date: str,
    vehicle: str | None = None,
) -> list[dict]:
    """Query the data lake for converted runs in a date range.

    Args:
        start_date: YYYY-MM-DD (inclusive)
        end_date: YYYY-MM-DD (exclusive — end of that day)
        vehicle: Optional vehicle name (hyphen form, e.g. "truck-806")

    Returns:
        List of run dicts with keys: run_uuid, custom_id, vehicle_name,
        log_collected_at, duration_s, duration_min, data_explorer_uuid,
        raw_data_uri, map_key, route, stack_commit
    """
    # Build the vehicle filter (Postgres ops tables use hyphens)
    vehicle_clause = ""
    if vehicle:
        vehicle_clause = f"\n  AND d.vehicle_name = '{vehicle}'"

    sql = f"""
SELECT
  CAST(d.uuid AS varchar) AS run_uuid,
  d.custom_id,
  d.vehicle_name,
  d.log_collected_at,
  d.duration_s,
  ROUND(d.duration_s / 60.0, 1) AS duration_min,
  d.data_explorer_uuid,
  d.raw_data_uri,
  json_extract_scalar(d.custom_metadata, '$.map_key') AS map_key,
  json_extract_scalar(d.custom_metadata, '$.route') AS route,
  json_extract_scalar(d.custom_metadata, '$.stack_commit') AS stack_commit
FROM ursa_log_management.public.duration_in_seconds_view d
JOIN ursa_log_management.public.log_conversions lc
  ON lc.source_run_uuid = d.uuid
WHERE d.log_collected_at >= TIMESTAMP '{start_date} 00:00:00'
  AND d.log_collected_at <  TIMESTAMP '{end_date} 00:00:00' + INTERVAL '1' day
  AND d.duration_s > 0
  AND lc.conversion_status = 2{vehicle_clause}
ORDER BY d.log_collected_at DESC
"""
    print(f"Querying data lake for runs from {start_date} to {end_date}...",
          file=sys.stderr)
    if vehicle:
        print(f"  Vehicle filter: {vehicle}", file=sys.stderr)

    rows = lake_client.query(sql)
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Stage 1: Discover candidate runs by date range."
    )
    parser.add_argument(
        "--start-date", required=True,
        help="Start date YYYY-MM-DD (inclusive)"
    )
    parser.add_argument(
        "--end-date", required=True,
        help="End date YYYY-MM-DD (inclusive)"
    )
    parser.add_argument(
        "--vehicle", default=None,
        help="Vehicle name (hyphen form, e.g. truck-806). Omit for all vehicles."
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})"
    )
    args = parser.parse_args()

    # Verify auth
    if not lake_client.check_auth():
        print("ERROR: Auth failed. Run: awslogin --sso", file=sys.stderr)
        sys.exit(1)

    # Discover runs
    runs = discover_runs(args.start_date, args.end_date, args.vehicle)

    # Print summary
    print(f"\nFound {len(runs)} converted runs.", file=sys.stderr)
    if runs:
        vehicles = sorted(set(r["vehicle_name"] for r in runs))
        print(f"  Vehicles: {', '.join(vehicles)}", file=sys.stderr)
        print(f"  Date range: {runs[-1]['log_collected_at'][:10]} "
              f"to {runs[0]['log_collected_at'][:10]}", file=sys.stderr)

        total_duration_min = sum(r.get("duration_min", 0) or 0 for r in runs)
        print(f"  Total duration: {total_duration_min:.1f} min "
              f"({total_duration_min / 60:.1f} hrs)", file=sys.stderr)

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(runs, f, indent=2)
    print(f"\nWrote {output_path} ({len(runs)} runs)", file=sys.stderr)


if __name__ == "__main__":
    main()
