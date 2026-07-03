#!/usr/bin/env python3
"""Stage 3: Enrich filtered runs with map_key, ODD status, S3 path, GPS bounds.

Reads runs_geo_filtered.json from Stage 2 and enriches each run with:
- map_key, s3_path, log_version (from runs.custom_metadata)
- in_odd, odd_area_name, autonomous_time_in_odd_s (from adk_state_timeline_metric)
- gps_bbox: min/max lat/lon of the run's trajectory

Usage:
    python 03_enrich_metadata.py --input output/runs_geo_filtered.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import lake_client

OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_OUTPUT = OUTPUT_DIR / "runs_enriched.json"


def enrich_run_metadata(run_uuids: list[str]) -> dict[str, dict]:
    """Batch query: map_key, s3_path, log_version for all runs.

    Returns {run_uuid: {"map_key": ..., "s3_path": ..., "log_version": ...}}
    """
    if not run_uuids:
        return {}

    # Build UUID list for IN clause
    # Cast to uuid type for the Postgres comparison
    uuid_list = ", ".join(f"CAST('{u}' AS uuid)" for u in run_uuids)

    sql = f"""
SELECT
  CAST(uuid AS varchar) AS run_uuid,
  json_extract_scalar(custom_metadata, '$.map_key')      AS map_key,
  json_extract_scalar(custom_metadata, '$.ursa_s3_path')  AS s3_path,
  json_extract_scalar(custom_metadata, '$.log_version')   AS log_version
FROM ursa_log_management.public.runs
WHERE uuid IN ({uuid_list})
"""
    print("  Enriching run metadata (map_key, s3_path)...", file=sys.stderr)
    rows = lake_client.query(sql)
    return {r["run_uuid"]: r for r in rows}


def enrich_odd_status(
    run_uuids: list[str],
    dt_floor: str = "2025-01-01",
) -> dict[str, dict]:
    """Query ADK state timeline for ODD status per run.

    For each run, gets in_odd, odd_area_name, and total ACTIVE time in-ODD.
    Returns {run_uuid: {"in_odd": bool, "odd_area_name": str, "autonomous_time_in_odd_s": float}}
    """
    if not run_uuids:
        return {}

    # Single batch query: all runs at once
    uuid_list = ", ".join(f"'{u}'" for u in run_uuids)

    sql = f"""
SELECT
  run_uuid,
  element_at(metric_value.master_odd, 'in_odd')         AS in_odd,
  element_at(metric_value.master_odd, 'odd_area_name')    AS odd_area_name,
  SUM(metric_value.duration_ns) / 1e9                     AS total_duration_s
FROM hudi_hms.ursa_metric.adk_state_timeline_metric
WHERE dt >= '{dt_floor}'
  AND run_uuid IN ({uuid_list})
  AND metric_value.adk_state_name = 'ACTIVE'
  AND element_at(metric_value.master_odd, 'in_odd') = 'true'
GROUP BY 1, 2, 3
"""
    print("  Querying ODD status (ADK state timeline)...", file=sys.stderr)
    rows = lake_client.query(sql)

    result = {}
    for r in rows:
        uid = r["run_uuid"]
        if uid not in result:
            result[uid] = {
                "in_odd": r["in_odd"] == "true",
                "odd_area_name": r.get("odd_area_name", ""),
                "autonomous_time_in_odd_s": float(r.get("total_duration_s", 0) or 0),
            }
        else:
            # Accumulate if multiple ODD areas
            result[uid]["autonomous_time_in_odd_s"] += float(
                r.get("total_duration_s", 0) or 0
            )

    return result


def enrich_gps_bbox(
    run_uuids: list[str],
    dt_floor: str = "2025-01-01",
) -> dict[str, dict]:
    """Query GPS bounding box (min/max lat/lon) per run.

    Single batch query.
    Returns {run_uuid: {"min_lat": ..., "max_lat": ..., "min_lon": ..., "max_lon": ...}}
    """
    if not run_uuids:
        return {}

    uuid_list = ", ".join(f"'{u}'" for u in run_uuids)

    sql = f"""
SELECT
  run_uuid,
  MIN(message.latitude_deg)   AS min_lat,
  MAX(message.latitude_deg)   AS max_lat,
  MIN(message.longitude_deg)  AS min_lon,
  MAX(message.longitude_deg)  AS max_lon
FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
WHERE dt >= '{dt_floor}'
  AND run_uuid IN ({uuid_list})
GROUP BY 1
"""
    print("  Querying GPS bounding boxes...", file=sys.stderr)
    rows = lake_client.query(sql)
    return {r["run_uuid"]: r for r in rows}


def enrich_runs(runs: list[dict]) -> list[dict]:
    """Enrich all runs with metadata, ODD, and GPS bounds."""
    run_uuids = [r["run_uuid"] for r in runs]
    if not run_uuids:
        return []

    # Batch queries
    metadata = enrich_run_metadata(run_uuids)
    odd_status = enrich_odd_status(run_uuids)
    gps_bboxes = enrich_gps_bbox(run_uuids)

    # Merge into runs
    enriched = []
    for run in runs:
        uid = run["run_uuid"]
        meta = metadata.get(uid, {})
        odd = odd_status.get(uid, {})
        bbox = gps_bboxes.get(uid, {})

        enriched_run = {
            **run,
            "map_key": meta.get("map_key"),
            "s3_path": meta.get("s3_path"),
            "log_version": meta.get("log_version"),
            "in_odd": odd.get("in_odd", False),
            "odd_area_name": odd.get("odd_area_name", ""),
            "autonomous_time_in_odd_s": odd.get("autonomous_time_in_odd_s", 0.0),
            "gps_min_lat": bbox.get("min_lat"),
            "gps_max_lat": bbox.get("max_lat"),
            "gps_min_lon": bbox.get("min_lon"),
            "gps_max_lon": bbox.get("max_lon"),
        }
        enriched.append(enriched_run)

    return enriched


def main():
    parser = argparse.ArgumentParser(
        description="Stage 3: Enrich runs with map, ODD, S3, GPS bounds."
    )
    parser.add_argument(
        "--input", default=str(OUTPUT_DIR / "runs_geo_filtered.json"),
        help="Input JSON from Stage 2"
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})"
    )
    args = parser.parse_args()

    # Load runs
    with open(args.input) as f:
        runs = json.load(f)
    print(f"Loaded {len(runs)} runs from {args.input}", file=sys.stderr)

    # Verify auth
    if not lake_client.check_auth():
        print("ERROR: Auth failed. Run: awslogin --sso", file=sys.stderr)
        sys.exit(1)

    # Enrich
    print("Enriching run metadata...", file=sys.stderr)
    enriched = enrich_runs(runs)

    # Print summary
    print(f"\nEnriched {len(enriched)} runs.", file=sys.stderr)
    if enriched:
        in_odd_count = sum(1 for r in enriched if r.get("in_odd"))
        print(f"  In-ODD: {in_odd_count}/{len(enriched)}", file=sys.stderr)

        map_keys = {}
        for r in enriched:
            mk = r.get("map_key") or "unknown"
            map_keys[mk] = map_keys.get(mk, 0) + 1
        print(f"  Map keys: {map_keys}", file=sys.stderr)

        odd_areas = {}
        for r in enriched:
            oa = r.get("odd_area_name") or "unknown"
            odd_areas[oa] = odd_areas.get(oa, 0) + 1
        print(f"  ODD areas: {odd_areas}", file=sys.stderr)

        total_odd_time = sum(r.get("autonomous_time_in_odd_s", 0) for r in enriched)
        print(f"  Total autonomous time in-ODD: {total_odd_time:.1f}s "
              f"({total_odd_time / 3600:.1f} hrs)", file=sys.stderr)

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(enriched, f, indent=2)
    print(f"\nWrote {output_path} ({len(enriched)} runs)", file=sys.stderr)


if __name__ == "__main__":
    main()
