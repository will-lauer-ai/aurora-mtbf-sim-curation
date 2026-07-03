#!/usr/bin/env python3
"""Stage 2: Filter runs by geographic region (bounding box or polygon).

Reads runs_raw.json from Stage 1, queries GPS data from the lake for each
run (batch bbox pre-filter, then exact polygon containment if needed),
and outputs runs_geo_filtered.json with only runs that have GPS points
inside the geofence.

Usage:
    python 02_filter_geo.py --input output/runs_raw.json --geofence geofences/japan_tomei.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import lake_client
from geofence import (
    load_geofence,
    geofence_bbox,
    point_in_bbox,
    point_in_geofence,
    filter_points_in_geofence,
)

OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_OUTPUT = OUTPUT_DIR / "runs_geo_filtered.json"
GPS_SAMPLE_LIMIT = 500  # max GPS points to retrieve per run


def batch_filter_bbox(
    run_uuids: list[str],
    bbox: tuple[float, float, float, float],
    dt_floor: str = "2025-01-01",
) -> set[str]:
    """Single SQL query: find all run_uuids that have GPS in a bounding box.

    This is much faster than querying per-run for bbox geofences.

    Returns:
        Set of run_uuids that have at least one GPS point inside the bbox.
    """
    min_lat, max_lat, min_lon, max_lon = bbox

    # Build IN clause (chunk if too many)
    # Trino handles large IN lists, but let's be reasonable
    uuid_list = ", ".join(f"'{u}'" for u in run_uuids)

    sql = f"""
SELECT DISTINCT run_uuid
FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
WHERE dt >= '{dt_floor}'
  AND run_uuid IN ({uuid_list})
  AND message.latitude_deg  BETWEEN {min_lat} AND {max_lat}
  AND message.longitude_deg BETWEEN {min_lon} AND {max_lon}
"""
    print("  Batch bbox pre-filter: querying GPS table...", file=sys.stderr)
    rows = lake_client.query(sql)
    return {r["run_uuid"] for r in rows}


def query_gps_for_run(
    run_uuid: str,
    dt_floor: str = "2025-01-01",
    limit: int = GPS_SAMPLE_LIMIT,
) -> list[dict]:
    """Query GPS points for a single run (sampled)."""
    sql = f"""
SELECT
  message.latitude_deg   AS lat,
  message.longitude_deg  AS lon
FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
WHERE dt >= '{dt_floor}'
  AND run_uuid = '{run_uuid}'
ORDER BY message.timestamp_ns
LIMIT {limit}
"""
    return lake_client.query(sql)


def query_gps_representative(
    run_uuids: list[str],
    dt_floor: str = "2025-01-01",
    days_back: int = 60,
) -> dict[str, dict]:
    """Get one representative GPS point per run (first point).

    Single query for all runs. Returns {run_uuid: {"lat": ..., "lon": ...}}.
    """
    if not run_uuids:
        return {}

    uuid_list = ", ".join(f"'{u}'" for u in run_uuids)
    sql = f"""
SELECT run_uuid, lat, lon
FROM (
  SELECT
    run_uuid,
    message.latitude_deg   AS lat,
    message.longitude_deg  AS lon,
    ROW_NUMBER() OVER (PARTITION BY run_uuid ORDER BY message.timestamp_ns) AS rn
  FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
  WHERE dt >= '{dt_floor}'
    AND CAST(dt AS DATE) >= date_add('day', -{days_back}, CURRENT_DATE)
    AND run_uuid IN ({uuid_list})
)
WHERE rn = 1
"""
    print("  Querying representative GPS points...", file=sys.stderr)
    rows = lake_client.query(sql)
    return {r["run_uuid"]: {"lat": r["lat"], "lon": r["lon"]} for r in rows}


def filter_runs_by_geo(
    runs: list[dict],
    geofence: dict,
) -> list[dict]:
    """Filter runs to those with GPS points inside the geofence.

    Strategy:
    - For bbox geofences: single batch SQL query (fast).
    - For polygon geofences: batch bbox pre-filter, then per-run GPS
      sampling for exact polygon containment.
    - In both cases, we also fetch a representative point for the output.
    """
    run_uuids = [r["run_uuid"] for r in runs]
    if not run_uuids:
        return []

    gf_bbox = geofence_bbox(geofence)
    min_lat, max_lat, min_lon, max_lon = gf_bbox

    print(f"Geofence: {geofence['type']} "
          f"({min_lat:.4f}-{max_lat:.4f} lat, {min_lon:.4f}-{max_lon:.4f} lon)",
          file=sys.stderr)

    # Step 1: Batch bbox pre-filter — find runs with ANY GPS in the bbox
    candidate_uuids = batch_filter_bbox(run_uuids, gf_bbox)
    print(f"  {len(candidate_uuids)} runs have GPS in bbox (pre-filter)",
          file=sys.stderr)

    if not candidate_uuids:
        return []

    # Step 2: For bbox geofences, the pre-filter IS the filter.
    # For polygon geofences, do exact polygon containment per run.
    if geofence["type"] == "bbox":
        # Bbox: the pre-filter is sufficient. Just get representative points.
        # Re-query for representative points of candidate runs.
        rep_points = query_gps_representative(list(candidate_uuids))

        result = []
        for run in runs:
            uid = run["run_uuid"]
            if uid in candidate_uuids:
                rep = rep_points.get(uid, {})
                in_fence = rep.get("lat") is not None and \
                    point_in_geofence(rep["lat"], rep["lon"], geofence)
                result.append({
                    **run,
                    "rep_lat": rep.get("lat"),
                    "rep_lon": rep.get("lon"),
                    "gps_points_in_geofence": -1,  # unknown for bbox batch
                    "gps_filter_method": "bbox_batch",
                })
        # Filter to only runs where the representative point is in the geofence
        # (or we couldn't get a rep point but the run had GPS in bbox)
        result_bbox = [r for r in result if r["rep_lat"] is not None and
                       point_in_geofence(r["rep_lat"], r["rep_lon"], geofence)]
        # If representative didn't match but run was in batch, keep it
        # (the rep point might be outside but other points inside)
        for r in result:
            if r not in result_bbox and r["run_uuid"] in candidate_uuids:
                result_bbox.append(r)

        return result_bbox

    else:
        # Polygon: need exact containment. Query GPS per candidate run.
        print(f"  Querying GPS samples for {len(candidate_uuids)} candidate runs...",
              file=sys.stderr)
        result = []
        for i, run in enumerate(runs):
            uid = run["run_uuid"]
            if uid not in candidate_uuids:
                continue

            if (i + 1) % 10 == 0 or i == 0:
                print(f"  Run {i+1}/{len(runs)}: {run.get('custom_id', uid[:16])}...",
                      file=sys.stderr)

            gps_points = query_gps_for_run(uid)
            in_fence_points = filter_points_in_geofence(gps_points, geofence)

            if in_fence_points:
                rep = in_fence_points[0]
                result.append({
                    **run,
                    "rep_lat": rep["lat"],
                    "rep_lon": rep["lon"],
                    "gps_points_in_geofence": len(in_fence_points),
                    "gps_filter_method": "polygon_exact",
                })

        return result


def main():
    parser = argparse.ArgumentParser(
        description="Stage 2: Filter runs by geographic region."
    )
    parser.add_argument(
        "--input", default=str(OUTPUT_DIR / "runs_raw.json"),
        help="Input JSON from Stage 1 (default: output/runs_raw.json)"
    )
    parser.add_argument(
        "--geofence", required=True,
        help="Path to geofence JSON file"
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

    # Load geofence
    geofence = load_geofence(args.geofence)

    # Verify auth
    if not lake_client.check_auth():
        print("ERROR: Auth failed. Run: awslogin --sso", file=sys.stderr)
        sys.exit(1)

    # Filter by geofence
    filtered = filter_runs_by_geo(runs, geofence)

    # Print summary
    print(f"\nFound {len(filtered)} runs in geofence.", file=sys.stderr)
    if filtered:
        vehicles = sorted(set(r["vehicle_name"] for r in filtered))
        print(f"  Vehicles: {', '.join(vehicles)}", file=sys.stderr)

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(filtered, f, indent=2)
    print(f"\nWrote {output_path} ({len(filtered)} runs)", file=sys.stderr)


if __name__ == "__main__":
    main()
