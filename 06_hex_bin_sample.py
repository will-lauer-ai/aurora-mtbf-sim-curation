#!/usr/bin/env python3
"""Stage 6: density-aware H3 hexagonal binning sampler.

Two H3 resolutions are used:

1. coverage resolution (`--h3-resolution`): fine hexes for visualizing where
   all GPS data exists and how dense it is.
2. sample resolution (`--sample-h3-resolution`): coarser hexes used as the
   actual sampling buckets. By default this is one level coarser than coverage.

This fixes the earlier problem where sampling one per fine occupied hex over-
sampled dense road corridors and missed sparse areas. The default behavior is
now: **one sample per coarser spatial bucket that has any in-geofence data**.

Outputs:
  - output/hex_bins.json       fine coverage hexes + density
  - output/hex_sample.json     selected samples, one per sample hex by default
  - output/hex_sample.csv      lookup-friendly sample CSV
  - output/hex_sample_map.jpg  plot with geofence underlay, density, samples
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from urllib.parse import quote
from collections import Counter, defaultdict
from pathlib import Path

import h3

import lake_client
from geofence import load_geofence, point_in_geofence

DATA_EXPLORER_URSA_PLAYBACK_BASE_URL = "https://neuron.oci.applied.dev/data_explorer/v2/log/playback"
DATA_EXPLORER_LOG_PLAYBACK_BASE_URL = "https://neuron.oci.applied.dev/data_explorer/library/log/playback"
DATA_EXPLORER_DRIVE_BASE_URL = "https://neuron.oci.applied.dev/data_explorer/library/drives"
FRONTIER_ADP_BASE_URL = "https://frontier.prod.applied.dev"
ADP_QUERY_HINT = (
    "LogSim replay URLs require the ADP LogSim simulation UUID. If raw_data_uri "
    "is a LogSim path, this script infers it from the final S3 path component; "
    "otherwise use Ursa DescribeRun(...).sim_run_info.adp_uuid after LogSim generation."
)

OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_BINS_OUTPUT = OUTPUT_DIR / "hex_bins.json"
DEFAULT_SAMPLE_JSON = OUTPUT_DIR / "hex_sample.json"
DEFAULT_SAMPLE_CSV = OUTPUT_DIR / "hex_sample.csv"
DEFAULT_MAP_OUTPUT = OUTPUT_DIR / "hex_sample_map.jpg"
DEFAULT_URLS_OUTPUT = OUTPUT_DIR / "hex_sample_urls.txt"

QUERY_CHUNK_SIZE = 20
DEFAULT_MAX_POINTS_PER_RUN = 500


def chunks(items: list[str], n: int):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def infer_adp_logsim_uuid(run: dict) -> str | None:
    """Infer ADP LogSim simulation UUID when available.

    The ADP replay URL uses the ADP simulation UUID, not the Ursa run_uuid.
    For Ursa LogSim runs, raw_data_uri usually looks like:

        s3://ursa-frontier-prod-raw-logs/LogSim/.../<adp_sim_uuid>/
        s3://ursa-frontier-prod-raw-logs/LogSim/.../batch-181671/<adp_sim_uuid>/

    For ordinary source drive logs, raw_data_uri is usually under
    s3://.../truck-XXX/... and there is no LogSim replay URL yet.
    """
    for key in ("adp_logsim_uuid", "adp_sim_uuid", "adp_uuid", "simulation_run_uuid"):
        val = run.get(key)
        if val:
            return str(val)

    raw_uri = run.get("raw_data_uri") or run.get("s3_path") or ""
    if "/LogSim/" not in raw_uri:
        return None
    trimmed = raw_uri.rstrip("/")
    if not trimmed:
        return None
    return trimmed.split("/")[-1] or None


def make_logsim_result_url(adp_logsim_uuid: str) -> str:
    return f"{FRONTIER_ADP_BASE_URL}/log_sim/results/sim/{adp_logsim_uuid}"


def make_logsim_replay_url(adp_logsim_uuid: str) -> str:
    return f"{FRONTIER_ADP_BASE_URL}/log_sim/results/sim/{adp_logsim_uuid}/playback"


def get_data_explorer_log_path(run: dict) -> str | None:
    """Return the Data Explorer logPath identifier, not the raw S3 URI.

    The multi-source playback route expects the processed drive logPath used by
    Data Explorer's LogConversion rows, e.g. `2026-06-28_08-08-12_truck-808`.
    Passing the full raw S3 URI causes `Log path ... not found`.
    """
    for key in ("data_explorer_log_path", "log_path", "custom_id"):
        val = run.get(key)
        if val:
            return str(val).strip().rstrip("/")

    raw_uri = (run.get("raw_data_uri") or run.get("s3_path") or "").strip().rstrip("/")
    if raw_uri:
        return raw_uri.split("/")[-1]
    return None


def add_lookup_links(runs: list[dict]) -> list[dict]:
    out = []
    for r in runs:
        rr = dict(r)
        rr["ursa_run_uuid"] = rr.get("run_uuid")
        rr["adp_lookup_custom_id"] = rr.get("custom_id")
        rr["adp_lookup_note"] = ADP_QUERY_HINT
        dx = rr.get("data_explorer_uuid")
        ursa_run_uuid = rr.get("run_uuid") or rr.get("ursa_run_uuid")
        data_explorer_log_path = get_data_explorer_log_path(rr)
        rr["data_explorer_log_path"] = data_explorer_log_path
        # Primary direct source-log playback route. The v2 visualizer accepts an Ursa log ID via
        # `?ursa=...`; this is the ID we reliably have from ursa_log_management.public.runs.
        rr["data_explorer_ursa_playback_url"] = (
            f"{DATA_EXPLORER_URSA_PLAYBACK_BASE_URL}?ursa={quote(str(ursa_run_uuid), safe='')}"
            if ursa_run_uuid else None
        )
        # Older/feature-flagged multi-source playback route. Keep as a secondary candidate only;
        # it can report `Log path ... not found` in Frontier depending on which index backs the UI.
        rr["data_explorer_log_playback_url"] = (
            f"{DATA_EXPLORER_LOG_PLAYBACK_BASE_URL}?logPath={quote(data_explorer_log_path, safe='')}"
            if data_explorer_log_path else None
        )
        # This route works only when dx is a DriveRun UUID. Frontier's data_explorer_uuid is often a
        # raw/source log identifier, so keep it as a tertiary candidate rather than the primary link.
        rr["data_explorer_drive_run_playback_url"] = f"{DATA_EXPLORER_DRIVE_BASE_URL}/{dx}/playback" if dx else None
        rr["data_explorer_playback_url"] = (
            rr["data_explorer_ursa_playback_url"]
            or rr["data_explorer_log_playback_url"]
            or rr["data_explorer_drive_run_playback_url"]
        )
        # Backwards-compatible alias.
        rr["data_explorer_url"] = rr["data_explorer_playback_url"]

        adp_logsim_uuid = infer_adp_logsim_uuid(rr)
        rr["adp_logsim_uuid"] = adp_logsim_uuid
        rr["logsim_result_url"] = make_logsim_result_url(adp_logsim_uuid) if adp_logsim_uuid else None
        rr["logsim_replay_url"] = make_logsim_replay_url(adp_logsim_uuid) if adp_logsim_uuid else None
        rr["logsim_replay_note"] = (
            "OK" if adp_logsim_uuid else "No LogSim replay URL: selected row appears to be a source drive/log, not an ADP LogSim run."
        )
        out.append(rr)
    return out


def query_gps_points_for_runs(
    run_uuids: list[str],
    max_points_per_run: int = DEFAULT_MAX_POINTS_PER_RUN,
    dt_floor: str = "2025-01-01",
) -> list[dict]:
    """Query evenly downsampled GPS points for candidate runs."""
    all_points: list[dict] = []
    for chunk_idx, chunk in enumerate(chunks(run_uuids, QUERY_CHUNK_SIZE), start=1):
        uuid_list = ", ".join(f"'{u}'" for u in chunk)
        sql = f"""
WITH numbered AS (
  SELECT
    run_uuid,
    message.latitude_deg   AS lat,
    message.longitude_deg  AS lon,
    ROW_NUMBER() OVER (PARTITION BY run_uuid ORDER BY message.timestamp_ns) AS rn,
    COUNT(*)    OVER (PARTITION BY run_uuid) AS cnt
  FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
  WHERE dt >= '{dt_floor}'
    AND run_uuid IN ({uuid_list})
), sampled AS (
  SELECT
    run_uuid, lat, lon, rn, cnt,
    GREATEST(1, CAST(CEIL(CAST(cnt AS double) / {max_points_per_run}) AS bigint)) AS stride
  FROM numbered
)
SELECT run_uuid, lat, lon, rn
FROM sampled
WHERE rn = 1
   OR rn = cnt
   OR MOD(rn - 1, stride) = 0
ORDER BY run_uuid, rn
"""
        print(f"GPS query chunk {chunk_idx}: {len(chunk)} runs (<= {max_points_per_run} pts/run)...", file=sys.stderr)
        all_points.extend(lake_client.query(sql, limit=9999))
    return all_points


def choose_best_for_cell(points: list[dict], run_by_uuid: dict[str, dict], center_lat: float, center_lon: float, excluded_run_uuids: set[str] | None = None):
    """Choose best representative point/run for a cell.

    Ranking:
      1. prefer runs not already selected if excluded_run_uuids provided
      2. more GPS points from that run in cell
      3. nearest point to cell center
      4. deterministic run metadata tie-breaks
    """
    excluded_run_uuids = excluded_run_uuids or set()
    run_counts = Counter(p["run_uuid"] for p in points)
    best = None
    for run_uuid, count in run_counts.items():
        run_pts = [p for p in points if p["run_uuid"] == run_uuid]
        nearest = min(run_pts, key=lambda p: haversine_m(center_lat, center_lon, p["lat"], p["lon"]))
        nearest_dist = haversine_m(center_lat, center_lon, nearest["lat"], nearest["lon"])
        run = run_by_uuid.get(run_uuid, {})
        candidate = (
            run_uuid in excluded_run_uuids,  # False sorts before True
            -count,
            nearest_dist,
            run.get("log_collected_at", ""),
            run.get("custom_id", ""),
            run_uuid,
            nearest,
        )
        if best is None or candidate < best:
            best = candidate
    return best, run_counts


def build_hex_bins(
    runs: list[dict],
    geofence: dict,
    h3_resolution: int,
    sample_h3_resolution: int,
    max_points_per_run: int,
    min_points_per_hex: int,
    min_points_per_sample_hex: int,
    samples_per_sample_hex: int,
    prefer_unique_runs: bool,
) -> tuple[list[dict], list[dict]]:
    """Build fine coverage bins and coarse density-aware sample buckets."""
    runs = add_lookup_links(runs)
    run_by_uuid = {r["run_uuid"]: r for r in runs}
    run_uuids = list(run_by_uuid.keys())

    print(f"Querying GPS for {len(run_uuids)} candidate runs...", file=sys.stderr)
    points = query_gps_points_for_runs(run_uuids, max_points_per_run=max_points_per_run)
    print(f"Pulled {len(points)} downsampled GPS points", file=sys.stderr)

    coverage_cell_points: dict[str, list[dict]] = defaultdict(list)
    sample_cell_points: dict[str, list[dict]] = defaultdict(list)
    kept = 0
    for p in points:
        lat, lon = p.get("lat"), p.get("lon")
        if lat is None or lon is None or not point_in_geofence(lat, lon, geofence):
            continue
        coverage_cell_points[h3.latlng_to_cell(lat, lon, h3_resolution)].append(p)
        sample_cell_points[h3.latlng_to_cell(lat, lon, sample_h3_resolution)].append(p)
        kept += 1

    print(f"Kept {kept} GPS points inside geofence", file=sys.stderr)
    print(f"Coverage H3 cells before filtering: {len(coverage_cell_points)}", file=sys.stderr)
    print(f"Sample H3 cells before filtering:   {len(sample_cell_points)}", file=sys.stderr)

    coverage_cell_points = {c: pts for c, pts in coverage_cell_points.items() if len(pts) >= min_points_per_hex}
    sample_cell_points = {c: pts for c, pts in sample_cell_points.items() if len(pts) >= min_points_per_sample_hex}

    print(f"Coverage H3 cells after min_points_per_hex={min_points_per_hex}: {len(coverage_cell_points)}", file=sys.stderr)
    print(f"Sample H3 cells after min_points_per_sample_hex={min_points_per_sample_hex}: {len(sample_cell_points)}", file=sys.stderr)

    # Fine coverage/density bins for visualization.
    hex_bins: list[dict] = []
    for cell, pts in sorted(coverage_cell_points.items()):
        center_lat, center_lon = h3.cell_to_latlng(cell)
        run_counts = Counter(p["run_uuid"] for p in pts)
        parent_sample_cell = h3.cell_to_parent(cell, sample_h3_resolution) if sample_h3_resolution < h3_resolution else cell
        hex_bins.append({
            "h3_cell": cell,
            "h3_resolution": h3_resolution,
            "sample_h3_cell": parent_sample_cell,
            "sample_h3_resolution": sample_h3_resolution,
            "center_lat": center_lat,
            "center_lon": center_lon,
            "point_count": len(pts),
            "distinct_runs": len(run_counts),
        })

    # Coarse sample buckets: one or N samples per sample hex.
    selected_runs: list[dict] = []
    already_selected_run_uuids: set[str] = set()
    for sample_cell, pts in sorted(sample_cell_points.items()):
        center_lat, center_lon = h3.cell_to_latlng(sample_cell)
        local_points = list(pts)
        selected_in_this_cell = 0
        local_excluded: set[str] = set()
        while selected_in_this_cell < samples_per_sample_hex and local_points:
            excluded = already_selected_run_uuids | local_excluded if prefer_unique_runs else local_excluded
            best, run_counts = choose_best_for_cell(local_points, run_by_uuid, center_lat, center_lon, excluded)
            if best is None:
                break
            _, _, nearest_dist, _, _, selected_uuid, selected_point = best
            selected_run = run_by_uuid[selected_uuid]
            sample = {
                **selected_run,
                "sample_h3_cell": sample_cell,
                "sample_h3_resolution": sample_h3_resolution,
                "coverage_h3_resolution": h3_resolution,
                "sample_lat": selected_point["lat"],
                "sample_lon": selected_point["lon"],
                "hex_center_lat": center_lat,
                "hex_center_lon": center_lon,
                "hex_point_count": len(pts),
                "hex_distinct_runs": len(Counter(p["run_uuid"] for p in pts)),
                "run_points_in_hex": run_counts[selected_uuid],
                "sample_dist_to_hex_center_m": round(nearest_dist, 2),
            }
            # Backward-compatible aliases.
            sample["h3_cell"] = sample_cell
            sample["h3_resolution"] = sample_h3_resolution
            selected_runs.append(sample)
            already_selected_run_uuids.add(selected_uuid)
            local_excluded.add(selected_uuid)
            selected_in_this_cell += 1

    return hex_bins, selected_runs


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def write_replay_urls(path: Path, samples: list[dict], *, print_urls: bool = True) -> list[dict]:
    """Write and optionally print unique Frontier ADP LogSim replay URLs.

    Samples are per hex, so the same run can be selected multiple times.
    This function deduplicates by LogSim URL / run UUID so the printed list is
    a practical replay checklist rather than hundreds of repeated links.
    """
    seen = set()
    rows: list[dict] = []
    for s in samples:
        run_uuid = s.get("run_uuid") or s.get("ursa_run_uuid") or ""
        custom_id = s.get("custom_id") or ""
        adp_logsim_uuid = s.get("adp_logsim_uuid") or ""
        logsim_replay_url = s.get("logsim_replay_url") or ""
        logsim_result_url = s.get("logsim_result_url") or ""
        data_explorer_playback_url = s.get("data_explorer_playback_url") or s.get("data_explorer_url") or ""
        data_explorer_ursa_playback_url = s.get("data_explorer_ursa_playback_url") or ""
        data_explorer_log_path = s.get("data_explorer_log_path") or ""
        data_explorer_log_playback_url = s.get("data_explorer_log_playback_url") or ""
        data_explorer_drive_run_playback_url = s.get("data_explorer_drive_run_playback_url") or ""
        raw_data_uri = s.get("raw_data_uri") or s.get("s3_path") or ""
        key = logsim_replay_url or run_uuid or custom_id
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append({
            "custom_id": custom_id,
            "ursa_run_uuid": run_uuid,
            "adp_logsim_uuid": adp_logsim_uuid,
            "logsim_replay_url": logsim_replay_url,
            "logsim_result_url": logsim_result_url,
            "data_explorer_playback_url": data_explorer_playback_url,
            "data_explorer_ursa_playback_url": data_explorer_ursa_playback_url,
            "data_explorer_log_path": data_explorer_log_path,
            "data_explorer_log_playback_url": data_explorer_log_playback_url,
            "data_explorer_drive_run_playback_url": data_explorer_drive_run_playback_url,
            "raw_data_uri": raw_data_uri,
            "note": s.get("logsim_replay_note") or "",
        })

    cols = [
        "custom_id", "ursa_run_uuid", "adp_logsim_uuid", "logsim_replay_url",
        "logsim_result_url", "data_explorer_playback_url", "data_explorer_ursa_playback_url",
        "data_explorer_log_path", "data_explorer_log_playback_url",
        "data_explorer_drive_run_playback_url", "raw_data_uri", "note",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    if print_urls:
        replay_rows = [r for r in rows if r.get("logsim_replay_url")]
        unresolved = [r for r in rows if not r.get("logsim_replay_url")]
        print("\nFrontier ADP LogSim replay URLs", file=sys.stderr)
        print("=" * 50, file=sys.stderr)
        print(f"Wrote URL list: {path}", file=sys.stderr)
        if replay_rows:
            for r in replay_rows:
                label = r["adp_logsim_uuid"] or r["custom_id"] or r["ursa_run_uuid"]
                print(f"- {label}", file=sys.stderr)
                print(f"  {r['logsim_replay_url']}", file=sys.stderr)
        else:
            print("No direct LogSim replay URLs could be inferred from the selected rows.", file=sys.stderr)
        if unresolved:
            print("\nRows without LogSim replay URL (likely source drive logs, not LogSim runs):", file=sys.stderr)
            for r in unresolved[:25]:
                print(f"- {r['custom_id']} | Ursa: {r['ursa_run_uuid']}", file=sys.stderr)
                if r.get("data_explorer_playback_url"):
                    print(f"  Data Explorer drive playback: {r['data_explorer_playback_url']}", file=sys.stderr)
            if len(unresolved) > 25:
                print(f"  ... {len(unresolved) - 25} more unresolved rows in {path}", file=sys.stderr)
            print("\nTo get a LogSim replay URL, run LogSim for the selected drive segment or resolve the ADP UUID via Ursa DescribeRun(...).sim_run_info.adp_uuid.", file=sys.stderr)
    return rows


def write_sample_csv(path: Path, samples: list[dict]) -> None:
    cols = [
        "sample_h3_cell", "sample_h3_resolution", "coverage_h3_resolution",
        "run_uuid", "custom_id", "vehicle_name", "log_collected_at", "duration_min",
        "sample_lat", "sample_lon", "hex_center_lat", "hex_center_lon",
        "hex_point_count", "hex_distinct_runs", "run_points_in_hex", "sample_dist_to_hex_center_m",
        "ursa_run_uuid", "adp_logsim_uuid", "logsim_replay_url", "logsim_result_url", "logsim_replay_note",
        "data_explorer_uuid", "data_explorer_playback_url", "data_explorer_ursa_playback_url",
        "data_explorer_log_path", "data_explorer_log_playback_url", "data_explorer_drive_run_playback_url",
        "data_explorer_url", "adp_lookup_custom_id", "adp_lookup_note",
        "raw_data_uri", "map_key", "route", "stack_commit",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(samples)


def _draw_geofence_underlay(ax, geofence, plt, MplPolygon):
    """Draw filled geofence underlay only."""
    if geofence["type"] == "bbox":
        min_lat, max_lat, min_lon, max_lon = geofence["min_lat"], geofence["max_lat"], geofence["min_lon"], geofence["max_lon"]
        fill = plt.Rectangle((min_lon, min_lat), max_lon - min_lon, max_lat - min_lat,
                             linewidth=0, facecolor="#ff6b6b", alpha=0.10, zorder=0)
        ax.add_patch(fill)
    elif geofence["type"] == "polygon":
        xy = [(p["lon"], p["lat"]) for p in geofence["points"]]
        ax.add_patch(MplPolygon(xy, closed=True, linewidth=0, facecolor="#ff6b6b", alpha=0.10, zorder=0))


def _draw_geofence_lines(ax, geofence, plt, MplPolygon, *, zorder: int = 9):
    """Draw highly visible geofence boundary lines on top of all map layers.

    Draws a white halo underneath a red dashed line so the boundary stays
    visible over dense hexes and selected sample points.
    """
    if geofence["type"] == "bbox":
        min_lat, max_lat, min_lon, max_lon = geofence["min_lat"], geofence["max_lat"], geofence["min_lon"], geofence["max_lon"]
        # White halo
        ax.add_patch(plt.Rectangle((min_lon, min_lat), max_lon - min_lon, max_lat - min_lat,
                                   linewidth=5.0, edgecolor="white", facecolor="none", linestyle="-", zorder=zorder))
        # Red dashed boundary
        ax.add_patch(plt.Rectangle((min_lon, min_lat), max_lon - min_lon, max_lat - min_lat,
                                   linewidth=2.6, edgecolor="#e60000", facecolor="none", linestyle="--", zorder=zorder + 1,
                                   label="Geofence boundary"))
    elif geofence["type"] == "polygon":
        xy = [(p["lon"], p["lat"]) for p in geofence["points"]]
        ax.add_patch(MplPolygon(xy, closed=True, linewidth=5.0, edgecolor="white", facecolor="none", linestyle="-", zorder=zorder))
        ax.add_patch(MplPolygon(xy, closed=True, linewidth=2.6, edgecolor="#e60000", facecolor="none", linestyle="--", zorder=zorder + 1,
                                label="Geofence boundary"))


def plot_hex_bins(hex_bins: list[dict], samples: list[dict], geofence: dict, output_path: Path, sample_dot_size: float = 35.0) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Polygon as MplPolygon
        from matplotlib.collections import PatchCollection
    except ImportError:
        print("matplotlib not installed; skipping plot. Run: pip install matplotlib", file=sys.stderr)
        return

    fig, ax = plt.subplots(1, 1, figsize=(13, 10), dpi=300)
    ax.set_facecolor("#fbfbfb")

    # Geofence underlay first, so density and samples sit on top of it.
    _draw_geofence_underlay(ax, geofence, plt, MplPolygon)

    # Fine coverage/density hexes.
    patches, counts = [], []
    for hb in hex_bins:
        boundary = h3.cell_to_boundary(hb["h3_cell"])
        patches.append(MplPolygon([(lon, lat) for lat, lon in boundary], closed=True))
        counts.append(hb["point_count"])
    if patches:
        collection = PatchCollection(patches, cmap="viridis", alpha=0.45, linewidths=0.25, edgecolors="#333333", zorder=2)
        collection.set_array(counts)
        ax.add_collection(collection)
        cbar = fig.colorbar(collection, ax=ax, fraction=0.035, pad=0.02)
        cbar.set_label("GPS points per fine coverage hex", fontsize=9)

    # Coarse sample hex outlines to show actual buckets.
    sample_cells = sorted({s["sample_h3_cell"] for s in samples})
    sample_patches = [MplPolygon([(lon, lat) for lat, lon in h3.cell_to_boundary(c)], closed=True) for c in sample_cells]
    if sample_patches:
        sample_collection = PatchCollection(sample_patches, facecolor="none", edgecolors="#0057ff", linewidths=0.85, alpha=0.55, zorder=3)
        ax.add_collection(sample_collection)

    # Selected samples as large red dots.
    ax.scatter([s["sample_lon"] for s in samples], [s["sample_lat"] for s in samples],
               s=sample_dot_size, color="#ff2d2d", edgecolors="black", linewidths=0.35, alpha=0.90, zorder=6,
               label=f"Selected samples: one per sample hex ({len(samples)})")

    # Draw geofence boundary lines last so they are always visible.
    _draw_geofence_lines(ax, geofence, plt, MplPolygon, zorder=9)

    # Axis limits from geofence.
    if geofence["type"] == "bbox":
        min_lat, max_lat, min_lon, max_lon = geofence["min_lat"], geofence["max_lat"], geofence["min_lon"], geofence["max_lon"]
    else:
        lats, lons = [p["lat"] for p in geofence["points"]], [p["lon"] for p in geofence["points"]]
        min_lat, max_lat, min_lon, max_lon = min(lats), max(lats), min(lons), max(lons)
    lat_pad = max((max_lat - min_lat) * 0.08, 0.005)
    lon_pad = max((max_lon - min_lon) * 0.08, 0.005)
    ax.set_xlim(min_lon - lon_pad, max_lon + lon_pad)
    ax.set_ylim(min_lat - lat_pad, max_lat + lat_pad)
    ax.set_aspect("equal", adjustable="box")

    total_points = sum(hb["point_count"] for hb in hex_bins)
    title = (
        "Density-Aware H3 Spatial Sampling\n"
        f"fine coverage hexes: {len(hex_bins)} • sample buckets: {len(sample_cells)} • samples: {len(samples)} • GPS points: {total_points}"
    )
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper right", framealpha=0.92)
    ax.text(0.01, 0.01,
            "Red fill + red dashed boundary = geofence. Viridis = source density. Blue outlines = coarse sample buckets. Red dots = selected samples.",
            transform=ax.transAxes, fontsize=8,
            bbox=dict(boxstyle="round,pad=0.35", facecolor="white", alpha=0.88, edgecolor="#bbbbbb"), zorder=10)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    fig.savefig(str(output_path), format="jpg", dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Wrote {output_path} ({output_path.stat().st_size // 1024} KB)", file=sys.stderr)


def print_summary(hex_bins: list[dict], samples: list[dict]) -> None:
    print("\nHex Sampling Summary", file=sys.stderr)
    print("=" * 50, file=sys.stderr)
    print(f"Coverage hexes:       {len(hex_bins)}", file=sys.stderr)
    print(f"Sample buckets:       {len(set(s['sample_h3_cell'] for s in samples))}", file=sys.stderr)
    print(f"Samples selected:     {len(samples)}", file=sys.stderr)
    if hex_bins:
        counts = [h["point_count"] for h in hex_bins]
        print(f"GPS points in hexes:  {sum(counts)}", file=sys.stderr)
        print(f"Points/fine hex:      min={min(counts)}, median={sorted(counts)[len(counts)//2]}, max={max(counts)}", file=sys.stderr)
    vehicle_counts = Counter(s.get("vehicle_name", "unknown") for s in samples)
    print("Sample vehicles:      " + ", ".join(f"{k} ({v})" for k, v in vehicle_counts.most_common()), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Stage 6: density-aware H3 hex-bin spatial sampler.")
    parser.add_argument("--input", default=str(OUTPUT_DIR / "runs_raw.json"), help="Candidate runs JSON, usually output/runs_raw.json")
    parser.add_argument("--geofence", required=True, help="Geofence JSON")
    parser.add_argument("--h3-resolution", type=int, default=10, help="Fine coverage H3 resolution for density map")
    parser.add_argument("--sample-h3-resolution", type=int, default=None, help="Coarser sampling H3 resolution. Default: h3-resolution - 1")
    parser.add_argument("--max-points-per-run", type=int, default=DEFAULT_MAX_POINTS_PER_RUN, help="Evenly downsample each run to this many GPS points")
    parser.add_argument("--min-points-per-hex", type=int, default=2, help="Drop fine coverage hexes with fewer points than this")
    parser.add_argument("--min-points-per-sample-hex", type=int, default=1, help="Drop sample buckets with fewer points than this")
    parser.add_argument("--samples-per-sample-hex", type=int, default=1, help="Samples to take per coarse sample bucket")
    parser.add_argument("--prefer-unique-runs", action="store_true", help="Prefer selecting UUIDs not already used in other buckets")
    parser.add_argument("--bins-output", default=str(DEFAULT_BINS_OUTPUT), help="Output JSON for fine coverage hex bins")
    parser.add_argument("--sample-json", default=str(DEFAULT_SAMPLE_JSON), help="Output JSON for selected samples")
    parser.add_argument("--sample-csv", default=str(DEFAULT_SAMPLE_CSV), help="Output CSV for selected samples")
    parser.add_argument("--plot", action="store_true", help="Generate a JPG plot of density + sample buckets + selected samples")
    parser.add_argument("--plot-output", default=str(DEFAULT_MAP_OUTPUT), help="Output JPG path for --plot")
    parser.add_argument("--sample-dot-size", type=float, default=35.0, help="Area of selected-sample dots in plot (default: 35; previous was 115)")
    parser.add_argument("--urls-output", default=str(DEFAULT_URLS_OUTPUT), help="Output TSV path for unique replay/lookup URLs")
    parser.add_argument("--no-print-urls", action="store_true", help="Do not print replay/lookup URLs to stderr; still writes --urls-output")
    args = parser.parse_args()

    sample_res = args.sample_h3_resolution if args.sample_h3_resolution is not None else max(0, args.h3_resolution - 1)
    if sample_res > args.h3_resolution:
        print("ERROR: --sample-h3-resolution should be <= --h3-resolution", file=sys.stderr)
        sys.exit(2)

    with open(args.input) as f:
        runs = json.load(f)
    print(f"Loaded {len(runs)} candidate runs from {args.input}", file=sys.stderr)
    print(f"Fine coverage H3 resolution: {args.h3_resolution}", file=sys.stderr)
    print(f"Coarse sample H3 resolution: {sample_res}", file=sys.stderr)

    geofence = load_geofence(args.geofence)
    if not lake_client.check_auth():
        print("ERROR: Auth failed. Run: awslogin --sso", file=sys.stderr)
        sys.exit(1)

    hex_bins, samples = build_hex_bins(
        runs, geofence,
        h3_resolution=args.h3_resolution,
        sample_h3_resolution=sample_res,
        max_points_per_run=args.max_points_per_run,
        min_points_per_hex=args.min_points_per_hex,
        min_points_per_sample_hex=args.min_points_per_sample_hex,
        samples_per_sample_hex=args.samples_per_sample_hex,
        prefer_unique_runs=args.prefer_unique_runs,
    )

    write_json(Path(args.bins_output), hex_bins)
    write_json(Path(args.sample_json), samples)
    write_sample_csv(Path(args.sample_csv), samples)
    replay_urls = write_replay_urls(Path(args.urls_output), samples, print_urls=not args.no_print_urls)
    print_summary(hex_bins, samples)
    print(f"\nWrote {args.bins_output}", file=sys.stderr)
    print(f"Wrote {args.sample_json}", file=sys.stderr)
    print(f"Wrote {args.sample_csv}", file=sys.stderr)
    n_logsim_urls = sum(1 for r in replay_urls if r.get("logsim_replay_url"))
    print(f"Wrote {args.urls_output} ({n_logsim_urls} LogSim replay URLs, {len(replay_urls)} unique rows)", file=sys.stderr)
    if args.plot:
        plot_hex_bins(hex_bins, samples, geofence, Path(args.plot_output), sample_dot_size=args.sample_dot_size)


if __name__ == "__main__":
    main()
