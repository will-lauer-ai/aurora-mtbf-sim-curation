#!/usr/bin/env python3
"""Stage 5: Visualize run coverage as a high-resolution JPG.

This plot has two layers:
  1. All locations pulled / considered (background, light gray)
  2. Sampled locations / final curated runs (foreground, colored)

The goal is to make it obvious whether the curated sample lies on the
same road corridors as the broader pulled candidate set.

Usage:
    # Recommended: background all pulled runs + colored sampled runs
    python 05_plot_coverage.py \
      --all-input output/runs_raw.json \
      --input output/runs_enriched.json \
      --geofence geofences/japan_tomei.json \
      --show-gps-tracks

    # Color sampled runs by ODD area instead of vehicle
    python 05_plot_coverage.py --show-gps-tracks --color-by odd_area \
      --all-input output/runs_raw.json \
      --input output/runs_enriched.json \
      --geofence geofences/japan_tomei.json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import lake_client
from geofence import load_geofence, geofence_bbox

OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_OUTPUT = OUTPUT_DIR / "coverage_map.jpg"
DEFAULT_ALL_INPUT = OUTPUT_DIR / "runs_raw.json"

# Keep query result chunks below the API's sharp edge near 10k rows.
DEFAULT_MAX_POINTS_PER_RUN = 350
DEFAULT_BACKGROUND_MAX_POINTS_PER_RUN = 120
QUERY_CHUNK_SIZE = 20


def _chunks(items: list[str], n: int):
    for i in range(0, len(items), n):
        yield items[i : i + n]


def query_gps_tracks_batch(
    run_uuids: list[str],
    dt_floor: str = "2025-01-01",
    max_points_per_run: int = DEFAULT_MAX_POINTS_PER_RUN,
    chunk_size: int = QUERY_CHUNK_SIZE,
) -> dict[str, list[dict]]:
    """Query downsampled GPS tracks for multiple runs.

    Important: this samples *across the full run*, not just the first N
    messages. It uses ROW_NUMBER() + COUNT() to retain roughly
    max_points_per_run evenly spaced points per run.

    Returns {run_uuid: [{"lat": ..., "lon": ...}, ...]}.
    """
    tracks: dict[str, list[dict]] = {}
    if not run_uuids:
        return tracks

    for chunk_idx, chunk in enumerate(_chunks(run_uuids, chunk_size), start=1):
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
        print(
            f"  GPS query chunk {chunk_idx}: {len(chunk)} runs "
            f"(<= {max_points_per_run} pts/run)...",
            file=sys.stderr,
        )
        rows = lake_client.query(sql, limit=9999)
        for r in rows:
            uid = r["run_uuid"]
            tracks.setdefault(uid, []).append({"lat": r["lat"], "lon": r["lon"]})

    return tracks


def make_color_map(values: list[str], plt):
    categories = sorted(set(values))
    if len(categories) <= 10:
        cmap = plt.cm.Set1
    elif len(categories) <= 20:
        cmap = plt.cm.tab20
    else:
        cmap = plt.cm.gist_ncar
    return {
        cat: cmap(i / max(len(categories), 1))
        for i, cat in enumerate(categories)
    }


def draw_geofence(ax, geofence, plt):
    min_lat, max_lat, min_lon, max_lon = geofence_bbox(geofence)
    if geofence["type"] == "bbox":
        rect = plt.Rectangle(
            (min_lon, min_lat),
            max_lon - min_lon,
            max_lat - min_lat,
            linewidth=2.5,
            edgecolor="red",
            facecolor="red",
            alpha=0.08,
            linestyle="--",
            zorder=2,
        )
        ax.add_patch(rect)
    elif geofence["type"] == "polygon":
        from matplotlib.patches import Polygon as MplPolygon

        pts = [(p["lon"], p["lat"]) for p in geofence["points"]]
        polygon = MplPolygon(
            pts,
            closed=True,
            linewidth=2.5,
            edgecolor="red",
            facecolor="red",
            alpha=0.08,
            linestyle="--",
            zorder=2,
        )
        ax.add_patch(polygon)


def plot_track_layer(
    ax,
    runs: list[dict],
    tracks: dict[str, list[dict]],
    color_for_run,
    *,
    linewidth: float,
    alpha: float,
    zorder: int,
    start_markers: bool = False,
    point_markers: bool = False,
    point_size: float = 18.0,
):
    plotted = 0
    for run in runs:
        uid = run["run_uuid"]
        track = tracks.get(uid, [])
        if len(track) < 2:
            continue
        lons = [p["lon"] for p in track if p.get("lon") is not None]
        lats = [p["lat"] for p in track if p.get("lat") is not None]
        if len(lons) < 2:
            continue
        color = color_for_run(run)
        ax.plot(lons, lats, "-", color=color, alpha=alpha, linewidth=linewidth, zorder=zorder)
        if point_markers:
            # Draw every downsampled GPS point as a visible sampled-location dot.
            # This makes spatial density / coverage much easier to inspect than
            # lines alone.
            ax.scatter(
                lons,
                lats,
                s=point_size,
                color=color,
                alpha=min(1.0, alpha + 0.05),
                edgecolors="black",
                linewidths=0.18,
                zorder=zorder + 1,
            )
        if start_markers:
            ax.plot(
                lons[0],
                lats[0],
                "*",
                color=color,
                markersize=7.0,
                markeredgecolor="black",
                markeredgewidth=0.35,
                alpha=min(1.0, alpha + 0.25),
                zorder=zorder + 2,
            )
        plotted += 1
    return plotted


def plot_point_layer(ax, runs: list[dict], color_for_run, *, alpha: float, zorder: int):
    plotted = 0
    for run in runs:
        lat = run.get("rep_lat")
        lon = run.get("rep_lon")
        if lat is None or lon is None:
            continue
        ax.plot(
            lon,
            lat,
            "o",
            color=color_for_run(run),
            markersize=6,
            markeredgecolor="black",
            markeredgewidth=0.35,
            alpha=alpha,
            zorder=zorder,
        )
        plotted += 1
    return plotted


def axis_limits_from_tracks_and_geofence(
    geofence: dict,
    all_tracks: dict[str, list[dict]],
    sampled_tracks: dict[str, list[dict]],
    sampled_runs: list[dict],
    zoom_to_geofence: bool,
) -> tuple[float, float, float, float]:
    """Return x/y axis limits as min_lon, max_lon, min_lat, max_lat."""
    gf_min_lat, gf_max_lat, gf_min_lon, gf_max_lon = geofence_bbox(geofence)

    if zoom_to_geofence:
        min_lat, max_lat, min_lon, max_lon = gf_min_lat, gf_max_lat, gf_min_lon, gf_max_lon
    else:
        lats = [gf_min_lat, gf_max_lat]
        lons = [gf_min_lon, gf_max_lon]
        for tracks in (all_tracks, sampled_tracks):
            for pts in tracks.values():
                for p in pts:
                    if p.get("lat") is not None and p.get("lon") is not None:
                        lats.append(p["lat"])
                        lons.append(p["lon"])
        for r in sampled_runs:
            if r.get("rep_lat") is not None and r.get("rep_lon") is not None:
                lats.append(r["rep_lat"])
                lons.append(r["rep_lon"])
        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)

    lat_pad = max((max_lat - min_lat) * 0.08, 0.005)
    lon_pad = max((max_lon - min_lon) * 0.08, 0.005)
    return min_lon - lon_pad, max_lon + lon_pad, min_lat - lat_pad, max_lat + lat_pad


def plot_coverage(
    sampled_runs: list[dict],
    geofence: dict,
    output_path: Path,
    *,
    all_runs: list[dict] | None = None,
    show_gps_tracks: bool = False,
    color_by: str = "vehicle",
    zoom_to_geofence: bool = True,
    max_points_per_run: int = DEFAULT_MAX_POINTS_PER_RUN,
    background_max_points_per_run: int = DEFAULT_BACKGROUND_MAX_POINTS_PER_RUN,
    sampled_point_size: float = 34.0,
) -> None:
    """Generate the coverage map JPG."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: matplotlib not installed. Run: pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    all_runs = all_runs or []
    sampled_ids = {r["run_uuid"] for r in sampled_runs}
    background_runs = [r for r in all_runs if r.get("run_uuid") not in sampled_ids]

    # --- Determine foreground coloring ---
    if color_by == "vehicle":
        color_key = lambda r: r.get("vehicle_name", "unknown")
        legend_title = "Sampled vehicles"
    elif color_by == "odd_area":
        color_key = lambda r: r.get("odd_area_name", "unknown") or "unknown"
        legend_title = "Sampled ODD areas"
    else:
        color_key = lambda r: "sampled"
        legend_title = "Sampled"

    fg_colors = make_color_map([color_key(r) for r in sampled_runs], plt)
    fg_color_for_run = lambda r: fg_colors[color_key(r)]

    # --- Query tracks if requested ---
    sampled_tracks: dict[str, list[dict]] = {}
    background_tracks: dict[str, list[dict]] = {}

    if show_gps_tracks:
        if background_runs:
            print(
                f"Querying BACKGROUND tracks for all pulled-but-not-sampled runs: "
                f"{len(background_runs)} runs...",
                file=sys.stderr,
            )
            background_tracks = query_gps_tracks_batch(
                [r["run_uuid"] for r in background_runs],
                max_points_per_run=background_max_points_per_run,
            )
            print(f"  Got background tracks for {len(background_tracks)}/{len(background_runs)} runs", file=sys.stderr)

        print(f"Querying SAMPLED tracks: {len(sampled_runs)} runs...", file=sys.stderr)
        sampled_tracks = query_gps_tracks_batch(
            [r["run_uuid"] for r in sampled_runs],
            max_points_per_run=max_points_per_run,
        )
        print(f"  Got sampled tracks for {len(sampled_tracks)}/{len(sampled_runs)} runs", file=sys.stderr)

    # --- Create plot ---
    fig, ax = plt.subplots(1, 1, figsize=(13, 10), dpi=300)
    ax.set_facecolor("#fbfbfb")

    # Background: all locations pulled / considered
    background_plotted = 0
    if show_gps_tracks and background_tracks:
        background_plotted = plot_track_layer(
            ax,
            background_runs,
            background_tracks,
            lambda _r: "#9e9e9e",
            linewidth=0.55,
            alpha=0.33,
            zorder=1,
            start_markers=False,
        )
    elif all_runs:
        # Fallback: use GPS bbox centers from any enriched/raw-like data if present.
        print("Background track mode disabled; only sampled points will be shown.", file=sys.stderr)

    # Geofence above background, below sampled foreground
    draw_geofence(ax, geofence, plt)

    # Foreground: sampled locations
    sampled_plotted = 0
    if show_gps_tracks and sampled_tracks:
        sampled_plotted = plot_track_layer(
            ax,
            sampled_runs,
            sampled_tracks,
            fg_color_for_run,
            linewidth=1.35,
            alpha=0.88,
            zorder=4,
            start_markers=True,
            point_markers=True,
            point_size=sampled_point_size,
        )
    else:
        sampled_plotted = plot_point_layer(
            ax,
            sampled_runs,
            fg_color_for_run,
            alpha=0.95,
            zorder=4,
        )

    # --- Legend ---
    handles = []
    labels = []
    if background_runs:
        bg_handle = ax.plot([], [], "-", color="#9e9e9e", linewidth=2, alpha=0.55)[0]
        handles.append(bg_handle)
        labels.append(f"All pulled locations ({len(all_runs)} runs; {background_plotted} tracks shown)")

    for cat, color in fg_colors.items():
        handle = ax.plot([], [], "-", color=color, linewidth=2.5)[0]
        handles.append(handle)
        labels.append(str(cat))

    gf_handle = ax.plot([], [], "--", color="red", linewidth=2.5)[0]
    handles.append(gf_handle)
    labels.append("Geofence")

    ax.legend(
        handles,
        labels,
        title=legend_title,
        loc="upper right",
        fontsize=8,
        title_fontsize=9,
        framealpha=0.92,
    )

    # --- Title / annotation ---
    all_for_dates = sampled_runs + background_runs
    date_min = min((r.get("log_collected_at", "")[:10] for r in all_for_dates if r.get("log_collected_at")), default="")
    date_max = max((r.get("log_collected_at", "")[:10] for r in all_for_dates if r.get("log_collected_at")), default="")
    title = (
        "Geo-Spatial Segment Curation Coverage\n"
        f"{date_min} to {date_max} • all pulled: {len(all_runs) or len(sampled_runs)} runs • "
        f"sampled: {len(sampled_runs)} runs"
    )
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Longitude", fontsize=11)
    ax.set_ylabel("Latitude", fontsize=11)
    ax.grid(True, alpha=0.25, linestyle="-")
    ax.tick_params(labelsize=8)

    note = (
        "Gray = all pulled candidate locations; colored large dots = sampled/final segment-set GPS points.\n"
        "Trajectories are downsampled evenly across each run; large colored dots show spatial sample density."
    )
    ax.text(
        0.01,
        0.01,
        note,
        transform=ax.transAxes,
        fontsize=8,
        va="bottom",
        ha="left",
        bbox=dict(boxstyle="round,pad=0.35", facecolor="white", alpha=0.86, edgecolor="#bbbbbb"),
        zorder=10,
    )

    # Axis limits
    x0, x1, y0, y1 = axis_limits_from_tracks_and_geofence(
        geofence,
        background_tracks,
        sampled_tracks,
        sampled_runs,
        zoom_to_geofence=zoom_to_geofence,
    )
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect("equal", adjustable="box")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    fig.savefig(str(output_path), format="jpg", dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    print(
        f"Wrote {output_path} ({output_path.stat().st_size // 1024} KB) • "
        f"sampled shown: {sampled_plotted} • background shown: {background_plotted}",
        file=sys.stderr,
    )


def main():
    parser = argparse.ArgumentParser(description="Stage 5: Visualize run coverage as JPG.")
    parser.add_argument(
        "--input",
        default=str(OUTPUT_DIR / "runs_enriched.json"),
        help="Sampled/final input JSON from Stage 3 (default: output/runs_enriched.json)",
    )
    parser.add_argument(
        "--all-input",
        default=str(DEFAULT_ALL_INPUT),
        help="All pulled candidate runs JSON from Stage 1, plotted as gray background (default: output/runs_raw.json)",
    )
    parser.add_argument("--geofence", required=True, help="Path to geofence JSON file")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help=f"Output JPG path (default: {DEFAULT_OUTPUT})")
    parser.add_argument(
        "--show-gps-tracks",
        action="store_true",
        help="Query and plot GPS tracks. Recommended for road/corridor visibility.",
    )
    parser.add_argument(
        "--color-by",
        choices=["vehicle", "odd_area"],
        default="vehicle",
        help="Color sampled runs by vehicle_name or odd_area_name (default: vehicle)",
    )
    parser.add_argument(
        "--full-extent",
        action="store_true",
        help="Show the full extent of all pulled tracks instead of zooming to the geofence.",
    )
    parser.add_argument(
        "--max-points-per-run",
        type=int,
        default=DEFAULT_MAX_POINTS_PER_RUN,
        help=f"Max sampled GPS points per sampled run (default: {DEFAULT_MAX_POINTS_PER_RUN})",
    )
    parser.add_argument(
        "--background-max-points-per-run",
        type=int,
        default=DEFAULT_BACKGROUND_MAX_POINTS_PER_RUN,
        help=f"Max sampled GPS points per all-pulled background run (default: {DEFAULT_BACKGROUND_MAX_POINTS_PER_RUN})",
    )
    parser.add_argument(
        "--sampled-point-size",
        type=float,
        default=34.0,
        help="Marker area for sampled GPS dots in track mode (default: 34; try 60 for very large dots)",
    )
    args = parser.parse_args()

    with open(args.input) as f:
        sampled_runs = json.load(f)
    print(f"Loaded {len(sampled_runs)} sampled runs from {args.input}", file=sys.stderr)

    all_runs = []
    all_path = Path(args.all_input) if args.all_input else None
    if all_path and all_path.exists():
        with open(all_path) as f:
            all_runs = json.load(f)
        print(f"Loaded {len(all_runs)} all-pulled candidate runs from {all_path}", file=sys.stderr)
    else:
        print("No --all-input found; plotting sampled runs only.", file=sys.stderr)

    geofence = load_geofence(args.geofence)

    if args.show_gps_tracks:
        if not lake_client.check_auth():
            print("ERROR: Auth failed. Run: awslogin --sso", file=sys.stderr)
            sys.exit(1)

    plot_coverage(
        sampled_runs,
        geofence,
        Path(args.output),
        all_runs=all_runs,
        show_gps_tracks=args.show_gps_tracks,
        color_by=args.color_by,
        zoom_to_geofence=not args.full_extent,
        max_points_per_run=args.max_points_per_run,
        background_max_points_per_run=args.background_max_points_per_run,
        sampled_point_size=args.sampled_point_size,
    )


if __name__ == "__main__":
    main()
