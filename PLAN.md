# Implementation Plan: Geo-Spatial Segment Curation Scripts

> **Goal:** Build a set of simple, standalone Python scripts that
> programmatically curate segments geo-spatially — from raw data lake
> discovery through to an exported segment set CSV with GPS, ODD status,
> and S3 paths.
>
> **Environment:** Python venv with minimal dependencies (`requests` only —
> all queries go through the frontier-data-lake HTTP API or shell out to
> `frontier_query.py`).
>
> **Project root:** `~/mtbf-curation/`

---

## Architecture Overview

```
~/mtbf-curation/
├── CONTEXT.md              # Domain knowledge (already exists)
├── PLAN.md                 # This file
├── venv/                   # Python virtual environment
├── lake_client.py           # Shared: data lake query client (Stage 0)
├── geofence.py              # Shared: bounding box / polygon helpers (Stage 0)
├── 01_discover_runs.py      # Stage 1: find candidate runs by date/vehicle
├── 02_filter_geo.py         # Stage 2: GPS filter runs by bounding box
├── 03_enrich_metadata.py    # Stage 3: add map_key, ODD, S3 paths, duration
├── 04_export_segment_set.py # Stage 4: deduplicate + write CSV segment set
├── 05_plot_coverage.py      # Stage 5: visualize run coverage as JPG
├── output/                  # Generated CSVs, JSON, plots
│   ├── runs_raw.json
│   ├── runs_geo_filtered.json
│   ├── runs_enriched.json
│   ├── segment_set.csv
│   └── coverage_map.jpg
└── geofences/               # Reusable geo definitions (JSON)
    ├── japan.tomebi.json
    └── bay_area.json
```

### Data flow

```
  [Date range + vehicles]
          │
          ▼
   01_discover_runs.py ──► runs_raw.json
          │                  (run_uuid, custom_id, vehicle, log_collected_at, duration_s)
          ▼
   02_filter_geo.py ────► runs_geo_filtered.json
          │                  (same + GPS bbox/polygon hit, representative lat/lon)
          ▼
   03_enrich_metadata.py ► runs_enriched.json
          │                  (same + map_key, s3_path, in_odd, odd_area, conversion_status)
          ▼
   04_export_segment_set.py ► segment_set.csv
          │                  (final deduplicated CSV with all columns)
          ▼
   05_plot_coverage.py ───► coverage_map.jpg
                             (scatter plot of run GPS tracks + geofence polygon)
```

Each script reads the previous stage's JSON output and writes its own.
This makes stages independently re-runnable and debuggable.

---

## Stage 0: Shared Infrastructure

### 0a. `lake_client.py` — Data Lake Query Client

A thin Python wrapper around the frontier-data-lake HTTP API. Shells out
to `frontier_query.py` (simpler, reuses all auth logic) rather than
reimplementing auth.

**Functions:**
- `query(sql, format="json", limit=50000) -> list[dict]` — run SQL, return
  rows as list of dicts. Shells out to `frontier_query.py --sql "..." --format json`.
- `query_table(sql, limit=50000) -> str` — same but returns pretty table
  string for debugging.
- `check_auth() -> bool` — run `frontier_query.py --check-auth`, return
  True if exit code 0.
- `describe(table) -> list[dict]` — wrapper for `--describe`.
- `sample(table, n=10) -> list[dict]` — wrapper for `--sample`.

**Design decisions:**
- Shell out to `frontier_query.py` (on PATH at `~/.claude/skills/frontier-data-lake/frontier_query.py`)
  rather than reimplementing JWT auth. The script handles all token
  caching and refresh.
- Parse JSON stdout into Python dicts.
- Raise a `LakeQueryError` with exit code + stderr on non-zero exit.
- No external deps beyond stdlib `subprocess` + `json`.

### 0b. `geofence.py` — Geo Helpers

Simple polygon / bounding-box utilities for filtering GPS points.

**Functions:**
- `load_geofence(path) -> dict` — load a geofence JSON file. Supports two
  formats: `{"type": "bbox", "min_lat": ..., "max_lat": ..., "min_lon": ..., "max_lon": ...}`
  or `{"type": "polygon", "points": [{"lat": ..., "lon": ...}, ...]}`.
- `point_in_bbox(lat, lon, bbox) -> bool` — bounding box containment.
- `point_in_polygon(lat, lon, polygon) -> bool` — ray-casting point-in-polygon.
- `point_in_geofence(lat, lon, geofence) -> bool` — dispatches on type.

**Design decisions:**
- Pure Python, no `shapely` dependency (keep it minimal). Ray-casting
  algorithm is ~20 lines and sufficient for this use case.
- Geofence files are simple JSON, easy to hand-edit.

---

## Stage 1: `01_discover_runs.py` — Discover Candidate Runs

**Input:** `--start-date YYYY-MM-DD --end-date YYYY-MM-DD [--vehicle truck-806]`
**Output:** `output/runs_raw.json`

**What it does:**
1. Queries `ursa_log_management.public.duration_in_seconds_view` (freshness
   within minutes) for runs in the date range, optionally filtered by
   vehicle name (hyphen convention for Postgres tables).
2. Only includes runs with `duration_s > 0` and `conversion_status = 2`
   (fully converted). Joins `log_conversions` to gate on conversion.
3. Returns: `run_uuid`, `custom_id`, `vehicle_name`, `log_collected_at`,
   `duration_s`, `duration_min`.
4. Writes to `output/runs_raw.json` as a list of dicts.

**SQL pattern:**
```sql
SELECT
  CAST(d.uuid AS varchar) AS run_uuid,
  d.custom_id,
  d.vehicle_name,
  d.log_collected_at,
  d.duration_s,
  ROUND(d.duration_s / 60.0, 1) AS duration_min
FROM ursa_log_management.public.duration_in_seconds_view d
JOIN ursa_log_management.public.log_conversions lc
  ON lc.source_run_uuid = d.uuid
WHERE d.vehicle_name = 'truck-806'          -- optional, hyphen form
  AND d.log_collected_at >= TIMESTAMP '2026-06-01 00:00:00'
  AND d.log_collected_at <  TIMESTAMP '2026-07-01 00:00:00'
  AND d.duration_s > 0
  AND lc.conversion_status = 2
ORDER BY d.log_collected_at DESC
```

**CLI:**
```
python 01_discover_runs.py --start-date 2026-06-01 --end-date 2026-06-30
python 01_discover_runs.py --start-date 2026-06-01 --end-date 2026-06-30 --vehicle truck-806
```

**Progress output:** prints to stderr: "Querying data lake...", "Found N
runs", "Wrote output/runs_raw.json".

---

## Stage 2: `02_filter_geo.py` — Filter Runs by Geographic Region

**Input:** `--input output/runs_raw.json --geofence geofences/japan_tomei.json`
**Output:** `output/runs_geo_filtered.json`

**What it does:**
1. Reads the list of runs from Stage 1.
2. For each run, queries `hudi_hms.log_messages.log__applanix_lvx_nav_proto`
   for GPS points (limited to a sample — e.g. first 100 points or
   downsampled every Nth message — to keep query volume manageable).
3. Tests each GPS point against the geofence (bbox or polygon).
4. If any point falls inside the geofence, the run is included.
5. Records the representative lat/lon (first in-geofence point) and the
   total count of in-geofence GPS points.
6. Writes filtered runs to `output/runs_geo_filtered.json`.

**SQL pattern (per run):**
```sql
SELECT
  message.latitude_deg  AS lat,
  message.longitude_deg AS lon
FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
WHERE dt >= '2025-01-01'
  AND run_uuid = '<RUN_UUID>'
ORDER BY message.timestamp_ns
LIMIT 500
```

**Optimization considerations:**
- Batch approach: query DISTINCT run_uuid from GPS table within the bbox
  first (single SQL), then intersect with Stage 1 runs. This is one query
  instead of N:
  ```sql
  SELECT DISTINCT run_uuid
  FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
  WHERE dt >= '2025-01-01'
    AND CAST(dt AS DATE) >= date_add('day', -30, CURRENT_DATE)
    AND message.latitude_deg  BETWEEN <min_lat> AND <max_lat>
    AND message.longitude_deg BETWEEN <min_lon> AND <max_lon>
  ```
  This only works for bbox, not arbitrary polygons. For polygons, we'll
  need to fetch GPS points and filter client-side. **Default: use the
  batch bbox approach when geofence type is bbox; fall back to per-run
  sampling for polygons.**
- For polygon geofences, we can still use a bbox pre-filter (compute the
  bounding box of the polygon) to narrow runs first, then do exact
  polygon containment on the candidate set.

**CLI:**
```
python 02_filter_geo.py --input output/runs_raw.json --geofence geofences/japan_tomei.json
```

**Progress output:** "Loaded N runs from runs_raw.json", "Geofence: bbox
35.0-35.5, 139.0-139.5", "Querying GPS for N runs...", "Run 5/50:
truck-806 2026-06-15... 3 GPS points in geofence", "Found 12 runs in
geofence", "Wrote output/runs_geo_filtered.json".

---

## Stage 3: `03_enrich_metadata.py` — Enrich with Map, ODD, S3 Path

**Input:** `--input output/runs_geo_filtered.json`
**Output:** `output/runs_enriched.json`

**What it does:**
Reads the geo-filtered runs and enriches each with:

1. **map_key** — extracted from `runs.custom_metadata` JSON.
2. **s3_path** — `ursa_s3_path` from `runs.custom_metadata`.
3. **conversion_status** — from `log_conversions` (already have this
   from Stage 1, but double-check).
4. **ODD status** — query `adk_state_timeline_metric` for the run to
   get `in_odd`, `odd_area_name`, and autonomous time within the geofence.
5. **GPS bounding box** — min/max lat/lon of the run's trajectory (for
   quick sanity-checking).

**SQL patterns:**

*Run metadata (single query for all runs):*
```sql
SELECT
  CAST(uuid AS varchar) AS run_uuid,
  json_extract_scalar(custom_metadata, '$.map_key')      AS map_key,
  json_extract_scalar(custom_metadata, '$.ursa_s3_path')   AS s3_path,
  json_extract_scalar(custom_metadata, '$.log_version')   AS log_version
FROM ursa_log_management.public.runs
WHERE uuid IN (CAST('<UUID1>' AS uuid), CAST('<UUID2>' AS uuid), ...)
```

*ODD status (per run, from ADK state timeline):*
```sql
SELECT
  element_at(metric_value.master_odd, 'in_odd')       AS in_odd,
  element_at(metric_value.master_odd, 'odd_area_name') AS odd_area_name,
  metric_value.adk_state_name                          AS adk_state,
  ROUND(metric_value.duration_ns / 1e9, 1)             AS duration_s
FROM hudi_hms.ursa_metric.adk_state_timeline_metric
WHERE dt >= '2025-01-01'
  AND run_uuid = '<RUN_UUID>'
  AND metric_value.adk_state_name = 'ACTIVE'
  AND element_at(metric_value.master_odd, 'in_odd') = 'true'
ORDER BY metric_value.start_timestamp_ns
```

*GPS bounding box (per run):*
```sql
SELECT
  MIN(message.latitude_deg)  AS min_lat,
  MAX(message.latitude_deg)  AS max_lat,
  MIN(message.longitude_deg) AS min_lon,
  MAX(message.longitude_deg) AS max_lon
FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
WHERE dt >= '2025-01-01'
  AND run_uuid = '<RUN_UUID>'
```

**Output schema per run:**
```json
{
  "run_uuid": "019f...",
  "custom_id": "2026-06-15_05-53-51_truck-806",
  "vehicle_name": "truck-806",
  "log_collected_at": "2026-06-15T05:53:51.000000",
  "duration_s": 1832,
  "duration_min": 30.5,
  "rep_lat": 35.123,
  "rep_lon": 139.456,
  "gps_points_in_geofence": 3,
  "map_key": "jp_zone_54@c376b3c",
  "s3_path": "s3://ursa-frontier-prod-raw-logs/LogSim/...",
  "log_version": "v2.3.1",
  "in_odd": true,
  "odd_area_name": "Shin-Tomei EB",
  "autonomous_time_in_odd_s": 1234.5,
  "gps_bbox": {"min_lat": 35.01, "max_lat": 35.20, "min_lon": 139.30, "max_lon": 139.60}
}
```

**CLI:**
```
python 03_enrich_metadata.py --input output/runs_geo_filtered.json
```

**Progress output:** "Loaded 12 runs", "Enriching run metadata (1 query
for all)...", "Querying ODD status per run: 3/12...", "Querying GPS
bounds per run: 3/12...", "Wrote output/runs_enriched.json".

---

## Stage 4: `04_export_segment_set.py` — Export Final Segment Set

**Input:** `--input output/runs_enriched.json [--output output/segment_set.csv]`
**Output:** `output/segment_set.csv` (and optionally `.json`)

**What it does:**
1. Reads enriched runs.
2. Deduplicates by `run_uuid` (safety net — Stage 1 may have dups if
   runs appear in multiple assembly groups).
3. Sorts by `log_collected_at`.
4. Writes a CSV with a clean column layout:

| Column | Source |
|--------|--------|
| `run_uuid` | Stage 1 |
| `custom_id` | Stage 1 |
| `vehicle_name` | Stage 1 |
| `log_collected_at` | Stage 1 |
| `duration_min` | Stage 1 |
| `rep_lat` | Stage 2 |
| `rep_lon` | Stage 2 |
| `gps_points_in_geofence` | Stage 2 |
| `map_key` | Stage 3 |
| `s3_path` | Stage 3 |
| `in_odd` | Stage 3 |
| `odd_area_name` | Stage 3 |
| `autonomous_time_in_odd_s` | Stage 3 |
| `gps_min_lat` | Stage 3 |
| `gps_max_lat` | Stage 3 |
| `gps_min_lon` | Stage 3 |
| `gps_max_lon` | Stage 3 |

5. Prints a summary to stderr: total runs, unique vehicles, date range,
   ODD coverage, map_key distribution.

**CLI:**
```
python 04_export_segment_set.py --input output/runs_enriched.json
python 04_export_segment_set.py --input output/runs_enriched.json --output custom.csv
```

**Summary output:**
```
Segment Set Export Summary
==========================
Total segments:        12
Unique vehicles:       3 (truck-806, truck-810, truck-814)
Date range:            2026-06-01 to 2026-06-28
In-ODD:                10/12 (83%)
ODD areas:             Shin-Tomei EB (8), Tomei Expy (2), unknown (2)
Map keys:              jp_zone_54@c376b3c (10), jp_zone_55@a1b2c3 (2)
Total autonomous time: 4.2 hours in-ODD
Wrote: output/segment_set.csv (12 rows, 16 columns)
```

---

## Execution Order

```bash
# 0. Setup
cd ~/mtbf-curation
python -m venv venv
source venv/bin/activate
pip install matplotlib  # only dep beyond stdlib (for Stage 5 visualization)

# 1. Discover runs
python 01_discover_runs.py --start-date 2026-06-01 --end-date 2026-06-30

# 2. Filter by geofence
python 02_filter_geo.py --input output/runs_raw.json --geofence geofences/japan_tomei.json

# 3. Enrich metadata
python 03_enrich_metadata.py --input output/runs_geo_filtered.json

# 4. Export segment set
python 04_export_segment_set.py --input output/runs_enriched.json

# 5. Visualize coverage
python 05_plot_coverage.py --input output/runs_enriched.json --geofence geofences/japan_tomei.json --show-gps-tracks
```

Each stage can be re-run independently if parameters change (e.g., try a
different geofence by re-running Stages 2–4 without re-querying Stage 1).

---

## Design Principles

1. **One script per stage** — each script does one thing, reads the
   previous output, writes its own. No monolithic pipeline.

2. **JSON between stages** — human-readable, debuggable, easy to
   inspect or hand-edit between stages.

3. **Minimal dependencies** — Python stdlib (`subprocess`, `json`,
   `csv`, `argparse`, `pathlib`) plus `matplotlib` for Stage 5
   visualization. The frontier-data-lake skill handles all auth and HTTP;
   we shell out to it.

4. **Progress to stderr** — all progress/diagnostic messages go to
   stderr; stdout stays clean for piping.

5. **Idempotent** — re-running a stage overwrites its output. No
   hidden state.

6. **SQL is inline** — each script contains its SQL as string constants
   with `.format()` or f-strings for parameterization. No external SQL
   files to manage.

7. **Gotcha-aware** — the SQL in each script follows the data lake
   gotchas: dt partition floor, vehicle name conventions, cast Postgres
   side down to varchar, avoid `trucking_dashboard_runs.datetime_utc`.

---

## Stage Dependencies and Risks

| Risk | Mitigation |
|------|------------|
| GPS table (`log__applanix_lvx_nav_proto`) lag (~1 day) | Stage 2 warns if date range includes last 24h; suggest using `adk_state_timeline_metric` lat/lon as fallback |
| Large number of runs → many per-run queries in Stage 2/3 | Batch SQL where possible (single query for all run_uuids); limit GPS samples to 500/run |
| Polygon geofence requires client-side filtering | Pre-filter with bbox of polygon, then exact containment on subset |
| `frontier_query.py` not on PATH | `lake_client.py` resolves path: checks `~/.claude/skills/frontier-data-lake/frontier_query.py` as fallback |
| Auth token expired (exit code 3) | `lake_client.py` detects exit code 3, prints clear "Run `awslogin --sso`" message |
| Some runs may not have GPS data | Stage 2 logs runs with 0 GPS points as "no GPS" and excludes them |
| matplotlib not installed for Stage 5 | Script checks import, prints `pip install matplotlib` hint and exits gracefully |

---

## Stage 5: `05_plot_coverage.py` — Visualize Run Coverage as JPG

**Input:** `--input output/runs_enriched.json --geofence geofences/japan_tomei.json [--output output/coverage_map.jpg]`
**Output:** `output/coverage_map.jpg`

**What it does:**
1. Reads enriched runs (which include representative lat/lon and GPS
   bounding boxes from Stage 3).
2. For richer visualization, optionally re-queries the data lake for
   full GPS tracks of each run in the segment set.
3. Generates a static matplotlib scatter/line plot showing:
   - **GPS tracks** of all runs as thin colored lines (one color per
     vehicle, or colored by `odd_area_name`).
   - **Geofence polygon** drawn as a semi-transparent filled overlay with
     a dashed border.
   - **Run start points** (first GPS point) as larger markers.
   - **Axes** labeled with lat/lon, grid lines, and a title showing the
     date range and run count.
   - **Legend** mapping colors to vehicles or ODD areas.
4. Saves as a high-resolution JPG (300 DPI, sized for screen or print).

**Plot layout:**
```
┌─────────────────────────────────────────┐
│  Segment Coverage Map                   │
│  2026-06-01 to 2026-06-30 • 12 runs     │
│                                         │
│    ┌─── dashed border (geofence) ───┐   │
│    │  ······  ─────  ······         │   │
│    │    ────────  ──                │   │
│    │  ··  ──────  ·····             │   │
│    │      ──  ──────  ··            │   │
│    └────────────────────────────────┘   │
│                                         │
│  Legend: ● truck-806  ● truck-810  ...  │
│  Longitude →                           │
└─────────────────────────────────────────┘
```

**GPS track query (per run, cached):**
```sql
SELECT
  message.latitude_deg  AS lat,
  message.longitude_deg AS lon
FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
WHERE dt >= '2025-01-01'
  AND run_uuid = '<RUN_UUID>'
ORDER BY message.timestamp_ns
```

**Design decisions:**
- Use `matplotlib` (not folium) — produces a static JPG, no web server
  needed, works in headless environments. This is the one pip dependency
  beyond stdlib.
- Color by vehicle by default; `--color-by odd_area` switches to ODD-area
  coloring.
- `--show-gps-tracks` flag: if set, queries full GPS tracks from the
  lake (slower, N queries). If not set, plots only the representative
  point (rep_lat/rep_lon from Stage 2) as a scatter.
- Downsample GPS tracks to max 500 points/run for plotting (stride sample).
- Figure size 12x10 inches, 300 DPI -> ~3600x3000 px JPG.
- If GPS tracks are not available (or `--show-gps-tracks` not set), fall
  back to a scatter of `rep_lat`/`rep_lon` with marker size proportional
  to `gps_points_in_geofence`.

**CLI:**
```
# Simple scatter of representative points (fast, no extra queries)
python 05_plot_coverage.py --input output/runs_enriched.json --geofence geofences/japan_tomei.json

# Full GPS tracks (slower, queries lake per run)
python 05_plot_coverage.py --input output/runs_enriched.json --geofence geofences/japan_tomei.json --show-gps-tracks

# Color by ODD area instead of vehicle
python 05_plot_coverage.py --input output/runs_enriched.json --geofence geofences/japan_tomei.json --show-gps-tracks --color-by odd_area
```

**Progress output:** "Loaded 12 runs", "Plotting 12 representative
points...", or "Querying GPS tracks: 3/12...", "Rendering map...",
"Wrote output/coverage_map.jpg (3600x3000)".

---

## Future Extensions (out of scope for initial build)

- **ODD polygon import**: Load ODD polygons from the ODD tool's API
  instead of hand-defined geofence files.
- **Segment-level granularity**: Current scripts work at the run level;
  a future stage could split runs into sub-segments using
  `ursa_metric_segment_flatten` for finer-grained geo-curation.
- **Lance export**: Write directly to a Lance dataset instead of CSV.
- **Ursa SDK integration**: For resolving `adp_uuid` (the one thing SQL
  can't do), add an optional `--use-ursa-sdk` flag to Stage 3.
