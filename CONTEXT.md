# Geo-Spatial Segment Curation — Project Context

> **Purpose:** Curate autonomous-driving segments (LogSim / drive logs)
> geo-spatially — filter by geographic region, ODD polygon, or route,
> validate coverage, and export segment sets for downstream sim, training,
> or map generation.

---

## 1. Where Segments Live

### 1.1 S3 Storage

Raw run data is stored in S3 under `LogSim/` prefixes:

| Bucket                          | Purpose                         |
|---------------------------------|---------------------------------|
| `ursa-frontier-prod-raw-logs`   | Frontier (trucking) raw logs    |
| `ursa-neuron-prod-raw-logs`     | Neuron raw logs                 |

Path pattern:
```
s3://ursa-frontier-prod-raw-logs/LogSim/<year>/<month>/<day>/<adp_uuid>/
    merged_0.mcap
    merged_1.mcap
    ...
    metadata.yaml
```

Upload manifests drive ingestion:
```
.manifests/<date>/<uuid>-LogSim-<name>.pb
```
A scanner processes these and kicks off post-upload / log-conversion Flyte
workflows. The manifest's `raw_data_uri` points to the S3 URI under `LogSim/`
with `sim_type: "LogSim"` and a `log_sim_source_segment` containing
`run_uuid`, start/end timestamps, and `run_custom_id`.

### 1.2 Ursa (Log/Run Management Service)

Every run/segment is tracked in Ursa with:
- **`run_uuid`** — canonical Ursa ID (UUIDv7) for a drive/sim run.
- **`custom_id` / `adp_uuid`** — human-readable identifier tied to the ADP
  sim run (format: `<date>_<time>_truck-<id>`).
- **`log_sim_source_segment`** — `{run_uuid, start_time, end_time}` pointing
  to the parent drive run and time window a LogSim segment was sourced from.
  This is what geo-locates the segment, since GPS/pose only exists on the
  parent drive log.
- **`custom_metadata.map_key`** — identifies which map (e.g.
  `jp_zone_54@c376b3c`) the segment/run corresponds to.

### 1.3 Querying Run Metadata via SQL (Data Lake)

Instead of calling the Ursa gRPC SDK directly, use the
**frontier-data-lake skill** (`frontier_query.py`) to query the Trino
federated lakehouse. Key tables:

| Table | What it gives you | Freshness |
|-------|-------------------|-----------|
| `ursa_log_management.public.runs` | Run catalog: `uuid`, `custom_id`, `vehicle_name`, `log_collected_at`, `custom_metadata` (JSON with `map_key`, `sim_status`, `ursa_s3_path`, etc.) | Seconds–minutes |
| `ursa_log_management.public.duration_in_seconds_view` | Per-run duration in seconds | Minutes |
| `ursa_log_management.public.drive_run_infos` | Drive assembly: `custom_id`, `data_explorer_uuid`, `run_uuid` | Minutes |
| `ursa_log_management.public.log_conversions` | Conversion status (`conversion_status = 2` = converted) | Minutes |
| `hudi_hms.log_messages.log__applanix_lvx_nav_proto` | Per-message GPS (`message.latitude_deg`, `message.longitude_deg`, `message.heading_deg`, `message.total_speed`) from onboard Applanix | ~1 day |
| `hudi_hms.ursa_metric.ego_state_metric` | KPI metrics (autonomous meters/hours driven) | ~1 day |
| `hudi_hms.ursa_metric.ursa_geo_tags_segment_metric` | Scene/geo tags per segment | ~1 day |
| `hudi_hms.ursa_metric.ursa_metric_segment_flatten` | Wide segment-level metrics (speed, solar angle, etc.) | ~6 days |
| `hudi_hms.ursa_metric.adk_state_timeline_metric` | ADK state transitions with `start_latitude_deg` / `start_longitude_deg` and `master_odd` (in_odd, odd_area_name) | ~1 day |
| `hudi_hms.ursa_metric.disengagement_event_metric` | Raw disengagement events with lat/lon | ~1 day |
| `iceberg.custom_dataset.sds_trucking_disengagements` | Enriched disengagements with ODD/route/stack metadata | ~1 day |
| `iceberg.iceberg.run_info` | ADP sim-run registry mirror (observer verdicts, scenario tags) | p50 ~15–20 min |

> **Critical gotchas for the data lake (see SKILL.md for full list):**
> - **Vehicle naming:** Postgres ops tables use hyphens (`truck-806`);
>   lakehouse ETL tables use underscores (`truck_806`). Always run
>   `frontier_query.py --vehicles` first.
> - **dt partition floor:** Every Hudi table needs `dt >= '2025-01-01'`
>   (literal string) before any `CAST(dt AS DATE)` predicate, or results
>   are silently empty.
> - **Cross-catalog joins:** Cast the Postgres side DOWN to varchar
>   (`CAST(r.uuid AS varchar) = h.run_uuid`), never the Hudi side up —
>   some `run_uuid` values are non-UUID strings and will throw
>   `INVALID_CAST_ARGUMENT`.
> - **`trucking_dashboard_runs.datetime_utc`** is garbage epoch values;
>   use `runs.log_collected_at` for wall-clock time.
> - Curly braces in SQL must be doubled (`{{3}}` not `{3}`) due to a
>   server-side `str.format()` layer.

### 1.4 GPS / Lat-Lon Per Segment

GPS/pose comes from onboard Applanix / `swiftnav_gps_output` topics recorded
in the log — it is NOT baked into the LogSim manifest itself.

**Ways to get lat/lon:**

1. **Data Lake SQL** — query
   `hudi_hms.log_messages.log__applanix_lvx_nav_proto` for per-message GPS
   (note: fields are `message.latitude_deg` / `message.longitude_deg`):
   ```sql
   SELECT message.timestamp_ns  AS ts,
          message.latitude_deg   AS lat,
          message.longitude_deg  AS lon,
          message.heading_deg    AS heading
   FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
   WHERE dt >= '2025-01-01'
     AND run_uuid = '<RUN_UUID>'
   ORDER BY message.timestamp_ns
   ```

2. **One GPS sample per run** (for fleet-wide scatterplots):
   ```sql
   SELECT run_uuid, lat, lon
   FROM (
     SELECT run_uuid,
            message.latitude_deg   AS lat,
            message.longitude_deg  AS lon,
            ROW_NUMBER() OVER (PARTITION BY run_uuid ORDER BY message.timestamp_ns) AS rn
     FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
     WHERE dt >= '2025-01-01'
       AND CAST(dt AS DATE) >= date_add('day', -7, CURRENT_DATE)
   )
   WHERE rn = 1
   LIMIT 500
   ```

3. **ADK state timeline** — `adk_state_timeline_metric` carries
   `start_latitude_deg` / `start_longitude_deg` per state interval, plus
   `master_odd` with `in_odd` and `odd_area_name`.

4. **Data Engine / Segment Library UI** — has a native "GEO" channel showing
   ego trajectory per segment, plus geo-polygon filters.

> **Caveat:** The legacy `derived_geo_output` table (populated by
> `index_gps_output_workflow.py`) had reliability issues. Prefer the
Segment Library's native GEO channel, `localization_coordinate_metric`, or
> direct `log__applanix_lvx_nav_proto` queries instead.

---

## 2. Tools for Geo-Spatial Curation

### 2.1 Data Engine 2.0 (formerly "Data Explorer")

**Primary UI:** https://neuron.oci.applied.dev/data_explorer/v2/library
(also `/overview`, `/collections`)

Capabilities:
- Filter by: segment_set, log_uuid, channel, tag, geo-polygon, NLS
  (natural-language search) query
- View geo-distribution heatmaps
- Draw geo-polygon filters (path / within-shape queries)
- Materialize and export Segment Sets (CSV or Lance dataset)
- Native "GEO" channel showing ego trajectory per segment

Legacy "Library/Events/Collections/Datasets" pages are being deprecated in
favor of Data Engine 2.0 + Vizkit for viewing individual drives.

**Programmatic curation pattern:**
> "We specify the date range (start_date, end_date) and geofence, it will
> select all the trips that go over the geofence in that date range (and
> remove the duplicated segments)" — this is how teams pull segments for a
> geographic patch (e.g. for map generation via multi-traversal SLAM).

### 2.2 Segment Set Registration from Lance

```bash
bazel run //ml/rl/scenarionet/tools:cli -- register \
    --dataset-uri s3://... \
    --name ... \
    --owner ...
```

### 2.3 Ursa Python SDK (gRPC)

For operations not available via SQL (notably resolving the ursa `run_uuid`
↔ ADP `adp_uuid` bridge):

```python
from ursa.public.sdk.python import log_management
run = log_management.DescribeRun("<ursa_run_uuid_or_custom_id>")
# Returns: raw_data_uri, duration, sim_run_info.log_sim_source_segment
#          (parent run_uuid + start/end times), custom_metadata fields
#          (map_key, log_version, dashboard_url, ursa_logs_url, etc.)
# Crucially: run.sim_run_info.adp_uuid is the ONLY bridge to ADP observer results.
```

**Prerequisites:**
- Bazel-vendored SDK in `~/core-stack`
- `URSA_SDK_GRPC_HOSTNAME=grpc.frontier.prod.applied.dev`
- `URSA_SDK_GRPC_AUTH_TOKEN` (from `ursa-refresh` or AWS Secrets Manager
  via `ensure_auth_token()`)

### 2.4 frontier-data-lake Skill

Location: `~/.claude/skills/frontier-data-lake/`

Tool: `frontier_query.py` — a `uv`-managed self-contained script that
authenticates via AWS SSO (profile `frontier`) and queries the Trino
federated lakehouse at
`https://api.frontier.prod.applied.dev/api/v2/query_sql`.

**Quick start:**
```bash
frontier_query.py --check-auth                    # verify auth
frontier_query.py --vehicles --format table        # list vehicles (run first!)
frontier_query.py --catalogs                       # discover catalogs
frontier_query.py --tables hudi_hms.ursa_metric   # list tables
frontier_query.py --describe <table>              # inspect schema
frontier_query.py --sample <table>                # 10-row sample
frontier_query.py --sql "SELECT ..."              # arbitrary SQL
frontier_query.py --format table --sql "..."      # pretty table output
```

**Auth:** Run `awslogin --sso` (AWS profile `frontier`) before first use.
Token is cached for 50 min in `~/.cache/frontier-data-lake/token`.

**Key data lake tables for geo-spatial work:**

| Table | Geo relevance |
|-------|----------------|
| `hudi_hms.log_messages.log__applanix_lvx_nav_proto` | Per-message GPS (`message.latitude_deg`, `message.longitude_deg`, `message.heading_deg`) — the primary source for ego trajectory |
| `hudi_hms.ursa_metric.adk_state_timeline_metric` | ADK state intervals with `start_latitude_deg`/`start_longitude_deg` and `master_odd` (in_odd, odd_area_name) |
| `hudi_hms.ursa_metric.ursa_geo_tags_segment_metric` | Scene/geo tags per segment |
| `hudi_hms.ursa_metric.disengagement_event_metric` | Disengagement events with lat/lon |
| `iceberg.custom_dataset.sds_trucking_disengagements` | Enriched disengagements with ODD/route metadata |
| `ursa_log_management.public.runs` | Run catalog with `custom_metadata` (contains `map_key`) |

---

## 3. Map & ODD Information

### 3.1 Maps

Maps are versioned/keyed objects identified by `map_key` (e.g.
`jp_zone_54@c376b3c`). Viewable/editable in Map Toolset:
https://neuron.maps.oci.applied.dev/map_toolset/...

The `map_key` is stored in `runs.custom_metadata` and can be extracted via
SQL:
```sql
SELECT json_extract_scalar(custom_metadata, '$.map_key') AS map_key
FROM ursa_log_management.public.runs
WHERE uuid = CAST('<UUID>' AS uuid)
```

### 3.2 ODD (Operational Design Domain)

ODD is defined as specific routes/lane-groups over a map (e.g. Tomei Expy
normal lanes but not toll/express lanes).

**Key facts:**
- ODD route definitions and actual map content don't always match — teams
  have had incidents where drivers/collection runs deviated from the
  defined ODD route, and where map content included lanes/routes outside
  the intended ODD.
- **Always verify** a segment's geo position is genuinely inside the
  intended ODD polygon, not just "on the map."

**ODD visualization tool:**
https://trucking2027odd.experimental.apps.applied.dev
- Visualizes shoulder width, driverless-vs-safety-driver segments, and
  MRM-relevant road properties along the ODD.

**ODD data in the lake:**
- `adk_state_timeline_metric.metric_value.master_odd` — a map with keys
  `in_odd` (`'true'`/`'false'`) and `odd_area_name` (e.g.
  `'Shin-Tomei EB'`).
- `sds_trucking_disengagements` carries `odd_area_type`, `odd_area_name`,
  and `route` columns.

### 3.3 Region Bounding Boxes

Region bounding boxes are sometimes hardcoded for ETL/geo filters. Known
regions: SF, Bay Area, Stuttgart, Michigan, LA, Japan. Used to bucket
drives by geographic area.

---

## 4. Practical Workflow

### Step 1: Define the geofence/ODD polygon (or route) and date range

Identify the geographic region of interest as a polygon or route, and the
time window for data collection.

### Step 2: Query/curate segments

**Option A — Data Engine UI:**
1. Open Data Engine 2.0 Segment Library.
2. Filter by geo-polygon, tags (highway/urban/tunnel), and/or existing
   segment sets.
3. Use the GEO channel to sanity-check trajectory coverage.
4. View heatmap/geo-distribution.

**Option B — Data Lake SQL:**
1. Query `log__applanix_lvx_nav_proto` for GPS tracks of candidate runs.
2. Filter runs by date range and vehicle.
3. Apply bounding-box or polygon filtering on lat/lon.
4. Cross-reference with `adk_state_timeline_metric.master_odd` to confirm
   in-ODD status.

```sql
-- Example: find runs with GPS points inside a bounding box in the last 7 days
WITH runs_in_box AS (
  SELECT DISTINCT run_uuid
  FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto
  WHERE dt >= '2025-01-01'
    AND CAST(dt AS DATE) >= date_add('day', -7, CURRENT_DATE)
    AND message.latitude_deg  BETWEEN 35.0 AND 35.5
    AND message.longitude_deg BETWEEN 139.0 AND 139.5
)
SELECT r.uuid, r.custom_id, r.vehicle_name, r.log_collected_at
FROM runs_in_box b
JOIN ursa_log_management.public.runs r
  ON CAST(r.uuid AS varchar) = b.run_uuid
ORDER BY r.log_collected_at DESC
```

### Step 3: Export the Segment Set

Export from Data Engine as CSV or registered Lance dataset. This gives you
`run_uuid`/`custom_id` + start/end time per segment.

For Lance registration:
```bash
bazel run //ml/rl/scenarionet/tools:cli -- register \
    --dataset-uri s3://... \
    --name <segment_set_name> \
    --owner <owner>
```

### Step 4: Pull underlying LogSim/drive data from S3

For each segment, resolve the exact `raw_data_uri`:
- Via SQL: extract `ursa_s3_path` from `runs.custom_metadata`:
  ```sql
  SELECT json_extract_scalar(custom_metadata, '$.ursa_s3_path') AS s3_path
  FROM ursa_log_management.public.runs
  WHERE uuid = CAST('<UUID>' AS uuid)
  ```
- Via Ursa SDK: `log_management.DescribeRun(uuid_or_custom_id)` returns
  `raw_data_uri` directly.
- Or download directly if the S3 path is already known.

### Step 5: Cross-reference map_key / geofence bounds

Associate each segment with the correct map tile/version for ODD sampling
analysis:
```sql
SELECT
  CAST(uuid AS varchar) AS run_uuid,
  custom_id,
  vehicle_name,
  log_collected_at,
  json_extract_scalar(custom_metadata, '$.map_key') AS map_key
FROM ursa_log_management.public.runs
WHERE uuid IN (CAST('<UUID1>' AS uuid), CAST('<UUID2>' AS uuid), ...)
```

---

## 5. Caveats

1. **Tooling churn:** Geo-curation tooling is actively evolving (Data
   Engine 2.0 migration, saved-query vs segment-set semantics still being
   debated). Expect some UI/URL churn.

2. **Geo output gaps:** `derived_geo_output` ingestion is known to have
   gaps. Prefer the Segment Library's native GEO channel,
   `localization_coordinate_metric`, or direct
   `log__applanix_lvx_nav_proto` queries if lat/lon looks missing.

3. **ODD vs. map mismatch:** ODD route definitions and actual map content
   don't always match. Verify a segment's geo position is genuinely inside
   the intended ODD polygon, not just "on the map."

4. **Vehicle naming duality:** Postgres ops tables use hyphens
   (`truck-806`); lakehouse ETL tables use underscores (`truck_806`).
   Always run `frontier_query.py --vehicles` first.

5. **ETL lag:** Dashboard ETL table (`trucking_dashboard_runs`) lags ops
   `runs` table by ~24h. Use `runs`/`duration_in_seconds_view` for
   freshness-sensitive queries.

6. **Sim vs. drive UUID bridge:** The ursa `run_uuid` ↔ ADP `adp_uuid`
   mapping is only available via the Ursa gRPC SDK's `DescribeRun`, not
   via SQL. This is the sole reason to use the SDK over the data lake.

7. **`dt` partition floor:** Always include literal `dt >= '2025-01-01'`
   on Hudi tables before dynamic date predicates, or results are silently
   empty.

8. **ODD area skew:** `odd_area_name` is heavily skewed — Shin-Tomei EB
   alone is ~74% of in-ODD intervals. Always stratify by ODD area and set
   minimum-exposure thresholds before reporting rates.

---

## 6. Key URLs

| Resource | URL |
|----------|-----|
| Data Engine 2.0 (Segment Library) | https://neuron.oci.applied.dev/data_explorer/v2/library |
| Data Engine 2.0 (Overview) | https://neuron.oci.applied.dev/data_explorer/v2/overview |
| Data Engine 2.0 (Collections) | https://neuron.oci.applied.dev/data_explorer/v2/collections |
| Map Toolset | https://neuron.maps.oci.applied.dev/map_toolset/... |
| ODD Coverage Tool | https://trucking2027odd.experimental.apps.applied.dev |
| Frontier Query API | https://api.frontier.prod.applied.dev/api/v2/query_sql |
| Ursa gRPC | `grpc.frontier.prod.applied.dev` |

---

## 7. Frontier Data Lake Skill — Quick Reference

**Location:** `~/.claude/skills/frontier-data-lake/`

**Key files:**
| File | Purpose |
|------|---------|
| `SKILL.md` | Full skill documentation with 15 gotchas |
| `frontier_query.py` | The CLI tool (uv-managed self-contained script) |
| `examples.md` | Curated query gallery |

**Auth flow:**
1. `awslogin --sso` (AWS profile `frontier`)
2. Script fetches `frontier-machine-auth-secrets` from AWS Secrets Manager
3. Calls `accounts.applied.co/api/machineCredential/get` to mint JWT
4. Caches for 50 min in `~/.cache/frontier-data-lake/token`

**Exit codes:**
| Code | Meaning |
|------|---------|
| 0 | Success |
| 2 | Bad CLI args |
| 3 | Auth error (run `awslogin --sso`) |
| 4 | Server-side query error |

**Most useful commands for geo-spatial work:**
```bash
# Orientation
frontier_query.py --check-auth
frontier_query.py --vehicles --format table

# Discover GPS table schema
frontier_query.py --describe hudi_hms.log_messages.log__applanix_lvx_nav_proto

# GPS track for a single run
frontier_query.py --sql "SELECT message.timestamp_ns AS ts, message.latitude_deg AS lat, message.longitude_deg AS lon FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto WHERE dt >= '2025-01-01' AND run_uuid = '<UUID>' ORDER BY message.timestamp_ns"

# One GPS sample per run (fleet scatterplot)
frontier_query.py --sql "SELECT run_uuid, lat, lon FROM (SELECT run_uuid, message.latitude_deg AS lat, message.longitude_deg AS lon, ROW_NUMBER() OVER (PARTITION BY run_uuid ORDER BY message.timestamp_ns) AS rn FROM hudi_hms.log_messages.log__applanix_lvx_nav_proto WHERE dt >= '2025-01-01' AND CAST(dt AS DATE) >= date_add('day', -7, CURRENT_DATE)) WHERE rn = 1 LIMIT 500"

# Run metadata with map_key
frontier_query.py --sql "SELECT CAST(uuid AS varchar) AS run_uuid, custom_id, vehicle_name, log_collected_at, json_extract_scalar(custom_metadata, '$.map_key') AS map_key, json_extract_scalar(custom_metadata, '$.ursa_s3_path') AS s3_path FROM ursa_log_management.public.runs WHERE custom_id LIKE '2026-04-%' ORDER BY log_collected_at DESC LIMIT 25"

# ODD status per ADK state interval
frontier_query.py --sql "SELECT dt, run_uuid, element_at(metric_value.master_odd, 'odd_area_name') AS odd_area, metric_value.adk_state_name, ROUND(metric_value.duration_ns / 1e9, 2) AS duration_s, metric_value.start_latitude_deg AS lat, metric_value.start_longitude_deg AS lon FROM hudi_hms.ursa_metric.adk_state_timeline_metric WHERE dt >= '2026-04-20' AND element_at(metric_value.master_odd, 'in_odd') = 'true' ORDER BY metric_value.start_timestamp_ns DESC LIMIT 50"
```

---

## 8. Data Lake Catalog Reference

| Catalog | Backend | What's here |
|---------|---------|-------------|
| `hudi_hms` | Hudi on S3 + Hive Metastore | Lakehouse: metrics, MLDS frames, log-message views, custom datasets |
| `iceberg` | Iceberg on S3 | ADP Query Engine native tables (`run_info`, `per_tick_stats_*`) |
| `ursa_log_management` | Postgres | Raw-log/drive catalog: `runs`, `drive_run_infos`, `log_conversions`, durations |
| `postgresql-legacy` | Postgres | ADP sim-run source of truth (`public.run_info`, tags). Quote catalog name: `"postgresql-legacy".public.run_info` |
| `dora_triage` | Postgres | DORA triage object store |
| `label_manager` | Postgres | Labeling tasks, ontologies |

Key schemas in `hudi_hms`:
| Schema | Contents |
|--------|----------|
| `ursa_metric` | All Ursa metric pipelines (ego_state, geo_tags, disengagement, adk_state_timeline, segment_flatten) |
| `ursa_lake` | MLDS: frames, annotations, segmentation, tracking |
| `log_messages` | Per-topic flattened proto views (`log__applanix_lvx_nav_proto`, `log__<can_signal>_proto`, etc.) |
| `iceberg` | Mirror of native Iceberg sim/observer data |
| `custom_dataset` | Tables written from Validation Toolset Insights notebooks |
