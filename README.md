# Aurora MTBF Sim Curation

Tools for geo-spatially curating Frontier/Ursa driving runs and LogSim source
segments. The pipeline discovers converted runs, filters them by geofence,
enriches them with metadata, exports CSV segment sets, visualizes coverage, and
supports density-aware H3 hexagonal sampling.

## Setup

```bash
cd ~/mtbf-curation
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

The data lake query wrapper shells out to the Frontier data-lake skill:

```bash
~/.claude/skills/frontier-data-lake/frontier_query.py
```

Before querying, authenticate:

```bash
awslogin --sso
```

## Pipeline

### 1. Discover converted runs

```bash
python 01_discover_runs.py \
  --start-date 2026-06-27 \
  --end-date 2026-06-28
```

Output:

```text
output/runs_raw.json
```

### 2. Filter candidate runs by geofence

```bash
python 02_filter_geo.py \
  --input output/runs_raw.json \
  --geofence geofences/japan_tomei.json
```

Output:

```text
output/runs_geo_filtered.json
```

### 3. Enrich with map, ODD, S3 path, GPS bounds

```bash
python 03_enrich_metadata.py \
  --input output/runs_geo_filtered.json
```

Output:

```text
output/runs_enriched.json
```

### 4. Export segment set CSV

```bash
python 04_export_segment_set.py \
  --input output/runs_enriched.json
```

Output:

```text
output/segment_set.csv
```

### 5. Plot coverage map

```bash
python 05_plot_coverage.py \
  --all-input output/runs_raw.json \
  --input output/runs_enriched.json \
  --geofence geofences/japan_tomei.json \
  --show-gps-tracks \
  --sampled-point-size 60 \
  --output output/coverage_map_large_sample_dots.jpg
```

### 6. Density-aware H3 hex sampling

Recommended starting point:

```bash
python 06_hex_bin_sample.py \
  --input output/runs_raw.json \
  --geofence geofences/japan_tomei.json \
  --h3-resolution 10 \
  --sample-h3-resolution 8 \
  --min-points-per-hex 2 \
  --min-points-per-sample-hex 1 \
  --max-points-per-run 400 \
  --plot \
  --sample-dot-size 35 \
  --bins-output output/hex_bins_r8.json \
  --sample-json output/hex_sample_r8.json \
  --sample-csv output/hex_sample_r8.csv \
  --urls-output output/hex_sample_r8_urls.txt \
  --plot-output output/hex_sample_r8_map.jpg
```

When this script runs, it prints a deduplicated Frontier ADP LogSim replay URL
list to stderr and also writes it to `--urls-output` as TSV:

```text
custom_id    ursa_run_uuid    adp_logsim_uuid    logsim_replay_url    logsim_result_url    data_explorer_url    raw_data_uri    note
```

Use `--no-print-urls` if you only want the TSV file and do not want the full
URL list printed in the terminal.
```

Outputs include lookup fields for ADP/Data Engine investigation:

- `run_uuid`
- `custom_id`
- `adp_logsim_uuid`
- `logsim_replay_url`
- `logsim_result_url`
- `data_explorer_uuid`
- `data_explorer_url`
- `raw_data_uri`
- `map_key`
- `route`
- `stack_commit`

`logsim_replay_url` is the Frontier ADP playback URL:

```text
https://frontier.prod.applied.dev/log_sim/results/sim/<adp_logsim_uuid>/playback
```

For source drive rows, this field may be empty because there is not yet an ADP
LogSim simulation run to replay. In that case, use the selected `run_uuid` /
`custom_id` to create or locate a LogSim run, or resolve the bridge through
`Ursa DescribeRun(...).sim_run_info.adp_uuid`.

## Geofences

Geofences are JSON files under `geofences/`.

Bounding box example:

```json
{
  "type": "bbox",
  "name": "Japan Tomei Expressway area",
  "min_lat": 34.8,
  "max_lat": 35.6,
  "min_lon": 138.8,
  "max_lon": 139.6
}
```

Polygon geofences are also supported:

```json
{
  "type": "polygon",
  "points": [
    {"lat": 35.0, "lon": 139.0},
    {"lat": 35.5, "lon": 139.0},
    {"lat": 35.25, "lon": 139.5}
  ]
}
```

## Git remote

```bash
git remote add origin git@github.com:will-lauer-ai/aurora-mtbf-sim-curation.git
git branch -M main
git push -u origin main
```
