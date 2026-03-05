"""
US Historical Weather — Maximum Granularity Grid (0.1°) → Single CSV
=====================================================================
API    : Open-Meteo Historical Archive (free, no key required)
Grid   : 0.1° x 0.1° over all 50 US states (~87,000 land points)
Period : last 2 years up to today
Output : weather_grid_usa_full.csv

Strategy
--------
- ONE GET request per grid point (avoids HTTP 414 URL-too-long completely)
- ThreadPoolExecutor with WORKERS parallel threads (fast despite single-point calls)
- Resume support: progress tracked per state in .progress.json
- Thread-safe CSV writing with a lock

Usage
-----
    pip install requests tqdm
    python query_weather_grid_usa_single_csv.py

Output columns
--------------
state, latitude, longitude, date,
temperature_2m_max, temperature_2m_min, temperature_2m_mean,
precipitation_sum, rain_sum, snowfall_sum,
windspeed_10m_max, windgusts_10m_max, sunshine_duration
"""

import csv
import json
import math
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from itertools import product as iproduct

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    raise SystemExit("Please run: pip install requests tqdm")

try:
    from tqdm import tqdm
except ImportError:
    raise SystemExit("Please run: pip install tqdm")

# ── Configuration ──────────────────────────────────────────────────────────────

GRID_RES    = 0.1    # degrees — maximum resolution of Open-Meteo
WORKERS     = 10     # parallel threads (increase to 20 if your connection allows)
RETRY_LIMIT = 5      # retries per point
POLITE_WAIT = 0.0    # extra sleep per request (0 = full speed with WORKERS threads)

END_DATE   = date.today()
START_DATE = END_DATE - timedelta(days=365 * 2)

DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "sunshine_duration",
]

OUTPUT_CSV    = "weather_grid_usa_full.csv"
PROGRESS_FILE = "weather_grid_usa_full.progress.json"
BASE_URL      = "https://archive-api.open-meteo.com/v1/archive"
HEADER        = ["state", "latitude", "longitude", "date"] + DAILY_VARS

# ── US state bounding boxes ────────────────────────────────────────────────────
US_STATES = {
    "Alabama":        (30.1,  35.0,  -88.5,  -84.9),
    "Alaska":         (54.5,  71.5, -168.0, -130.0),
    "Arizona":        (31.3,  37.0, -114.8, -109.0),
    "Arkansas":       (33.0,  36.5,  -94.6,  -89.6),
    "California":     (32.5,  42.0, -124.4, -114.1),
    "Colorado":       (37.0,  41.0, -109.1, -102.0),
    "Connecticut":    (40.9,  42.1,  -73.7,  -71.8),
    "Delaware":       (38.4,  39.8,  -75.8,  -75.0),
    "Florida":        (24.4,  31.0,  -87.6,  -79.9),
    "Georgia":        (30.4,  35.0,  -85.6,  -80.8),
    "Hawaii":         (18.9,  22.2, -160.3, -154.8),
    "Idaho":          (42.0,  49.0, -117.2, -111.0),
    "Illinois":       (36.9,  42.5,  -91.5,  -87.5),
    "Indiana":        (37.8,  41.8,  -88.1,  -84.8),
    "Iowa":           (40.4,  43.5,  -96.6,  -90.1),
    "Kansas":         (37.0,  40.0, -102.1,  -94.6),
    "Kentucky":       (36.5,  39.1,  -89.6,  -81.9),
    "Louisiana":      (28.9,  33.0,  -94.1,  -89.0),
    "Maine":          (43.1,  47.5,  -71.1,  -66.9),
    "Maryland":       (37.9,  39.7,  -79.5,  -75.0),
    "Massachusetts":  (41.2,  42.9,  -73.5,  -69.9),
    "Michigan":       (41.7,  48.3,  -90.4,  -82.4),
    "Minnesota":      (43.5,  49.4,  -97.2,  -89.5),
    "Mississippi":    (30.2,  35.0,  -91.7,  -88.1),
    "Missouri":       (36.0,  40.6,  -95.8,  -89.1),
    "Montana":        (44.4,  49.0, -116.1, -104.0),
    "Nebraska":       (40.0,  43.0, -104.1,  -95.3),
    "Nevada":         (35.0,  42.0, -120.0, -114.0),
    "New Hampshire":  (42.7,  45.3,  -72.6,  -70.6),
    "New Jersey":     (38.9,  41.4,  -75.6,  -73.9),
    "New Mexico":     (31.3,  37.0, -109.1, -103.0),
    "New York":       (40.5,  45.0,  -79.8,  -71.9),
    "North Carolina": (33.8,  36.6,  -84.3,  -75.5),
    "North Dakota":   (45.9,  49.0, -104.1,  -96.6),
    "Ohio":           (38.4,  42.3,  -84.8,  -80.5),
    "Oklahoma":       (33.6,  37.0, -103.0,  -94.4),
    "Oregon":         (42.0,  46.3, -124.6, -116.5),
    "Pennsylvania":   (39.7,  42.3,  -80.5,  -74.7),
    "Rhode Island":   (41.1,  42.0,  -71.9,  -71.1),
    "South Carolina": (32.0,  35.2,  -83.4,  -78.5),
    "South Dakota":   (42.5,  45.9, -104.1,  -96.4),
    "Tennessee":      (34.9,  36.7,  -90.3,  -81.6),
    "Texas":          (25.8,  36.5, -106.6,  -93.5),
    "Utah":           (37.0,  42.0, -114.1, -109.0),
    "Vermont":        (42.7,  45.0,  -73.4,  -71.5),
    "Virginia":       (36.5,  39.5,  -83.7,  -75.2),
    "Washington":     (45.5,  49.0, -124.7, -116.9),
    "West Virginia":  (37.2,  40.6,  -82.6,  -77.7),
    "Wisconsin":      (42.5,  47.1,  -92.9,  -86.8),
    "Wyoming":        (41.0,  45.0, -111.1, -104.0),
}

# ── Thread-local session (one HTTP session per thread) ─────────────────────────
_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        retry = Retry(
            total=RETRY_LIMIT,
            backoff_factor=2,          # 2s, 4s, 8s, 16s, 32s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1))
        _local.session = s
    return _local.session

# ── Grid generation ────────────────────────────────────────────────────────────

def generate_grid(lat_min, lat_max, lon_min, lon_max, res=GRID_RES):
    lats = [round(lat_min + i * res, 2)
            for i in range(math.ceil((lat_max - lat_min) / res) + 1)
            if round(lat_min + i * res, 2) <= lat_max]
    lons = [round(lon_min + j * res, 2)
            for j in range(math.ceil((lon_max - lon_min) / res) + 1)
            if round(lon_min + j * res, 2) <= lon_max]
    return [(la, lo) for la, lo in iproduct(lats, lons)]

# ── Single-point fetch ─────────────────────────────────────────────────────────

def fetch_point(state, lat, lon):
    """Fetch weather for a single grid point. Returns list of row tuples."""
    params = {
        "latitude":           lat,
        "longitude":          lon,
        "start_date":         START_DATE.isoformat(),
        "end_date":           END_DATE.isoformat(),
        "daily":              ",".join(DAILY_VARS),
        "timezone":           "UTC",
        "temperature_unit":   "celsius",
        "windspeed_unit":     "kmh",
        "precipitation_unit": "mm",
    }
    try:
        r = get_session().get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()
        data  = r.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        n     = len(dates)
        rows  = []
        for i, d in enumerate(dates):
            row = [state, lat, lon, d] + [
                daily.get(v, [None] * n)[i] for v in DAILY_VARS
            ]
            rows.append(row)
        return rows
    except Exception as exc:
        return exc   # caller handles errors

# ── Progress helpers ───────────────────────────────────────────────────────────

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return set(json.load(f).get("done", []))
    return set()

def save_progress(done_states):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"done": list(done_states)}, f)

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    done_states = load_progress()

    file_exists = os.path.exists(OUTPUT_CSV)
    csv_file    = open(OUTPUT_CSV, "a", newline="", buffering=1 << 20)
    writer      = csv.writer(csv_file)
    write_lock  = threading.Lock()

    if not file_exists or os.path.getsize(OUTPUT_CSV) == 0:
        writer.writerow(HEADER)

    total_states = len(US_STATES)
    grand_rows   = 0

    print("=" * 68)
    print("  US Historical Weather — 0.1° Grid — Single CSV Output")
    print(f"  Period  : {START_DATE}  →  {END_DATE}")
    print(f"  Workers : {WORKERS} parallel threads")
    print(f"  Output  : {OUTPUT_CSV}")
    print(f"  Already done: {len(done_states)} states (will skip)")
    print("=" * 68, flush=True)

    for s_idx, (state, bbox) in enumerate(US_STATES.items(), 1):
        if state in done_states:
            print(f"[{s_idx:02d}/{total_states}] {state} — skipped.")
            continue

        grid    = generate_grid(*bbox)
        n_pts   = len(grid)
        state_rows    = 0
        failed_points = 0

        print(f"\n[{s_idx:02d}/{total_states}] {state}  |  {n_pts:,} points", flush=True)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {
                pool.submit(fetch_point, state, lat, lon): (lat, lon)
                for lat, lon in grid
            }
            with tqdm(total=n_pts, desc=f"  {state}", unit="pt", leave=False) as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    if isinstance(result, list) and result:
                        with write_lock:
                            writer.writerows(result)
                        state_rows += len(result)
                    elif isinstance(result, Exception):
                        failed_points += 1
                    pbar.update(1)

        csv_file.flush()
        grand_rows += state_rows

        if state_rows > 0 and failed_points == 0:
            done_states.add(state)
            save_progress(done_states)
            print(f"  ✅ {state} — {state_rows:,} rows, 0 failures.", flush=True)
        elif state_rows > 0:
            print(f"  ⚠️  {state} — {state_rows:,} rows, {failed_points} points FAILED "
                  f"(not marking done — will retry).", flush=True)
        else:
            print(f"  ❌ {state} — all requests failed. Check connection.", flush=True)

    csv_file.close()
    size_mb = os.path.getsize(OUTPUT_CSV) / 1_048_576

    print("\n" + "=" * 68)
    print(f"  ✅ COMPLETE")
    print(f"     States completed : {len(done_states)}")
    print(f"     Total rows       : ~{grand_rows:,}")
    print(f"     File size        : {size_mb:,.1f} MB")
    print(f"     Output           : {os.path.abspath(OUTPUT_CSV)}")
    print("=" * 68)

if __name__ == "__main__":
    main()
