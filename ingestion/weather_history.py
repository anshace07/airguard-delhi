"""Step 7: hourly weather history for every station (Role 1: Riya Maurya).

    python -m ingestion.weather_history                       # years found in data/raw/openaq/
    python -m ingestion.weather_history --start 2024-01-01 --end 2025-12-31

Source: Open-Meteo historical API (archive-api.open-meteo.com/v1/archive), no key needed,
free for non-commercial use. Variables: temperature_2m, relative_humidity_2m,
wind_speed_10m (asked in m/s with wind_speed_unit=ms), wind_direction_10m. Times in GMT (UTC).

To save API calls, stations closer than --grid degrees (0.1° ≈ 11 km) share one
weather point: the weather model's own grid is about that coarse anyway.
Output: data/raw/weather/station_<id>_<year>.csv with
    station_id, ts_utc, temperature_c, humidity_pct, wind_speed_ms, wind_dir_deg
Already-downloaded station-years are skipped, so it can be re-run after a stop.
"""

from __future__ import annotations

import argparse
import csv
import re
import time
from collections import defaultdict
from datetime import date, timedelta

import requests

from common.config import PATHS

URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m"]
COLUMNS = ["station_id", "ts_utc", "temperature_c", "humidity_pct", "wind_speed_ms", "wind_dir_deg"]
POINTS_PER_CALL = 10


def years_in_archive() -> list[int]:
    years = {int(m.group(1)) for p in PATHS.raw_openaq.glob("locationid=*/year=*") if (m := re.search(r"year=(\d{4})", p.name))}
    return sorted(years)


def load_stations() -> list[dict]:
    if not PATHS.stations_csv.exists():
        raise SystemExit("No data/raw/stations.csv yet. Run: python -m ingestion.stations")
    with open(PATHS.stations_csv) as fh:
        return [{**r, "station_id": int(r["station_id"]), "lat": float(r["lat"]), "lon": float(r["lon"])} for r in csv.DictReader(fh)]


def fetch(points: list[tuple[float, float]], start: date, end: date) -> list[dict]:
    params = {
        "latitude": ",".join(f"{lat:.4f}" for lat, _ in points),
        "longitude": ",".join(f"{lon:.4f}" for _, lon in points),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY),
        "wind_speed_unit": "ms",
        "timezone": "GMT",
    }
    for attempt in range(6):
        try:
            response = requests.get(URL, params=params, timeout=120)
        except requests.RequestException:
            time.sleep(10 * (attempt + 1))
            continue
        if response.status_code == 429:
            reason = response.json().get("reason", "") if response.headers.get("content-type", "").startswith("application/json") else ""
            if "daily" in reason.lower():
                raise SystemExit(f"Open-Meteo daily limit reached ({reason}). Re-run tomorrow; finished files are kept.")
            print("  Open-Meteo rate limit, waiting 65 s ...")
            time.sleep(65)
            continue
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else [data]
    raise RuntimeError("Open-Meteo did not answer after several tries")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download hourly weather history from Open-Meteo.")
    parser.add_argument("--start", help="YYYY-MM-DD (default: 1 Jan of the first archive year)")
    parser.add_argument("--end", help="YYYY-MM-DD (default: 31 Dec of the last archive year, at most 2 days ago)")
    parser.add_argument("--grid", type=float, default=0.1, help="degrees; stations within one cell share weather")
    args = parser.parse_args()

    years = years_in_archive()
    if not years and not (args.start and args.end):
        raise SystemExit("No archive files found. Download them first or pass --start and --end.")
    start = date.fromisoformat(args.start) if args.start else date(years[0], 1, 1)
    end = date.fromisoformat(args.end) if args.end else date(years[-1], 12, 31)
    end = min(end, date.today() - timedelta(days=2))

    stations = load_stations()
    cells: dict[tuple[float, float], list[int]] = defaultdict(list)
    for s in stations:
        cell = (round(round(s["lat"] / args.grid) * args.grid, 4), round(round(s["lon"] / args.grid) * args.grid, 4))
        cells[cell].append(s["station_id"])
    print(f"{len(stations)} stations -> {len(cells)} weather points, {start} to {end}")

    PATHS.raw_weather.mkdir(parents=True, exist_ok=True)
    calls = 0
    for year in range(start.year, end.year + 1):
        y_start, y_end = max(start, date(year, 1, 1)), min(end, date(year, 12, 31))
        if y_start > y_end:
            continue
        todo = [c for c, ids in cells.items() if not all((PATHS.raw_weather / f"station_{i}_{year}.csv").exists() for i in ids)]
        for i in range(0, len(todo), POINTS_PER_CALL):
            chunk = todo[i : i + POINTS_PER_CALL]
            results = fetch(chunk, y_start, y_end)
            calls += 1
            for cell, result in zip(chunk, results):
                hourly = result["hourly"]
                rows = list(zip(hourly["time"], *(hourly[v] for v in HOURLY)))
                for station_id in cells[cell]:
                    path = PATHS.raw_weather / f"station_{station_id}_{year}.csv"
                    with open(path.with_suffix(".tmp"), "w", newline="") as fh:
                        writer = csv.writer(fh)
                        writer.writerow(COLUMNS)
                        for t, temp, rh, ws, wd in rows:
                            writer.writerow([station_id, f"{t}:00Z", temp, rh, ws, wd])
                    path.with_suffix(".tmp").replace(path)
            print(f"  {year}: {min(i + POINTS_PER_CALL, len(todo))}/{len(todo)} weather points done")
            time.sleep(1.0)
    print(f"Done with {calls} API calls. Files in {PATHS.raw_weather}")


if __name__ == "__main__":
    main()
