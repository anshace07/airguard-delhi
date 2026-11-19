"""Step 1: find the monitoring stations (Role 1: Riya Maurya).

    python -m ingestion.stations                 # Delhi-NCR (default)
    python -m ingestion.stations --scope india   # every station in India (for the big history download)

Writes:
    data/raw/stations.csv          station_id, station_name, city, lat, lon   (CONTRACT.md section 4)
    data/raw/stations_detail.csv   + sensors, provider, first/last reading, reporting_now
Prints how many stations report right now (last reading within 24 hours).

By default only reference monitors (government stations such as CPCB/DPCC) are kept;
--include-low-cost adds cheap sensors, which are noisier.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta, timezone

from common.config import DELHI_NCR_BBOX, PATHS
from ingestion.openaq_client import OpenAQ, has_pm25, parse_location, parse_utc


def bbox_param(bbox=DELHI_NCR_BBOX) -> str:
    """common.config stores (min_lat, min_lon, max_lat, max_lon); OpenAQ wants lon first."""
    min_lat, min_lon, max_lat, max_lon = bbox
    return f"{min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f}"


def fetch_stations(client: OpenAQ, scope: str = "ncr", monitors_only: bool = True) -> list[dict]:
    params: dict = {"iso": "IN"} if scope == "india" else {"bbox": bbox_param()}
    if monitors_only:
        params["monitor"] = "true"
    raw = client.paged("/locations", params)
    stations = [parse_location(loc) for loc in raw]
    return [s for s in stations if has_pm25(s) and not s["is_mobile"] and s["lat"] is not None]


def in_ncr(station: dict) -> bool:
    min_lat, min_lon, max_lat, max_lon = DELHI_NCR_BBOX
    return min_lat <= station["lat"] <= max_lat and min_lon <= station["lon"] <= max_lon


def main() -> None:
    parser = argparse.ArgumentParser(description="List OpenAQ stations and save data/raw/stations.csv.")
    parser.add_argument("--scope", choices=["ncr", "india"], default="ncr")
    parser.add_argument("--include-low-cost", action="store_true", help="also keep non-reference sensors")
    args = parser.parse_args()

    client = OpenAQ()
    stations = fetch_stations(client, args.scope, monitors_only=not args.include_low_cost)
    now = datetime.now(timezone.utc)
    for s in stations:
        last = parse_utc(s["datetime_last_utc"])
        s["reporting_now"] = bool(last and now - last < timedelta(hours=24))

    PATHS.stations_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(PATHS.stations_csv, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["station_id", "station_name", "city", "lat", "lon"])
        for s in sorted(stations, key=lambda s: s["station_id"]):
            writer.writerow([s["station_id"], s["station_name"], s["city"], s["lat"], s["lon"]])

    detail = PATHS.stations_csv.with_name("stations_detail.csv")
    with open(detail, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["station_id", "station_name", "city", "lat", "lon", "provider", "is_monitor",
                         "parameters", "sensors_json", "datetime_first_utc", "datetime_last_utc", "reporting_now"])
        for s in sorted(stations, key=lambda s: s["station_id"]):
            params = sorted({p for p, _ in s["sensors"].values()})
            writer.writerow([s["station_id"], s["station_name"], s["city"], s["lat"], s["lon"], s["provider"],
                             s["is_monitor"], " ".join(params), json.dumps({str(k): v for k, v in s["sensors"].items()}),
                             s["datetime_first_utc"], s["datetime_last_utc"], s["reporting_now"]])

    ncr = [s for s in stations if in_ncr(s)]
    print(f"Stations with a PM2.5 sensor ({args.scope}): {len(stations)}")
    print(f"  reporting in the last 24 h: {sum(s['reporting_now'] for s in stations)}")
    print(f"  inside the Delhi-NCR box: {len(ncr)}, of which reporting now: {sum(s['reporting_now'] for s in ncr)}")
    print(f"API calls used: {client.calls}")
    print(f"Saved {PATHS.stations_csv} and {detail}")


if __name__ == "__main__":
    main()
