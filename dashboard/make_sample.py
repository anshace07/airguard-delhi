"""Fake predictions in the exact CONTRACT.md section 6 format (Role 4: Riya Katiyar).

    python -m dashboard.make_sample

Writes data/samples/SAMPLE_predictions_latest.csv: ~30 made-up stations around Delhi,
3 rows each (1, 3, 6 h). Lets the dashboard be built and shown without any real data.
The dashboard shows a big SAMPLE banner whenever this file is used. Never present it as real.
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone

import pandas as pd

from common.aqi import ALERT_CATEGORIES, pm25_category
from common.config import DELHI_NCR_BBOX, HORIZONS, PATHS
from common.schemas import PREDICTION_FIELDS

SAMPLE_FILE = PATHS.samples / "SAMPLE_predictions_latest.csv"
CITIES = ["Delhi"] * 6 + ["Noida", "Gurugram", "Ghaziabad", "Faridabad"]


def make(n_stations: int = 30, seed: int = 3) -> pd.DataFrame:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    min_lat, min_lon, max_lat, max_lon = DELHI_NCR_BBOX
    stations = []
    for i in range(n_stations):
        city = rng.choice(CITIES)
        current = round(max(15.0, rng.lognormvariate(5.0, 0.5)), 1)
        trend = rng.choice(["rising", "stable", "falling"])
        stations.append({
            "station_id": 100 + i, "station_name": f"SAMPLE Station {i + 1:02d}, {city}", "city": city,
            "lat": round(rng.uniform(min_lat + 0.15, max_lat - 0.1), 4), "lon": round(rng.uniform(min_lon + 0.15, max_lon - 0.1), 4),
            "current": current, "trend": trend,
        })
    stations.sort(key=lambda s: -s["current"])
    rows = []
    for rank, s in enumerate(stations, 1):
        drift = {"rising": 1.12, "stable": 1.0, "falling": 0.9}[s["trend"]]
        for h in HORIZONS:
            predicted = round(s["current"] * drift ** h * rng.uniform(0.93, 1.07), 1)
            category = pm25_category(predicted)
            alert = category in ALERT_CATEGORIES
            rows.append({
                "station_id": s["station_id"], "station_name": s["station_name"], "city": s["city"],
                "lat": s["lat"], "lon": s["lon"], "ts_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "horizon_h": h,
                "target_ts_utc": (now + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "current_pm25": s["current"], "current_category": pm25_category(s["current"]),
                "predicted_pm25": predicted, "predicted_category": category, "trend": s["trend"],
                "hotspot_rank": rank, "alert": int(alert),
                "alert_reason": f"Forecast {category} in {h} h (PM2.5 about {predicted:.0f})" if alert else "",
                "source": "live", "model_name": "SAMPLE (fake numbers)",
                "scored_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
    return pd.DataFrame(rows)[PREDICTION_FIELDS]


def main() -> None:
    parser = argparse.ArgumentParser(description="Write fake predictions for building the dashboard.")
    parser.add_argument("--stations", type=int, default=30)
    args = parser.parse_args()
    df = make(args.stations)
    SAMPLE_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(SAMPLE_FILE, index=False)
    print(f"Wrote {len(df)} SAMPLE rows ({args.stations} fake stations) to {SAMPLE_FILE}")


if __name__ == "__main__":
    main()
