"""SYNTHETIC test data in the exact OpenAQ archive format. For testing the code only.

Never use these numbers in the report or the demo: they are made up.
Every station is called "SYNTH Station NN" so it is obvious if they ever show up.

It writes into AIRGUARD_DATA_DIR, which must point somewhere other than the real
data/ folder (scripts/smoke_test.sh sets this up for you):

    AIRGUARD_DATA_DIR=/tmp/airguard_test/data python -m scripts.make_synthetic_data

What it creates (same layout the real downloaders create):
    raw/openaq/locationid=<id>/year=<y>/month=<m>/location-<id>-<yyyymmdd>.csv.gz
    raw/weather/station_<id>_<year>.csv
    raw/stations.csv

On purpose it includes the mess real data has, so the cleaning code is tested:
missing hours, a 2-day outage, gases in ppm for one station, 15-minute values for
another, a UTC-with-space datetime format, negative sentinels, impossible values
and exact duplicate rows.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from common.config import DATA_DIR, PATHS  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
MOLAR_MASS = {"no2": 46.0055, "so2": 64.066, "co": 28.010, "o3": 47.997}
HEADER = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]
CITIES = ["Delhi", "Delhi", "Delhi", "Noida", "Gurugram", "Delhi", "Ghaziabad", "Delhi", "Faridabad", "Delhi"]


def make_stations(n: int, rng: random.Random) -> list[dict]:
    stations = []
    for i in range(n):
        city = CITIES[i % len(CITIES)]
        stations.append(
            {
                "station_id": 900001 + i,
                "station_name": f"SYNTH Station {i + 1:02d}, {city} - TEST",
                "city": city,
                "lat": round(rng.uniform(28.45, 28.80), 4),
                "lon": round(rng.uniform(77.00, 77.40), 4),
                "base": rng.uniform(55, 135),
            }
        )
    return stations


def seasonal(day_of_year: int) -> float:
    """Winter peak (late Dec), monsoon low: about 0.35 to 1.75."""
    return 1.05 + 0.7 * math.cos(2 * math.pi * (day_of_year - 355) / 365)


def diurnal(hour_ist: int) -> float:
    """Evening peak around 22:00 IST, smaller morning peak, afternoon low."""
    return 1 + 0.35 * math.cos(2 * math.pi * (hour_ist - 22) / 24) + 0.12 * math.cos(2 * math.pi * (hour_ist - 8) / 12)


def generate(stations, start: datetime, hours: int, rng: random.Random):
    """Yield (station, ts_utc, values dict, weather dict) for every station-hour."""
    for s_index, st in enumerate(stations):
        noise = 0.0
        wind = 2.5
        wind_dir = rng.uniform(0, 360)
        outage_start = rng.randrange(24 * 20, max(24 * 21, hours - 72)) if s_index == 0 else None
        for k in range(hours):
            ts = start + timedelta(hours=k)
            local = ts.astimezone(IST)
            doy = local.timetuple().tm_yday
            # Weather: night is calmer, AR(1) so it changes smoothly.
            night = 1 if local.hour < 7 or local.hour > 19 else 0
            wind = max(0.2, 0.8 * wind + 0.2 * (3.2 - 1.3 * night) + rng.gauss(0, 0.45))
            wind_dir = (wind_dir + rng.gauss(0, 15)) % 360
            temp = 25 + 9 * math.cos(2 * math.pi * (doy - 160) / 365) + 5 * math.cos(2 * math.pi * (local.hour - 15) / 24) + rng.gauss(0, 1)
            humidity = min(100, max(10, 95 - 1.6 * temp + rng.gauss(0, 6)))
            weather = {"temperature_c": round(temp, 1), "humidity_pct": round(humidity, 0),
                       "wind_speed_ms": round(wind, 2), "wind_dir_deg": round(wind_dir, 0)}

            if outage_start is not None and outage_start <= k < outage_start + 48:
                yield st, ts, None, weather  # sensor down, weather still exists
                continue
            if rng.random() < 0.04:
                yield st, ts, None, weather  # a missing hour
                continue

            noise = 0.85 * noise + rng.gauss(0, 0.16)
            pm25 = st["base"] * seasonal(doy) * diurnal(local.hour) * math.exp(-0.22 * (wind - 2.5) + noise)
            pm25 = max(3.0, pm25)
            values = {
                "pm25": pm25,
                "pm10": pm25 * rng.uniform(1.6, 2.1) + rng.gauss(0, 5),
                "no2": max(2.0, 18 + 0.28 * pm25 + rng.gauss(0, 6)),
                "so2": max(1.0, 9 + rng.gauss(0, 3)),
                "co": max(100.0, 700 + 6 * pm25 + rng.gauss(0, 120)),
                "o3": max(1.0, 12 + 45 * (1 - night) - 0.04 * pm25 + rng.gauss(0, 5)),
            }
            if rng.random() < 0.05:
                values["pm10"] = None
            if s_index == 3:
                values["so2"] = None  # this station has no SO2 sensor
            yield st, ts, values, weather


def to_unit(param: str, value: float, s_index: int) -> tuple[str, float]:
    """Station 2 reports gases in ppm (as some real providers do), everyone else µg/m³."""
    if s_index == 2 and param in MOLAR_MASS:
        return "ppm", value * 24.45 / (MOLAR_MASS[param] * 1000)
    return "µg/m³", value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stations", type=int, default=8)
    parser.add_argument("--days", type=int, default=150)
    parser.add_argument("--end-date", default=None, help="last UTC day (default: 4 days ago)")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--force", action="store_true", help="allow writing into the repo's real data/ folder")
    args = parser.parse_args()

    # A real clone (has .git) must never get synthetic data in its data/ folder.
    # scripts/smoke_test.sh runs in a throwaway copy without .git, so it passes this check.
    real_clone = (REPO_ROOT / ".git").exists()
    if real_clone and DATA_DIR.resolve() == (REPO_ROOT / "data").resolve() and not args.force:
        sys.exit(
            "Refusing to write synthetic data into the real data/ folder.\n"
            "Set AIRGUARD_DATA_DIR to a temporary folder (scripts/smoke_test.sh does this)."
        )

    rng = random.Random(args.seed)
    end_day = (
        datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.end_date
        else (datetime.now(timezone.utc) - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0)
    )
    start = end_day - timedelta(days=args.days - 1)
    hours = args.days * 24

    stations = make_stations(args.stations, rng)
    PATHS.raw_openaq.mkdir(parents=True, exist_ok=True)
    PATHS.raw_weather.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "raw" / "SYNTHETIC_DATA_DO_NOT_REPORT.txt").write_text(
        "Everything under this data folder was generated by scripts/make_synthetic_data.py for testing.\n"
    )

    with open(PATHS.stations_csv, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["station_id", "station_name", "city", "lat", "lon"])
        for st in stations:
            writer.writerow([st["station_id"], st["station_name"], st["city"], st["lat"], st["lon"]])

    files: dict[Path, list] = {}
    weather_rows: dict[tuple[int, int], list] = {}
    params = ["pm25", "pm10", "no2", "so2", "co", "o3"]
    n_rows = 0
    for st, ts, values, weather in generate(stations, start, hours, rng):
        s_index = st["station_id"] - 900001
        weather_rows.setdefault((st["station_id"], ts.year), []).append(
            [st["station_id"], ts.strftime("%Y-%m-%dT%H:%M:%SZ")] + list(weather.values())
        )
        if values is None:
            continue
        day = ts.strftime("%Y%m%d")
        path = (
            PATHS.raw_openaq
            / f"locationid={st['station_id']}"
            / f"year={ts.year}"
            / f"month={ts.month:02d}"
            / f"location-{st['station_id']}-{day}.csv.gz"
        )
        rows = files.setdefault(path, [])
        # Station 4 writes UTC with a space; everyone else local time with offset (both appear in the real archive).
        stamps = [ts]
        if s_index == 1:
            stamps = [ts + timedelta(minutes=m) for m in (0, 15, 30, 45)]  # 15-minute values
        for p_index, param in enumerate(params):
            value = values[param]
            if value is None:
                continue
            for stamp in stamps:
                v = value * (1 + rng.gauss(0, 0.03)) if len(stamps) > 1 else value
                if rng.random() < 0.001:
                    v = -999.0  # sentinel some providers use for "missing"
                elif param == "pm25" and rng.random() < 0.0005:
                    v = 5000.0  # physically impossible spike
                unit, v = to_unit(param, v, s_index)
                if s_index == 4:
                    dt = stamp.strftime("%Y-%m-%d %H:%M:%S+00:00")
                else:
                    dt = stamp.astimezone(IST).isoformat()
                row = [st["station_id"], st["station_id"] * 10 + p_index, st["station_name"].split(",")[0],
                       dt, st["lat"], st["lon"], param, unit, round(v, 4)]
                rows.append(row)
                if rng.random() < 0.005:
                    rows.append(list(row))  # exact duplicate
        n_rows += 1

    for path, rows in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(HEADER)
            writer.writerows(rows)

    for (station_id, year), rows in weather_rows.items():
        with open(PATHS.raw_weather / f"station_{station_id}_{year}.csv", "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["station_id", "ts_utc", "temperature_c", "humidity_pct", "wind_speed_ms", "wind_dir_deg"])
            writer.writerows(rows)

    print(
        f"SYNTHETIC data written to {DATA_DIR}: {len(stations)} stations, {args.days} days "
        f"({start:%Y-%m-%d} to {end_day:%Y-%m-%d}), {len(files)} archive files, {n_rows} station-hours."
    )


if __name__ == "__main__":
    main()
