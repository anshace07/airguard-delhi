"""The live feed: OpenAQ + Open-Meteo -> Kafka topic aq_live (Role 1: Riya Maurya).

RUN THIS ON ANSH'S LAPTOP (the demo machine), not on another laptop: the demo's Kafka
and stream job live there, and messages sent to another laptop's Kafka never reach them.
Keep the laptop awake, plugged in, Docker open.

    python -m ingestion.fetch_live --once --dry-run          # one cycle, print messages, send nothing
    python -m ingestion.fetch_live --backfill-hours 30        # first start: last 30 h, then live every 15 min
    python -m ingestion.fetch_live                            # live, every 15 minutes, forever
    python -m ingestion.fetch_live --replay latest --speed 60 # demo backup: replay a stored day, 1 hour per minute
    python -m ingestion.fetch_live --replay 2025-11-05 --speed 0   # a given day, as fast as possible
    ... --sink files                                          # if Kafka will not start (stream job: --source files)

Live mode, each cycle:
 1. Station list from OpenAQ (Delhi-NCR box, reference monitors with PM2.5), refreshed every 6 h.
 2. /locations/{id}/latest for each station: one call per station, paced under 60/min.
 3. The reading's hour = the newest measurement hour of that station (truncated to the hour,
    same rule as the archive). Values older than that hour are left null; stations whose
    newest value is older than --max-age-hours are skipped as offline.
 4. Weather for that hour from Open-Meteo (one call for all stations, wind in m/s).
 5. One message per station exactly as CONTRACT.md section 3, checked with
    validate_live_reading(), sent to Kafka with key str(station_id).
 6. A station-hour is sent once (state in data/live/fetch_state.json); polling every
    15 minutes just catches late data sooner.

Backfill: the stream job needs the last ~24 h of each station to compute lags and rolling
means. --backfill-hours N sends the last N hours of PM2.5/PM10 (OpenAQ hourly endpoint)
before going live, so forecasts start immediately. These are real readings (source "live").

Replay: reads a past day from data/clean/readings (plus the 30 h before it for lags) and
sends it with source = "replay", so the dashboard shows the REPLAY banner. Never live.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone

import requests

from common.config import DELHI_NCR_BBOX, PATHS
from common.schemas import LIVE_READING_FIELDS, POLLUTANTS, WEATHER, validate_live_reading
from ingestion.openaq_client import OpenAQ, OpenAQError, has_pm25, iso_z, parse_location, parse_utc, to_ugm3
from ingestion.sinks import make_sink
from ingestion.stations import bbox_param

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_VARS = {"temperature_2m": "temperature_c", "relative_humidity_2m": "humidity_pct",
                "wind_speed_10m": "wind_speed_ms", "wind_direction_10m": "wind_dir_deg"}
STATE_FILE = PATHS.live_readings.parent / "fetch_state.json"
REPLAY_WARMUP_HOURS = 30


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def hour_of(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------- stations

def load_live_stations(client: OpenAQ) -> dict[int, dict]:
    raw = client.paged("/locations", {"bbox": bbox_param(DELHI_NCR_BBOX), "monitor": "true"})
    stations = [parse_location(loc) for loc in raw]
    return {s["station_id"]: s for s in stations if has_pm25(s) and not s["is_mobile"] and s["lat"] is not None}


# ---------------------------------------------------------------- weather

def weather_by_hour(stations: list[dict]) -> dict[int, dict[str, dict]]:
    """{station_id: {"2026-11-20T03:00:00Z": {temperature_c: .., ...}}} for the last 2 days + today."""
    out: dict[int, dict[str, dict]] = {}
    for i in range(0, len(stations), 50):
        chunk = stations[i : i + 50]
        params = {
            "latitude": ",".join(f"{s['lat']:.4f}" for s in chunk),
            "longitude": ",".join(f"{s['lon']:.4f}" for s in chunk),
            "hourly": ",".join(WEATHER_VARS),
            "wind_speed_unit": "ms",
            "timezone": "GMT",
            "past_days": 2,
            "forecast_days": 1,
        }
        try:
            response = requests.get(FORECAST_URL, params=params, timeout=60)
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"  weather unavailable this cycle ({exc.__class__.__name__}); sending readings without weather")
            continue
        data = response.json()
        data = data if isinstance(data, list) else [data]
        for station, result in zip(chunk, data):
            hourly = result.get("hourly", {})
            table = {}
            for k, t in enumerate(hourly.get("time", [])):
                table[f"{t}:00Z"] = {ours: hourly[theirs][k] for theirs, ours in WEATHER_VARS.items()}
            out[station["station_id"]] = table
    return out


# ---------------------------------------------------------------- messages

def make_message(station: dict, ts: datetime, values: dict, weather: dict | None, source: str) -> dict:
    msg = {
        "station_id": int(station["station_id"]),
        "station_name": station["station_name"],
        "city": station["city"],
        "lat": float(station["lat"]),
        "lon": float(station["lon"]),
        "ts_utc": iso_z(ts),
        **{p: values.get(p) for p in POLLUTANTS},
        **{w: (None if weather is None else _num(weather.get(w))) for w in WEATHER},
        "source": source,
        "ingested_at_utc": iso_z(now_utc()),
    }
    return {k: msg[k] for k in LIVE_READING_FIELDS}


def _num(value):
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def reading_from_latest(station: dict, results: list[dict], max_age_hours: float) -> tuple[datetime, dict] | None:
    """Turn /locations/{id}/latest results into (hour, {pollutant: µg/m³}). None if offline."""
    by_param: dict[str, tuple[datetime, float]] = {}
    for r in results:
        sensor = station["sensors"].get(int(r.get("sensorsId", -1)))
        when = parse_utc((r.get("datetime") or {}).get("utc"))
        if not sensor or when is None:
            continue
        param, units = sensor
        value = to_ugm3(param, r.get("value"), units)
        if value is None:
            continue
        if param not in by_param or when > by_param[param][0]:
            by_param[param] = (when, value)
    if "pm25" not in by_param:
        return None
    newest = max(hour_of(when) for when, _ in by_param.values())
    if now_utc() - newest > timedelta(hours=max_age_hours):
        return None
    values = {p: v for p, (when, v) in by_param.items() if hour_of(when) == newest}
    if "pm25" not in values:
        return None
    return newest, values


def backfill_messages(client: OpenAQ, station: dict, hours: int, weather: dict) -> list[dict]:
    """Last `hours` hourly means of PM2.5 and PM10 (one API call per sensor)."""
    end = now_utc()
    start = end - timedelta(hours=hours)
    rows: dict[datetime, dict] = {}
    for sensor_id, (param, units) in station["sensors"].items():
        if param not in ("pm25", "pm10"):
            continue
        data = client.get(f"/sensors/{sensor_id}/hours",
                          {"datetime_from": iso_z(start), "datetime_to": iso_z(end), "limit": 200})
        for r in data.get("results", []):
            period = r.get("period") or {}
            # Label the hour like the archive and /latest do: by its timestamp (period end),
            # truncated to the hour. See "Time convention" in ingestion/README.md.
            when = parse_utc((period.get("datetimeTo") or {}).get("utc"))
            value = to_ugm3(param, r.get("value"), (r.get("parameter") or {}).get("units", units))
            if when is not None and value is not None:
                rows.setdefault(hour_of(when), {})[param] = value
    return [make_message(station, ts, values, weather.get(iso_z(ts)), "live")
            for ts, values in sorted(rows.items()) if "pm25" in values]


# ---------------------------------------------------------------- state

def load_state() -> dict[str, str]:
    try:
        return json.loads(STATE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict[str, str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE_FILE)


def send_valid(sink, messages: list[dict]) -> tuple[int, int]:
    good, bad = [], 0
    for m in messages:
        problems = validate_live_reading(m)
        if problems:
            bad += 1
            print(f"  skipped invalid message for station {m.get('station_id')}: {problems}")
        else:
            good.append(m)
    return sink.send(good), bad


# ---------------------------------------------------------------- live loop

def live(args) -> None:
    client = OpenAQ()
    sink = make_sink(args.sink)
    state = load_state()
    stations: dict[int, dict] = {}
    stations_loaded_at = None
    backfill_done = args.backfill_hours <= 0

    while True:
        cycle_start = time.time()
        if stations_loaded_at is None or time.time() - stations_loaded_at > 6 * 3600:
            stations = load_live_stations(client)
            stations_loaded_at = time.time()
            print(f"{len(stations)} Delhi-NCR stations with PM2.5 found")
        weather = weather_by_hour(list(stations.values()))

        if not backfill_done:
            total = 0
            for i, station in enumerate(stations.values(), 1):
                try:
                    msgs = backfill_messages(client, station, args.backfill_hours, weather.get(station["station_id"], {}))
                except OpenAQError as exc:
                    print(f"  backfill failed for {station['station_id']}: {exc}")
                    continue
                sent, _ = send_valid(sink, msgs)
                total += sent
                if i % 10 == 0:
                    print(f"  backfill: {i}/{len(stations)} stations")
            # The state is NOT updated here on purpose: the newest hour is sent again below from
            # /latest with all six pollutants, and the stream job keeps the later message.
            backfill_done = True
            print(f"Backfill sent {total} hourly readings ({args.backfill_hours} h)")

        sent = stale = errors = invalid = 0
        messages = []
        for station in stations.values():
            try:
                data = client.get(f"/locations/{station['station_id']}/latest")
            except OpenAQError:
                errors += 1
                continue
            found = reading_from_latest(station, data.get("results", []), args.max_age_hours)
            if found is None:
                stale += 1
                continue
            ts, values = found
            key = str(station["station_id"])
            if state.get(key, "") >= iso_z(ts):
                continue  # this hour was already sent
            messages.append(make_message(station, ts, values, weather.get(station["station_id"], {}).get(iso_z(ts)), "live"))
        if messages:
            try:
                sent, invalid = send_valid(sink, messages)
                for m in messages:
                    state[str(m["station_id"])] = max(state.get(str(m["station_id"]), ""), m["ts_utc"])
                save_state(state)
            except RuntimeError as exc:
                print(f"  {exc}")
                errors += len(messages)

        stamp = (now_utc() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
        print(f"[{stamp}] {len(stations)} stations polled: {sent} new readings sent, {stale} offline/stale, "
              f"{invalid} invalid, {errors} errors. API calls so far: {client.calls}")
        if args.once:
            return
        time.sleep(max(30.0, args.interval_minutes * 60 - (time.time() - cycle_start)))


# ---------------------------------------------------------------- replay

def replay(args) -> None:
    """Send a stored day with source="replay" (plus the hours before it, so lags exist)."""
    from pyspark.sql import functions as F

    from common.spark import get_spark

    spark = get_spark("replay")
    clean = spark.read.parquet(str(PATHS.clean_readings))
    min_lat, min_lon, max_lat, max_lon = DELHI_NCR_BBOX
    ncr = clean.filter(F.col("lat").between(min_lat, max_lat) & F.col("lon").between(min_lon, max_lon))
    if ncr.limit(1).count() == 0:
        print("No stations inside the Delhi-NCR box in the clean data; replaying all stations.")
        ncr = clean
    if args.replay == "latest":
        last = ncr.agg(F.max("ts_utc")).first()[0]
        day = (last - timedelta(days=1)).date()  # the last FULL day
    else:
        day = datetime.strptime(args.replay, "%Y-%m-%d").date()
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    first = day_start - timedelta(hours=REPLAY_WARMUP_HOURS)
    last = day_start + timedelta(days=1)
    rows = (
        ncr.filter((F.col("ts_utc") >= F.lit(first)) & (F.col("ts_utc") < F.lit(last)))
        .withColumn("ts_utc", F.date_format("ts_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))  # text, so no time-zone guessing
        .orderBy("ts_utc", "station_id")
        .toPandas()
    )
    spark.stop()
    if rows.empty:
        raise SystemExit(f"No clean readings for {day}. Pick a day inside the downloaded history.")

    sink = make_sink(args.sink)
    by_hour = rows.groupby("ts_utc")
    print(f"Replaying {day} (UTC) for {rows['station_id'].nunique()} stations, after {REPLAY_WARMUP_HOURS} warm-up hours, "
          f"source='replay', speed {args.speed}x")
    sent = 0
    for ts_text, part in by_hour:
        ts = parse_utc(ts_text)
        messages = []
        for r in part.itertuples(index=False):
            station = {"station_id": int(r.station_id), "station_name": r.station_name, "city": r.city,
                       "lat": r.lat, "lon": r.lon}
            values = {p: _num(getattr(r, p)) for p in POLLUTANTS}
            weather = {w: _num(getattr(r, w)) for w in WEATHER}
            messages.append(make_message(station, ts, {k: (None if v != v else v) for k, v in values.items()},
                                         {k: (None if v is None or v != v else v) for k, v in weather.items()}, "replay"))
        n, _ = send_valid(sink, messages)
        sent += n
        warmup = ts < day_start
        if not warmup:
            print(f"  replayed {iso_z(ts)}: {n} readings")
            if args.speed > 0:
                time.sleep(3600 / args.speed)
    print(f"Replay finished: {sent} readings sent.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="one live cycle, then stop")
    parser.add_argument("--dry-run", action="store_true", help="print messages instead of sending (same as --sink print)")
    parser.add_argument("--sink", choices=["kafka", "files", "print"], default="kafka")
    parser.add_argument("--interval-minutes", type=float, default=15)
    parser.add_argument("--max-age-hours", type=float, default=3, help="skip stations whose newest value is older")
    parser.add_argument("--backfill-hours", type=int, default=0, help="first send the last N hours (try 30)")
    parser.add_argument("--replay", metavar="YYYY-MM-DD|latest", help="replay a stored day instead of going live")
    parser.add_argument("--speed", type=float, default=60, help="replay speed: 60 = one hour per minute, 0 = no waiting")
    args = parser.parse_args()
    if args.dry_run:
        args.sink = "print"

    if args.replay:
        replay(args)
    else:
        live(args)


if __name__ == "__main__":
    main()
