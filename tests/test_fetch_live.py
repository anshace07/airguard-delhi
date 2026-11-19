"""Live fetcher logic without calling any API: parsing, units, staleness, contract validity."""

from datetime import datetime, timedelta, timezone

import pytest

from common.schemas import validate_live_reading
from ingestion.fetch_live import make_message, reading_from_latest
from ingestion.openaq_client import city_from_name, iso_z, parse_location, to_ugm3

LOCATION = {  # shape of one /v3/locations result (OpenAQ docs)
    "id": 8118, "name": "Anand Vihar, Delhi - DPCC", "locality": None,
    "coordinates": {"latitude": 28.6469, "longitude": 77.3152},
    "sensors": [
        {"id": 1, "name": "pm25 µg/m³", "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"}},
        {"id": 2, "name": "pm10 µg/m³", "parameter": {"id": 1, "name": "pm10", "units": "µg/m³"}},
        {"id": 3, "name": "no2 ppm", "parameter": {"id": 5, "name": "no2", "units": "ppm"}},
    ],
    "isMobile": False, "isMonitor": True, "provider": {"name": "CPCB"},
    "datetimeLast": {"utc": "2026-11-19T10:00:00Z", "local": "2026-11-19T15:30:00+05:30"},
}


def latest(sensor, when, value):
    return {"datetime": {"utc": iso_z(when)}, "value": value, "sensorsId": sensor, "locationsId": 8118}


def test_parse_location_and_city():
    s = parse_location(LOCATION)
    assert s["station_id"] == 8118 and s["city"] == "Delhi"
    assert s["sensors"][3] == ("no2", "ppm")
    assert city_from_name("Sector 62, Noida - IMD") == "Noida"
    assert city_from_name("Lodhi Road") == "Unknown"


def test_units():
    assert to_ugm3("pm25", 50, "µg/m³") == 50
    assert to_ugm3("co", 1.5, "mg/m³") == 1500
    assert to_ugm3("no2", 0.05, "ppm") == pytest.approx(94.08, rel=1e-3)
    assert to_ugm3("pm25", -999, "µg/m³") is None
    assert to_ugm3("pm25", 0, "µg/m³") is None
    assert to_ugm3("temperature", 20, "c") is None


def test_latest_reading_uses_newest_hour_only():
    s = parse_location(LOCATION)
    now = datetime.now(timezone.utc).replace(minute=20, second=0, microsecond=0)
    old = now - timedelta(hours=2)
    ts, values = reading_from_latest(s, [latest(1, now, 180.0), latest(2, now, 300.0), latest(3, old, 0.05)], 3)
    assert ts == now.replace(minute=0)
    assert values == {"pm25": 180.0, "pm10": 300.0}  # the stale NO2 is left out (null)


def test_offline_station_is_skipped():
    s = parse_location(LOCATION)
    long_ago = datetime.now(timezone.utc) - timedelta(hours=10)
    assert reading_from_latest(s, [latest(1, long_ago, 180.0)], 3) is None


def test_message_matches_contract():
    s = parse_location(LOCATION)
    ts = datetime(2026, 11, 19, 10, tzinfo=timezone.utc)
    weather = {"temperature_c": 22.5, "humidity_pct": 70, "wind_speed_ms": 1.2, "wind_dir_deg": 300}
    msg = make_message(s, ts, {"pm25": 180.0}, weather, "live")
    assert validate_live_reading(msg) == []
    assert msg["ts_utc"] == "2026-11-19T10:00:00Z" and msg["no2"] is None
    assert validate_live_reading(make_message(s, ts, {"pm25": 1.0}, None, "replay")) == []
