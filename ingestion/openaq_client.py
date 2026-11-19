"""Small OpenAQ API v3 client shared by stations.py and fetch_live.py (Role 1: Riya Maurya).

Checked against the OpenAQ docs (Nov 2026):
- Base URL https://api.openaq.org/v3, key sent in the `X-API-Key` header.
- Free limits: 60 requests/minute and 2,000/hour. Over the limit -> HTTP 429.
  Headers x-ratelimit-remaining / x-ratelimit-reset tell us when to slow down.
- /v3/locations?bbox=minLon,minLat,maxLon,maxLat  (longitude FIRST in bbox)
- /v3/locations/{id}/latest -> results[].{datetime.utc, value, sensorsId, locationsId}
- /v3/sensors/{id}/hours?datetime_from=..&datetime_to=.. -> hourly means with period.datetimeFrom/To

The API key is never printed or logged.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone

import requests

from common.config import OPENAQ_API_KEY
from common.schemas import POLLUTANTS

API = "https://api.openaq.org/v3"
MOLAR_MASS = {"no2": 46.0055, "so2": 64.066, "co": 28.010, "o3": 47.997}


class OpenAQError(RuntimeError):
    pass


class OpenAQ:
    """GET helper with pacing (stays under 60/min), retries and clear error messages."""

    def __init__(self, api_key: str | None = None, per_minute: int = 55):
        key = api_key if api_key is not None else OPENAQ_API_KEY
        if not key:
            raise SystemExit(
                "OPENAQ_API_KEY is empty. Get a free key at https://explore.openaq.org (account settings),\n"
                "then put it in .env as OPENAQ_API_KEY=... (never commit .env)."
            )
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": key, "Accept": "application/json"})
        self.min_gap = 60.0 / per_minute
        self._last = 0.0
        self.calls = 0

    def get(self, path: str, params: dict | None = None, retries: int = 5) -> dict:
        for attempt in range(retries):
            wait = self._last + self.min_gap - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            try:
                response = self.session.get(API + path, params=params, timeout=40)
            except requests.RequestException as exc:
                self._last = time.monotonic()
                if attempt == retries - 1:
                    raise OpenAQError(f"network error calling {path}: {exc.__class__.__name__}") from None
                time.sleep(2 ** attempt * 3)
                continue
            self._last = time.monotonic()
            self.calls += 1
            if response.status_code in (401, 403):
                raise SystemExit("OpenAQ rejected the API key (HTTP %d). Check OPENAQ_API_KEY in .env." % response.status_code)
            if response.status_code == 429:
                reset = _int(response.headers.get("x-ratelimit-reset"), 60)
                print(f"  OpenAQ rate limit reached, waiting {reset + 2} s ...")
                time.sleep(min(reset, 3600) + 2)
                continue
            if response.status_code >= 500:
                time.sleep(2 ** attempt * 3)
                continue
            if response.status_code == 404:
                return {"results": []}
            response.raise_for_status()
            if _int(response.headers.get("x-ratelimit-remaining"), 99) <= 1:
                time.sleep(_int(response.headers.get("x-ratelimit-reset"), 60) + 1)
            return response.json()
        raise OpenAQError(f"giving up on {path} after {retries} tries")

    def paged(self, path: str, params: dict, limit: int = 1000, max_pages: int = 50) -> list[dict]:
        out: list[dict] = []
        for page in range(1, max_pages + 1):
            data = self.get(path, {**params, "limit": limit, "page": page})
            results = data.get("results", [])
            out.extend(results)
            if len(results) < limit:
                break
        return out


def _int(value, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def city_from_name(name: str | None, locality: str | None = None) -> str:
    """CPCB names look like "Anand Vihar, Delhi - DPCC": take the part after the last comma."""
    if locality:
        return locality.strip()
    if name and "," in name:
        city = re.sub(r"\s*-[^,]*$", "", name.rsplit(",", 1)[1]).strip()
        if city:
            return city
    return "Unknown"


def parse_location(loc: dict) -> dict:
    """The fields we use from one /v3/locations result."""
    coords = loc.get("coordinates") or {}
    sensors = {}
    for s in loc.get("sensors") or []:
        param = (s.get("parameter") or {})
        sensors[int(s["id"])] = (str(param.get("name", "")).lower(), str(param.get("units", "")))
    last = (loc.get("datetimeLast") or {}).get("utc")
    first = (loc.get("datetimeFirst") or {}).get("utc")
    return {
        "station_id": int(loc["id"]),
        "station_name": loc.get("name") or f"Location {loc['id']}",
        "city": city_from_name(loc.get("name"), loc.get("locality")),
        "lat": coords.get("latitude"),
        "lon": coords.get("longitude"),
        "sensors": sensors,
        "is_monitor": bool(loc.get("isMonitor")),
        "is_mobile": bool(loc.get("isMobile")),
        "provider": (loc.get("provider") or {}).get("name"),
        "datetime_first_utc": first,
        "datetime_last_utc": last,
    }


def has_pm25(station: dict) -> bool:
    return any(param == "pm25" for param, _ in station["sensors"].values())


def to_ugm3(param: str, value, units: str):
    """Convert one value to µg/m³ (CONTRACT.md units). None if it cannot be converted or is invalid."""
    if value is None or param not in POLLUTANTS:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    unit = units.lower().replace("µ", "u").replace("μ", "u").replace("³", "3").replace(" ", "")
    if unit in ("ug/m3", "ugm-3"):
        factor = 1.0
    elif unit in ("mg/m3", "mgm-3"):
        factor = 1000.0
    elif unit == "ppm" and param in MOLAR_MASS:
        factor = MOLAR_MASS[param] * 1000 / 24.45
    elif unit == "ppb" and param in MOLAR_MASS:
        factor = MOLAR_MASS[param] / 24.45
    else:
        return None
    value *= factor
    if value < 0 or (param in ("pm25", "pm10") and value == 0):
        return None
    return round(value, 3)


def parse_utc(text: str | None) -> datetime | None:
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
