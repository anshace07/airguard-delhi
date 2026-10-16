"""CPCB AQI categories for PM2.5 and PM10 (CONTRACT.md section 8).

Pure Python, no Spark needed, so the dashboard can use it too.
For Spark DataFrames use common.spark.category_col() instead (no slow UDFs).

    from common.aqi import pm25_category, CATEGORY_COLORS
    pm25_category(182.0)  # -> "Very Poor"
"""

from __future__ import annotations

import math

CATEGORIES = ("Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe")

# Upper end of each category (24-hour average, µg/m³). Last one is open-ended.
# Source: CPCB National Air Quality Index breakpoint table.
PM25_UPPER = (30.0, 60.0, 90.0, 120.0, 250.0)
PM10_UPPER = (50.0, 100.0, 250.0, 350.0, 430.0)

# AQI range for each category.
AQI_RANGES = ((0, 50), (51, 100), (101, 200), (201, 300), (301, 400), (401, 500))

# ASSUMPTION, verify in CPCB's AQI technical document before quoting:
# the concentration at which the Severe band reaches AQI 500. This only changes
# the AQI *number* inside Severe, never the category.
SEVERE_TOP = {"pm25": 380.0, "pm10": 510.0}

# Display colours, close to the CPCB colour scheme (dark green -> maroon).
CATEGORY_COLORS = {
    "Good": "#009865",
    "Satisfactory": "#98C93C",
    "Moderate": "#FFDE33",
    "Poor": "#FF9933",
    "Very Poor": "#E8191F",
    "Severe": "#7E0023",
}

ALERT_CATEGORIES = ("Very Poor", "Severe")


def _is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _category(value, uppers) -> str | None:
    if _is_missing(value):
        return None
    if value < 0:
        raise ValueError(f"Concentration cannot be negative: {value}")
    for name, upper in zip(CATEGORIES, uppers):
        if value <= upper:
            return name
    return CATEGORIES[-1]


def pm25_category(value) -> str | None:
    """CPCB category for a PM2.5 concentration (µg/m³). None if missing."""
    return _category(value, PM25_UPPER)


def pm10_category(value) -> str | None:
    """CPCB category for a PM10 concentration (µg/m³). None if missing."""
    return _category(value, PM10_UPPER)


def category_index(name: str) -> int:
    """0 for Good ... 5 for Severe. Handy for label columns in classifiers."""
    return CATEGORIES.index(name)


def is_alert_category(name: str | None) -> bool:
    return name in ALERT_CATEGORIES


def _sub_index(value, uppers, pollutant: str) -> float | None:
    """CPCB sub-index: straight-line interpolation inside the category band."""
    if _is_missing(value):
        return None
    lows = (0.0,) + tuple(u + 1 for u in uppers)  # 0, 31, 61, ... (CPCB writes bands as integers)
    highs = uppers + (SEVERE_TOP[pollutant],)
    for (bp_lo, bp_hi), (i_lo, i_hi) in zip(zip(lows, highs), AQI_RANGES):
        if value <= bp_hi:
            value_in_band = max(value, bp_lo)
            return i_lo + (i_hi - i_lo) * (value_in_band - bp_lo) / (bp_hi - bp_lo)
    return 500.0


def pm_aqi(pm25=None, pm10=None) -> float | None:
    """AQI-style number from PM2.5 and/or PM10 (the higher sub-index wins).

    Simplified on purpose: the official CPCB AQI needs 24-hour averages and at
    least three pollutants. Call this an "hourly PM indicator" in the report.
    """
    subs = [
        s
        for s in (_sub_index(pm25, PM25_UPPER, "pm25"), _sub_index(pm10, PM10_UPPER, "pm10"))
        if s is not None
    ]
    return round(max(subs)) if subs else None
