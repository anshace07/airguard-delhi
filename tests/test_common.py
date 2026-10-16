"""Quick checks for the shared code. Run: python -m pytest -q"""

import pytest

from common.aqi import CATEGORIES, is_alert_category, pm10_category, pm25_category, pm_aqi
from common.config import HORIZONS, model_path
from common.schemas import LIVE_READING_FIELDS, PREDICTION_FIELDS, validate_live_reading


@pytest.mark.parametrize(
    "value, expected",
    [
        (0, "Good"),
        (30, "Good"),
        (30.5, "Satisfactory"),
        (60, "Satisfactory"),
        (90, "Moderate"),
        (120, "Poor"),
        (121, "Very Poor"),
        (250, "Very Poor"),
        (251, "Severe"),
        (900, "Severe"),
        (None, None),
        (float("nan"), None),
    ],
)
def test_pm25_category(value, expected):
    assert pm25_category(value) == expected


def test_pm10_category_edges():
    assert pm10_category(50) == "Good"
    assert pm10_category(250) == "Moderate"
    assert pm10_category(431) == "Severe"


def test_negative_is_rejected():
    with pytest.raises(ValueError):
        pm25_category(-1)


def test_pm_aqi_band_edges():
    assert pm_aqi(pm25=0) == 0
    assert pm_aqi(pm25=30) == 50
    assert pm_aqi(pm25=60) == 100
    assert pm_aqi(pm25=250) == 400
    assert pm_aqi(pm25=10_000) == 500
    assert pm_aqi() is None
    # the higher sub-index wins
    assert pm_aqi(pm25=20, pm10=300) > 200


def test_alerts():
    assert is_alert_category("Severe")
    assert is_alert_category("Very Poor")
    assert not is_alert_category("Poor")
    assert len(CATEGORIES) == 6


def test_model_path():
    assert model_path(3).name == "pm25_h3"
    assert HORIZONS == (1, 3, 6)
    with pytest.raises(ValueError):
        model_path(12)


def _example_reading():
    reading = {f: None for f in LIVE_READING_FIELDS}
    reading.update(
        station_id=12345,
        station_name="Example Station, Delhi",
        city="Delhi",
        lat=28.61,
        lon=77.21,
        ts_utc="2026-10-16T03:00:00Z",
        pm25=182.0,
        source="live",
        ingested_at_utc="2026-10-16T03:07:12Z",
    )
    return reading


def test_validate_live_reading_ok():
    assert validate_live_reading(_example_reading()) == []


def test_validate_live_reading_catches_problems():
    bad = _example_reading()
    bad["source"] = "fake"
    bad["pm25"] = -5
    del bad["city"]
    problems = validate_live_reading(bad)
    assert len(problems) == 3


def test_prediction_fields_unique():
    assert len(PREDICTION_FIELDS) == len(set(PREDICTION_FIELDS))
