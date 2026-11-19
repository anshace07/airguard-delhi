"""One stream micro-batch without Kafka: output matches CONTRACT.md section 6, alerts, ranks, dedup."""

from datetime import datetime, timedelta, timezone

import pytest

from common.config import PATHS
from common.schemas import PREDICTION_FIELDS, live_reading_schema
from pipeline import stream_job


@pytest.fixture()
def paths(tmp_path, monkeypatch):
    for name in ("live_readings", "predictions_history"):
        monkeypatch.setattr(PATHS, name, tmp_path / name)
    monkeypatch.setattr(PATHS, "predictions_latest", tmp_path / "predictions" / "latest.csv")
    monkeypatch.setattr(PATHS, "features_latest", tmp_path / "features" / "latest_by_station.parquet")
    monkeypatch.setattr(PATHS, "models", tmp_path / "no_models")  # -> persistence placeholder
    return tmp_path


def readings(hours=8, source="replay"):
    start = datetime(2026, 11, 10, tzinfo=timezone.utc)
    rows = []
    for station, base in ((1, 300.0), (2, 40.0)):
        for k in range(hours):
            ts = start + timedelta(hours=k)
            rows.append({
                "station_id": station, "station_name": f"Test {station}, Delhi", "city": "Delhi", "lat": 28.6, "lon": 77.2,
                "ts_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "pm25": base + k, "pm10": 2 * base, "no2": 40.0, "so2": 5.0,
                "co": 1000.0, "o3": 20.0, "temperature_c": 20.0, "humidity_pct": 60.0, "wind_speed_ms": 1.0,
                "wind_dir_deg": 90.0, "source": source, "ingested_at_utc": "2026-11-19T00:00:00Z",
            })
    return rows


def test_batch_writes_contract_predictions(spark, paths):
    batch = spark.createDataFrame(readings(), live_reading_schema())
    pdf = stream_job.process_batch(spark, batch, 0)
    assert list(pdf.columns) == PREDICTION_FIELDS
    assert len(pdf) == 2 * 3  # 2 stations x 3 horizons
    assert PATHS.predictions_latest.exists()
    worst = pdf[pdf["station_id"] == 1]
    assert set(worst["hotspot_rank"]) == {1}
    assert worst["model_name"].str.contains("placeholder").all()  # no models trained in this test
    assert (worst["predicted_category"] == "Severe").all() and (worst["alert"] == 1).all()
    clean = pdf[pdf["station_id"] == 2]
    assert (clean["alert"] == 0).all() and (clean["alert_reason"].fillna("") == "").all()
    assert set(pdf["source"]) == {"replay"}
    assert worst["ts_utc"].iloc[0] == "2026-11-10T07:00:00Z"
    assert set(worst["target_ts_utc"]) == {"2026-11-10T08:00:00Z", "2026-11-10T10:00:00Z", "2026-11-10T13:00:00Z"}
    assert worst["trend"].iloc[0] == "stable"  # +3 in 3 h is below the 10 µg/m³ / 10% threshold


def test_duplicates_and_reruns_are_harmless(spark, paths):
    batch = spark.createDataFrame(readings(), live_reading_schema())
    first = stream_job.process_batch(spark, batch, 0)
    again = stream_job.process_batch(spark, batch, 1)  # same messages again (e.g. a replayed batch)
    assert first[["station_id", "horizon_h", "predicted_pm25"]].equals(again[["station_id", "horizon_h", "predicted_pm25"]])
