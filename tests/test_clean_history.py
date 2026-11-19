"""clean_history: units, impossible values, duplicates, 15-minute averaging, time parsing, pivot."""

import csv
import gzip

import pytest

from common.config import PATHS
from pipeline import clean_history

HEADER = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]
NAME = "Anand Vihar, Delhi - DPCC"


@pytest.fixture()
def cleaned(spark, tmp_path, monkeypatch):
    raw = tmp_path / "raw" / "locationid=7" / "year=2026" / "month=01"
    raw.mkdir(parents=True)
    rows = [
        # 15-minute PM2.5 values in the 10:00 IST-offset hour (04:30 UTC -> hour 04:00 UTC)
        [7, 70, NAME, "2026-01-05T10:00:00+05:30", 28.65, 77.31, "pm25", "µg/m³", 100],
        [7, 70, NAME, "2026-01-05T10:15:00+05:30", 28.65, 77.31, "pm25", "µg/m³", 120],
        [7, 70, NAME, "2026-01-05T10:15:00+05:30", 28.65, 77.31, "pm25", "µg/m³", 120],  # exact duplicate
        [7, 71, NAME, "2026-01-05T10:00:00+05:30", 28.65, 77.31, "no2", "ppm", 0.05],  # ppm -> µg/m³
        [7, 72, NAME, "2026-01-05T10:00:00+05:30", 28.65, 77.31, "co", "mg/m³", 1.2],  # mg -> µg
        [7, 73, NAME, "2026-01-05T10:00:00+05:30", 28.65, 77.31, "pm10", "µg/m³", -999],  # sentinel
        [7, 70, NAME, "2026-01-05 05:00:00+00:00", 28.65, 77.31, "pm25", "µg/m³", 5000],  # impossible
        [7, 70, NAME, "2026-01-05 06:00:00+00:00", 28.65, 77.31, "pm25", "µg/m³", 80],  # UTC, space format
        [7, 74, NAME, "2026-01-05T10:00:00+05:30", 28.65, 77.31, "temperature", "c", 20],  # not ours
    ]
    with gzip.open(raw / "location-7-20260105.csv.gz", "wt", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        writer.writerows(rows)
    monkeypatch.setattr(PATHS, "stations_csv", tmp_path / "none.csv")
    monkeypatch.setattr(PATHS, "raw_weather", tmp_path / "no_weather")
    quality = clean_history.clean(spark, raw_dir=tmp_path / "raw", out_dir=tmp_path / "clean", quality_csv=tmp_path / "q.csv")
    table = {r.ts_utc.hour: r for r in spark.read.parquet(str(tmp_path / "clean")).collect()}
    return quality, table


def test_quality_counts(cleaned):
    quality, _ = cleaned
    assert quality["raw_rows"] == 9
    assert quality["dropped_not_one_of_6_pollutants"] == 1
    assert quality["dropped_negative"] == 1
    assert quality["dropped_above_limit"] == 1
    assert quality["dropped_exact_duplicates"] == 1


def test_one_row_per_station_hour_with_converted_units(cleaned):
    _, table = cleaned
    assert sorted(table) == [4, 6]  # 10:00 IST = 04:30 UTC -> hour 04; 06:00 UTC kept; 05:00 was impossible
    r = table[4]
    assert r.pm25 == pytest.approx(110.0)  # mean of 100 and 120 (duplicate removed first)
    assert r.no2 == pytest.approx(0.05 * 46.0055 * 1000 / 24.45, rel=1e-6)
    assert r.co == pytest.approx(1200.0)
    assert r.pm10 is None
    assert r.city == "Delhi"
    assert r.station_name == NAME
    assert table[6].pm25 == 80.0
