"""add_features(): lags respect gaps, no future leaks into inputs, IST time features, targets."""

from common.features import DERIVED_COLUMNS, add_features

SCHEMA = "station_id long, ts_utc string, pm25 double, pm10 double"
# Station 1 has no reading at 02:00 and 05:00 (gaps). Station 2 is separate.
ROWS = [
    (1, "2026-01-01T00:00:00Z", 10.0, 100.0),
    (1, "2026-01-01T01:00:00Z", 20.0, 110.0),
    (1, "2026-01-01T03:00:00Z", 40.0, 130.0),
    (1, "2026-01-01T04:00:00Z", 50.0, 140.0),
    (1, "2026-01-01T06:00:00Z", 70.0, 160.0),
    (2, "2026-01-01T01:00:00Z", 999.0, 999.0),
]


def by_hour(spark, rows=ROWS, with_targets=True):
    out = add_features(spark.createDataFrame(rows, SCHEMA), with_targets=with_targets)
    return {r.ts_utc.hour: r for r in out.filter("station_id = 1").collect()}


def test_all_contract_columns_exist(spark):
    out = add_features(spark.createDataFrame(ROWS, SCHEMA))
    for column in DERIVED_COLUMNS:
        assert column in out.columns


def test_lags_use_time_not_row_count(spark):
    h = by_hour(spark)
    assert h[3].pm25_lag_1h is None  # 02:00 is missing -> null, NOT the 01:00 row
    assert h[4].pm25_lag_1h == 40.0
    assert h[4].pm25_lag_3h == 20.0  # exactly 3 hours earlier = 01:00
    assert h[6].pm25_lag_3h == 40.0
    assert h[6].pm25_lag_6h == 10.0
    assert h[4].pm10_lag_1h == 130.0


def test_lags_never_cross_stations(spark):
    h = by_hour(spark)
    assert h[1].pm25_lag_1h == 10.0  # its own station, not station 2's 999


def test_rolling_mean_needs_enough_hours(spark):
    h = by_hour(spark)
    assert h[0].pm25_roll_3h is None  # only 1 of 3 hours -> not trusted
    assert h[4].pm25_roll_3h == 45.0  # 03:00 and 04:00 present (02:00 missing)
    assert h[4].pm25_change_1h == 10.0


def test_targets_look_forward_exactly(spark):
    h = by_hour(spark)
    assert h[0].target_pm25_1h == 20.0
    assert h[1].target_pm25_1h is None  # 02:00 missing
    assert h[0].target_pm25_3h == 40.0
    assert h[0].target_pm25_6h == 70.0


def test_live_mode_has_null_targets(spark):
    h = by_hour(spark, with_targets=False)
    assert h[0].target_pm25_1h is None and h[0].target_pm25_6h is None


def test_no_leakage_into_inputs(spark):
    """Changing a FUTURE value must not change any input feature of earlier hours."""
    changed = [r if r[1] != "2026-01-01T06:00:00Z" else (1, r[1], 5000.0, 5000.0) for r in ROWS]
    a, b = by_hour(spark), by_hour(spark, changed)
    inputs = [c for c in DERIVED_COLUMNS if not c.startswith("target_")]
    for hour in (0, 1, 3, 4):
        for column in inputs:
            assert getattr(a[hour], column) == getattr(b[hour], column), (hour, column)


def test_time_features_are_ist(spark):
    h = by_hour(spark)
    # 00:00 UTC on Thu 1 Jan 2026 = 05:30 IST, Thursday (Spark: 1 = Sunday, so Thursday = 5)
    assert h[0].hour == 5
    assert h[0].day_of_week == 5
    assert h[0].is_weekend == 0
    assert h[0].month == 1
