"""Feature building, shared by batch training and the live job (CONTRACT.md section 5).

OWNER: Ansh (Role 2).

Why it lives in common/: the training data and the live data must get exactly
the same feature columns. If the two paths had separate code, live predictions
would silently go wrong. pipeline/build_features.py (batch) and
pipeline/stream_job.py (live) both call add_features().

How the windows work (the part examiners ask about):
- Every window is partitioned by station and ordered by time in SECONDS.
- We use rangeBetween (a time distance), not rowsBetween (a row count).
  "3 hours ago" means "the row whose timestamp is exactly 3 h earlier".
  If that hour is missing, the frame is empty and the lag is null.
  rowsBetween would silently take "3 rows back", which may be 9 hours ago.
- Lags and rolling means look only backwards (and at the current row).
  Only the target columns look forwards, and they are used as labels, never as inputs.
"""

from __future__ import annotations

import math

from common.config import HORIZONS, LOCAL_TZ

LAG_HOURS = (1, 3, 6)
ROLL_HOURS = (3, 6, 24)

# A rolling mean is only trusted if at least this share of its hours are present
# (e.g. the 24 h mean needs 12+ readings). Otherwise it is null and the model imputes it.
ROLL_MIN_SHARE = 0.5

LAG_COLUMNS = [f"{p}_lag_{h}h" for p in ("pm25", "pm10") for h in LAG_HOURS]
ROLL_COLUMNS = [f"pm25_roll_{h}h" for h in ROLL_HOURS]
CHANGE_COLUMNS = ["pm25_change_1h"]
TIME_FEATURES = ["hour", "day_of_week", "month", "is_weekend"]
TARGET_COLUMNS = [f"target_pm25_{h}h" for h in HORIZONS]

# Every column add_features() creates, in order.
DERIVED_COLUMNS = LAG_COLUMNS + ROLL_COLUMNS + CHANGE_COLUMNS + TIME_FEATURES + TARGET_COLUMNS


def add_features(df, with_targets: bool = True):
    """Return df with the lag, rolling, change, time and target columns.

    df: Spark DataFrame of clean readings, one row per (station_id, ts_utc).
        ts_utc may be a timestamp or an ISO string; it is converted to a timestamp.
        Needs at least station_id, ts_utc, pm25, pm10.
    with_targets: True for training data (targets = PM2.5 h hours later).
        False in the live job: the future is unknown, so the target columns are null.
        The columns still exist, so both paths produce the same schema.

    Needs a Spark session in UTC (common.spark.get_spark sets this), so that the
    IST time features come out right.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if dict(df.dtypes).get("ts_utc") == "string":
        df = df.withColumn("ts_utc", F.to_timestamp("ts_utc"))

    # One row per station-hour. Clean data already satisfies this; the live job
    # de-duplicates too. Kept here so a duplicate can never shift a lag.
    df = df.dropDuplicates(["station_id", "ts_utc"])

    # Time as whole seconds, so window ranges can be written in seconds.
    df = df.withColumn("_t", F.col("ts_utc").cast("long"))
    by_station = Window.partitionBy("station_id").orderBy("_t")

    def exactly(hours: int):
        """Frame containing only the row exactly `hours` away (negative = the past)."""
        seconds = hours * 3600
        return by_station.rangeBetween(seconds, seconds)

    # Lags: value exactly 1/3/6 hours earlier at the same station, null if that hour is missing.
    for pollutant in ("pm25", "pm10"):
        for lag in LAG_HOURS:
            df = df.withColumn(f"{pollutant}_lag_{lag}h", F.first(pollutant).over(exactly(-lag)))

    # Rolling means over the last N hours including now (only past + present rows).
    for hours in ROLL_HOURS:
        window = by_station.rangeBetween(-(hours - 1) * 3600, 0)
        enough = F.count("pm25").over(window) >= math.ceil(hours * ROLL_MIN_SHARE)
        df = df.withColumn(f"pm25_roll_{hours}h", F.when(enough, F.avg("pm25").over(window)))

    df = df.withColumn("pm25_change_1h", F.col("pm25") - F.col("pm25_lag_1h"))

    # Time features in IST: pollution follows local daily life (traffic, cooking, burning).
    local = F.from_utc_timestamp("ts_utc", LOCAL_TZ)
    df = (
        df.withColumn("hour", F.hour(local))
        .withColumn("day_of_week", F.dayofweek(local))  # 1 = Sunday ... 7 = Saturday
        .withColumn("month", F.month(local))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
    )

    # Targets: PM2.5 exactly h hours LATER. Labels for training only.
    for h in HORIZONS:
        name = f"target_pm25_{h}h"
        if with_targets:
            df = df.withColumn(name, F.first("pm25").over(exactly(h)))
        else:
            df = df.withColumn(name, F.lit(None).cast("double"))

    return df.drop("_t")
