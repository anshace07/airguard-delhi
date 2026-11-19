"""Build the training feature table (Role 2: Ansh).

    python -m pipeline.build_features            # full history
    python -m pipeline.build_features --sample   # also write a small sample for quick model tests

Input:  data/clean/readings/        (pipeline.clean_history)
Output: data/features/history/      clean readings + features + targets, Parquet by year (Role 3 trains on it)
        data/features/latest_by_station.parquet
            newest feature row per station. The live stream job overwrites this every batch;
            writing it here first means the what-if page works before the live feed has run.
        data/samples/SAMPLE_features_1month_5stations.parquet   (only with --sample)

The features come from common.features.add_features(), the same function the live job calls.
"""

from __future__ import annotations

import argparse
import time

from common.config import PATHS
from common.features import add_features
from common.spark import get_spark


def newest_row_per_station(features):
    """The most recent row per station that has a PM2.5 value (needed for a forecast)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    newest_first = Window.partitionBy("station_id").orderBy(F.col("ts_utc").desc())
    return (
        features.filter(F.col("pm25").isNotNull())
        .withColumn("_r", F.row_number().over(newest_first))
        .filter("_r = 1")
        .drop("_r")
    )


def write_parquet_dir_atomically(df, target) -> None:
    """Write to a temp folder, then swap it in, so a reader never sees half a folder."""
    import shutil

    tmp = target.with_name(target.name + ".tmp")
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    old = target.with_name(target.name + ".old")
    if target.exists():
        if old.exists():
            shutil.rmtree(old)
        target.rename(old)
    tmp.rename(target)
    if old.exists():
        shutil.rmtree(old, ignore_errors=True)


def main() -> None:
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description="Build data/features/history from the clean readings.")
    parser.add_argument("--sample", action="store_true", help="also write a 1-month, 5-station sample")
    args = parser.parse_args()

    started = time.time()
    spark = get_spark("build_features")
    clean = spark.read.parquet(str(PATHS.clean_readings)).drop("year")

    features = add_features(clean, with_targets=True).withColumn("year", F.year("ts_utc"))
    features.write.mode("overwrite").partitionBy("year").parquet(str(PATHS.features_history))

    history = spark.read.parquet(str(PATHS.features_history))
    n_rows = history.count()
    n_targets = history.filter(F.col("target_pm25_3h").isNotNull()).count()

    latest = newest_row_per_station(history.drop("year"))
    for c in ("target_pm25_1h", "target_pm25_3h", "target_pm25_6h"):
        latest = latest.withColumn(c, F.lit(None).cast("double"))
    PATHS.features_latest.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_dir_atomically(latest, PATHS.features_latest)

    print(f"Feature rows: {n_rows:,} ({n_targets:,} with a 3-hour target) in {time.time() - started:.0f} s")
    print(f"Written to {PATHS.features_history}")
    print(f"Newest row per station ({latest.count()} stations) written to {PATHS.features_latest}")

    if args.sample:
        busiest = [r.station_id for r in history.groupBy("station_id").count().orderBy(F.desc("count")).limit(5).collect()]
        last_ts = history.agg(F.max("ts_utc")).first()[0]
        sample = history.filter(F.col("station_id").isin(busiest)).filter(
            F.col("ts_utc") > F.lit(last_ts) - F.expr("INTERVAL 30 DAYS")
        )
        target = PATHS.samples / "SAMPLE_features_1month_5stations.parquet"
        sample.coalesce(1).write.mode("overwrite").parquet(str(target))
        print(f"Sample ({sample.count():,} rows) written to {target}")

    spark.stop()


if __name__ == "__main__":
    main()
