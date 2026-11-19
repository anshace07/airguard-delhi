"""Measure the dataset for the report's "why big data" section (Role 1: Riya Maurya).

    python -m ingestion.dataset_numbers

Counts everything with Spark and writes outputs/dataset_numbers.md (+ .json).
Copy the numbers into docs/DATASET_NUMBERS.md and the report. Measured, never guessed.
"""

from __future__ import annotations

import json
import os
import platform
from datetime import datetime, timezone

from common.config import DELHI_NCR_BBOX, PATHS
from common.schemas import POLLUTANTS, WEATHER
from common.spark import get_spark


def folder_bytes(folder) -> int:
    return sum(p.stat().st_size for p in folder.rglob("*") if p.is_file()) if folder.exists() else 0


def main() -> None:
    from pyspark.sql import functions as F

    spark = get_spark("dataset_numbers")
    n: dict = {"measured_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
               "machine": f"{platform.system()} {platform.machine()}, {os.cpu_count()} CPU cores"}

    raw_files = list(PATHS.raw_openaq.rglob("*.csv.gz")) if PATHS.raw_openaq.exists() else []
    n["raw_files"] = len(raw_files)
    n["raw_size_mb_compressed"] = round(sum(f.stat().st_size for f in raw_files) / 1e6, 1)
    if raw_files:
        raw = spark.read.option("header", "true").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.csv.gz").csv(str(PATHS.raw_openaq))
        r = raw.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("location_id").alias("stations"),
                    F.countDistinct("parameter").alias("parameters")).first()
        n["raw_rows_narrow"] = r["rows"]
        n["raw_stations"] = r["stations"]
        n["raw_parameters"] = r["parameters"]

    if PATHS.clean_readings.exists():
        clean = spark.read.parquet(str(PATHS.clean_readings))
        min_lat, min_lon, max_lat, max_lon = DELHI_NCR_BBOX
        c = clean.agg(
            F.count(F.lit(1)).alias("rows"), F.countDistinct("station_id").alias("stations"),
            F.min("ts_utc").alias("first"), F.max("ts_utc").alias("last"),
            F.countDistinct(F.when(F.col("lat").between(min_lat, max_lat) & F.col("lon").between(min_lon, max_lon), F.col("station_id"))).alias("ncr"),
            *[(F.avg(F.col(x).isNull().cast("double")) * 100).alias(f"miss_{x}") for x in POLLUTANTS + WEATHER],
        ).first()
        n["clean_rows_station_hours"] = c["rows"]
        n["clean_stations"] = c["stations"]
        n["clean_stations_delhi_ncr"] = c["ncr"]
        n["date_range_utc"] = f"{c['first']} to {c['last']}"
        n["clean_size_mb_parquet"] = round(folder_bytes(PATHS.clean_readings) / 1e6, 1)
        for x in POLLUTANTS + WEATHER:
            n[f"missing_pct_{x}"] = round(c[f"miss_{x}"], 2)

    if PATHS.features_history.exists():
        f = spark.read.parquet(str(PATHS.features_history))
        n["feature_rows"] = f.count()
        n["feature_columns"] = len(f.columns)
        n["feature_size_mb_parquet"] = round(folder_bytes(PATHS.features_history) / 1e6, 1)

    if PATHS.live_readings.exists() and any(PATHS.live_readings.rglob("*.parquet")):
        live = spark.read.parquet(str(PATHS.live_readings))
        lv = live.filter(F.col("source") == "live").agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("station_id").alias("stations"),
                                                         F.min("ts_utc").alias("first"), F.max("ts_utc").alias("last")).first()
        n["live_messages_received"] = lv["rows"]
        n["live_stations"] = lv["stations"]
        n["live_period_utc"] = f"{lv['first']} to {lv['last']}"

    spark.stop()
    PATHS.outputs.mkdir(parents=True, exist_ok=True)
    (PATHS.outputs / "dataset_numbers.json").write_text(json.dumps(n, indent=2, default=str))
    lines = ["# Dataset numbers (measured by ingestion/dataset_numbers.py)", "", "| Measure | Value |", "|---|---|"]
    lines += [f"| {k} | {v:,} |" if isinstance(v, int) else f"| {k} | {v} |" for k, v in n.items()]
    (PATHS.outputs / "dataset_numbers.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print("\nSaved outputs/dataset_numbers.md and .json")


if __name__ == "__main__":
    main()
