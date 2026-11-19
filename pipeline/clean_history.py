"""Clean the OpenAQ archive into one row per station per hour (Role 2: Ansh).

    python -m pipeline.clean_history

Input  (Role 1): data/raw/openaq/**/*.csv.gz   narrow: one row per sensor, pollutant and time
                 data/raw/weather/*.csv         hourly weather per station
                 data/raw/stations.csv          station_id, station_name, city, lat, lon
Output:          data/clean/readings/           Parquet, partitioned by year (CONTRACT.md section 4)
                 outputs/data_quality.csv       what was dropped and why, % missing per column

Steps, in order:
 1. Read every archive file with Spark (no pandas: this is the big part).
 2. Parse the time. The archive writes local time with an offset ("...T01:00:00+05:30");
    Spark turns it into one UTC instant. Then truncate to the hour (CONTRACT.md: ts_utc).
 3. Keep the 6 pollutants, convert every unit to µg/m³ (ppm/ppb gases, mg/m³ CO).
 4. Drop impossible values: negative numbers (often -999 "missing" codes), PM of exactly 0,
    and values above physical limits (QUALITY_LIMITS below).
 5. Drop exact duplicate rows.
 6. Average within the hour and pivot narrow -> wide in one step: one row per
    (station_id, ts_utc) with a column per pollutant. Sub-hourly (15-min) values and
    two sensors for the same pollutant become one hourly mean.
 7. Add station name/city/lat/lon and join the hourly weather.
 8. Write Parquet partitioned by year, then print the data-quality summary.
"""

from __future__ import annotations

import argparse
import time

from common.config import PATHS
from common.schemas import POLLUTANTS, WEATHER
from common.spark import get_spark

# Anything above these (µg/m³, hourly) is treated as a sensor fault. Chosen generously:
# Delhi's worst real hours reach roughly 1,000 µg/m³ PM2.5 (instrument ceiling).
QUALITY_LIMITS = {"pm25": 1000.0, "pm10": 2000.0, "no2": 1000.0, "so2": 1000.0, "co": 50000.0, "o3": 1000.0}

# Gas conversion at 25 °C, 1 atm: µg/m³ = ppm × M × 1000 / 24.45 (M = molar mass in g/mol)
MOLAR_MASS = {"no2": 46.0055, "so2": 64.066, "co": 28.010, "o3": 47.997}

CLEAN_COLUMNS = ["station_id", "station_name", "city", "lat", "lon", "ts_utc"] + POLLUTANTS + WEATHER


def read_raw(spark, raw_dir):
    """All archive CSVs as strings. recursiveFileLookup stops Spark from turning the
    folder names (locationid=, year=, month=) into extra columns."""
    from pyspark.sql import functions as F

    df = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv.gz")
        .csv(str(raw_dir))
    )
    # The OpenAQ docs show both spellings for two columns.
    if "sensor_id" in df.columns and "sensors_id" not in df.columns:
        df = df.withColumnRenamed("sensor_id", "sensors_id")
    if "unit" in df.columns and "units" not in df.columns:
        df = df.withColumnRenamed("unit", "units")
    return df.select(
        F.col("location_id").cast("long").alias("station_id"),
        F.col("sensors_id").cast("long").alias("sensors_id"),
        F.col("location").alias("location_name"),
        F.col("datetime").alias("datetime_raw"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        F.lower(F.trim("parameter")).alias("parameter"),
        F.col("units").alias("units_raw"),
        F.col("value").cast("double").alias("value_raw"),
    )


def unit_factor_col():
    """Spark column: the number that converts value_raw to µg/m³ (null = unknown unit)."""
    from pyspark.sql import functions as F

    # "µg/m³", "μg/m³" (two different µ characters), "ug/m3" -> "ug/m3"
    unit = F.lower(F.trim(F.col("units_raw")))
    unit = F.regexp_replace(unit, "[µμ]", "u")
    unit = F.regexp_replace(unit, "³", "3")
    unit = F.regexp_replace(unit, "\\s", "")

    factor = F.when(unit.isin("ug/m3", "ugm-3"), F.lit(1.0)).when(unit.isin("mg/m3", "mgm-3"), F.lit(1000.0))
    for gas, mass in MOLAR_MASS.items():
        factor = factor.when((unit == "ppm") & (F.col("parameter") == gas), F.lit(mass * 1000 / 24.45))
        factor = factor.when((unit == "ppb") & (F.col("parameter") == gas), F.lit(mass / 24.45))
    return factor  # anything else stays null


def flag_rows(raw):
    """Add ts_utc, value (µg/m³) and one 0/1 flag per reason a row is dropped."""
    from pyspark.sql import functions as F

    ts = F.to_timestamp("datetime_raw")
    df = raw.withColumn("ts_utc", F.date_trunc("hour", ts)).withColumn("factor", unit_factor_col())
    df = df.withColumn("value", F.col("value_raw") * F.col("factor"))

    limit = F.lit(None).cast("double")
    for pollutant, cap in QUALITY_LIMITS.items():
        limit = F.when(F.col("parameter") == pollutant, F.lit(cap)).otherwise(limit)

    is_pollutant = F.col("parameter").isin(*POLLUTANTS)
    return (
        df.withColumn("f_other_parameter", (~is_pollutant | F.col("parameter").isNull()).cast("int"))
        .withColumn("f_bad_time", (is_pollutant & (F.col("ts_utc").isNull() | F.col("station_id").isNull())).cast("int"))
        .withColumn("f_bad_unit", (is_pollutant & F.col("factor").isNull()).cast("int"))
        .withColumn("f_missing_value", (is_pollutant & F.col("value_raw").isNull()).cast("int"))
        .withColumn("f_negative", (is_pollutant & (F.col("value") < 0)).cast("int"))
        .withColumn(
            "f_zero_pm", (F.col("parameter").isin("pm25", "pm10") & (F.col("value") == 0)).cast("int")
        )
        .withColumn("f_too_high", (is_pollutant & (F.col("value") > limit)).cast("int"))
    )


FLAGS = ["f_other_parameter", "f_bad_time", "f_bad_unit", "f_missing_value", "f_negative", "f_zero_pm", "f_too_high"]


def station_table(spark, narrow):
    """station_id, station_name, city, lat, lon. stations.csv wins; the archive fills gaps."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from_archive = (
        narrow.groupBy("station_id")
        .agg(F.first("location_name", ignorenulls=True).alias("a_name"), F.avg("lat").alias("a_lat"), F.avg("lon").alias("a_lon"))
    )
    if PATHS.stations_csv.exists():
        listed = spark.read.option("header", "true").csv(str(PATHS.stations_csv)).select(
            F.col("station_id").cast("long").alias("station_id"),
            F.col("station_name").alias("s_name"),
            F.col("city").alias("s_city"),
            F.col("lat").cast("double").alias("s_lat"),
            F.col("lon").cast("double").alias("s_lon"),
        )
        listed = listed.withColumn("_r", F.row_number().over(Window.partitionBy("station_id").orderBy("s_name"))).filter("_r = 1").drop("_r")
        joined = from_archive.join(listed, "station_id", "left")
    else:
        joined = from_archive
        for c in ("s_name", "s_city", "s_lat", "s_lon"):
            joined = joined.withColumn(c, F.lit(None).cast("string" if c in ("s_name", "s_city") else "double"))

    name = F.coalesce("s_name", "a_name")
    # CPCB names look like "Anand Vihar, Delhi - DPCC": the city sits between the last comma and " - ".
    city_from_name = F.trim(F.regexp_extract(name, ",\\s*([^,]+?)\\s*(?:-[^,]*)?$", 1))
    city = F.coalesce(F.nullif(F.col("s_city"), F.lit("")), F.nullif(city_from_name, F.lit("")), F.lit("Unknown"))
    return joined.select(
        "station_id",
        name.alias("station_name"),
        city.alias("city"),
        F.coalesce("s_lat", "a_lat").alias("lat"),
        F.coalesce("s_lon", "a_lon").alias("lon"),
    )


def read_weather(spark):
    """Hourly weather written by ingestion/weather_history.py, or None if there is none yet."""
    from pyspark.sql import functions as F

    files = list(PATHS.raw_weather.glob("*.csv")) if PATHS.raw_weather.exists() else []
    if not files:
        return None
    w = spark.read.option("header", "true").csv([str(f) for f in files])
    w = w.select(
        F.col("station_id").cast("long").alias("station_id"),
        F.date_trunc("hour", F.to_timestamp("ts_utc")).alias("ts_utc"),
        *[F.col(c).cast("double").alias(c) for c in WEATHER],
    )
    return w.groupBy("station_id", "ts_utc").agg(*[F.avg(c).alias(c) for c in WEATHER])


def clean(spark, raw_dir=None, out_dir=None, quality_csv=None) -> dict:
    """Run the whole cleaning job. Returns the data-quality numbers as a dict."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    raw_dir = raw_dir or PATHS.raw_openaq
    out_dir = out_dir or PATHS.clean_readings
    quality_csv = quality_csv or (PATHS.outputs / "data_quality.csv")

    started = time.time()
    flagged = flag_rows(read_raw(spark, raw_dir)).persist(StorageLevel.MEMORY_AND_DISK)

    counts = flagged.agg(F.count(F.lit(1)).alias("raw_rows"), *[F.sum(f).alias(f) for f in FLAGS]).first().asDict()
    any_flag = sum(F.col(f) for f in FLAGS) > 0
    valid = flagged.filter(~any_flag)
    valid_rows = valid.count()
    deduped = valid.dropDuplicates(["station_id", "sensors_id", "parameter", "datetime_raw", "value_raw"])
    dedup_rows = deduped.count()

    # Hourly mean per pollutant and narrow -> wide, in one Spark step.
    wide = deduped.groupBy("station_id", "ts_utc").pivot("parameter", POLLUTANTS).agg(F.avg("value"))
    stations = station_table(spark, flagged.filter(F.col("f_bad_time") == 0))
    wide = wide.join(F.broadcast(stations), "station_id", "left")

    weather = read_weather(spark)
    if weather is not None:
        wide = wide.join(weather, ["station_id", "ts_utc"], "left")
    else:
        print("WARNING: no weather files in data/raw/weather/ yet; weather columns will be empty.")
        for c in WEATHER:
            wide = wide.withColumn(c, F.lit(None).cast("double"))

    wide = wide.select(*CLEAN_COLUMNS).withColumn("year", F.year("ts_utc"))
    wide.write.mode("overwrite").partitionBy("year").parquet(str(out_dir))
    flagged.unpersist()

    result = spark.read.parquet(str(out_dir))
    summary_row = result.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("station_id").alias("stations"),
        F.min("ts_utc").alias("first_hour"),
        F.max("ts_utc").alias("last_hour"),
        *[(F.avg(F.col(c).isNull().cast("double")) * 100).alias(f"missing_pct_{c}") for c in POLLUTANTS + WEATHER],
    ).first().asDict()

    quality = {
        "raw_rows": counts["raw_rows"],
        "dropped_not_one_of_6_pollutants": counts["f_other_parameter"],
        "dropped_bad_time_or_station": counts["f_bad_time"],
        "dropped_unknown_unit": counts["f_bad_unit"],
        "dropped_missing_value": counts["f_missing_value"],
        "dropped_negative": counts["f_negative"],
        "dropped_zero_pm": counts["f_zero_pm"],
        "dropped_above_limit": counts["f_too_high"],
        "valid_rows": valid_rows,
        "dropped_exact_duplicates": valid_rows - dedup_rows,
        "clean_rows_station_hours": summary_row["rows"],
        "stations": summary_row["stations"],
        "first_hour_utc": str(summary_row["first_hour"]),
        "last_hour_utc": str(summary_row["last_hour"]),
        **{k: round(v, 2) for k, v in summary_row.items() if k.startswith("missing_pct_") and v is not None},
        "seconds": round(time.time() - started, 1),
    }
    # A row can carry two flags (e.g. negative and too high cannot, but bad unit + negative can),
    # so the dropped counts may add up to slightly more than raw - valid.

    quality_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(quality_csv, "w") as fh:
        fh.write("metric,value\n")
        for k, v in quality.items():
            fh.write(f"{k},{v}\n")
    return quality


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean the OpenAQ archive into hourly station rows.")
    parser.parse_args()
    spark = get_spark("clean_history")
    quality = clean(spark)
    print("\nData-quality summary (also saved to outputs/data_quality.csv)")
    print("-" * 60)
    for k, v in quality.items():
        print(f"{k:<38} {v}")
    print(f"\nClean readings written to {PATHS.clean_readings}")
    spark.stop()


if __name__ == "__main__":
    main()
