"""Column lists and Spark schemas from CONTRACT.md (sections 3, 5 and 6).

The plain lists work without Spark. The *_schema() functions import PySpark
only when called, so the dashboard does not need Spark to import this file.
"""

from __future__ import annotations

POLLUTANTS = ["pm25", "pm10", "no2", "so2", "co", "o3"]
WEATHER = ["temperature_c", "humidity_pct", "wind_speed_ms", "wind_dir_deg"]

# CONTRACT.md section 3, in this exact order
LIVE_READING_FIELDS = (
    ["station_id", "station_name", "city", "lat", "lon", "ts_utc"]
    + POLLUTANTS
    + WEATHER
    + ["source", "ingested_at_utc"]
)

# CONTRACT.md section 6, in this exact order
PREDICTION_FIELDS = [
    "station_id",
    "station_name",
    "city",
    "lat",
    "lon",
    "ts_utc",
    "horizon_h",
    "target_ts_utc",
    "current_pm25",
    "current_category",
    "predicted_pm25",
    "predicted_category",
    "trend",
    "hotspot_rank",
    "alert",
    "alert_reason",
    "source",
    "model_name",
    "scored_at_utc",
]

TREND_VALUES = ("rising", "stable", "falling")
SOURCE_VALUES = ("live", "replay")

TARGET_COLUMNS = {1: "target_pm25_1h", 3: "target_pm25_3h", 6: "target_pm25_6h"}


def live_reading_schema():
    """Spark schema for parsing the JSON value of a Kafka message.

    ts_utc stays a string here; convert it with F.to_timestamp("ts_utc") after parsing.
    """
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    string_fields = {"station_name", "city", "ts_utc", "source", "ingested_at_utc"}
    fields = []
    for name in LIVE_READING_FIELDS:
        if name == "station_id":
            spark_type = LongType()
        elif name in string_fields:
            spark_type = StringType()
        else:
            spark_type = DoubleType()
        fields.append(StructField(name, spark_type, nullable=True))
    return StructType(fields)


def validate_live_reading(message: dict) -> list[str]:
    """Return a list of problems with one live-reading dict (empty list = valid).

    Role 1: call this before sending to Kafka. Role 2: call it in tests.
    """
    problems = []
    missing = [f for f in LIVE_READING_FIELDS if f not in message]
    extra = [f for f in message if f not in LIVE_READING_FIELDS]
    if missing:
        problems.append(f"missing fields: {missing}")
    if extra:
        problems.append(f"unexpected fields: {extra}")
    if not isinstance(message.get("station_id"), int):
        problems.append("station_id must be an int")
    if message.get("source") not in SOURCE_VALUES:
        problems.append(f"source must be one of {SOURCE_VALUES}")
    ts = message.get("ts_utc")
    if not (isinstance(ts, str) and ts.endswith("Z")):
        problems.append("ts_utc must be an ISO 8601 UTC string ending in 'Z'")
    for field in POLLUTANTS:
        value = message.get(field)
        if value is not None and (not isinstance(value, (int, float)) or value < 0):
            problems.append(f"{field} must be a non-negative number or null")
    return problems
