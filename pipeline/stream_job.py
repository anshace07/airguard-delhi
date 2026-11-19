"""The live engine: Kafka -> Spark Structured Streaming -> predictions + alerts (Role 2: Ansh).

    python -m pipeline.stream_job                       # read Kafka topic aq_live, run until Ctrl+C
    python -m pipeline.stream_job --source files        # fallback: read data/live/inbox/ (fetcher --sink files)
    python -m pipeline.stream_job --available-now       # process whatever is waiting, then stop (tests, catch-up)

Every micro-batch (foreachBatch), in order:
 1. Parse the JSON readings (common.schemas.live_reading_schema), drop broken rows.
 2. APPEND them to data/live/readings/ (Parquet, partitioned by source).
 3. THE LAG TRAP: a micro-batch holds only the new readings, so it cannot compute
    "PM2.5 3 hours ago" by itself. So we read back the last 30 hours of that source
    from data/live/readings/, de-duplicate (keep the latest ingested_at_utc per
    station-hour, CONTRACT.md section 1) and run the SAME add_features() as training.
 4. Keep the newest row per station that has PM2.5 -> data/features/latest_by_station.parquet
    (the what-if page uses it).
 5. Load the saved models (reloaded automatically if retrained) and predict PM2.5 at
    1, 3 and 6 h. Before models exist, a clearly labelled persistence placeholder is used.
 6. Rules on top: CPCB category, trend (last 3 h), spike (z-score of the 1-hour jump),
    hotspot rank, alert + a human-readable reason.
 7. Write data/predictions/latest.csv ATOMICALLY (temp file + rename, so the dashboard
    never reads half a file) and append to data/predictions/history/.

Restart safety: the checkpoint (data/live/_checkpoint/<source>) records which Kafka
offsets are done. Stop and start the job: it continues where it stopped, nothing is
lost. If it crashes mid-batch, that batch runs again; step 3's de-duplication makes the
repeat harmless.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone

from common.aqi import ALERT_CATEGORIES
from common.config import HORIZONS, KAFKA_BOOTSTRAP, KAFKA_TOPIC, PATHS, model_path
from common.features import add_features
from common.schemas import PREDICTION_FIELDS, POLLUTANTS, live_reading_schema
from common.spark import category_col, get_spark

WINDOW_HOURS = 30  # enough history for the 24 h rolling mean and the 6 h lag
TREND_MIN_CHANGE = 10.0  # µg/m³ over 3 h, and at least 10% of the old value, to count as rising/falling
SPIKE_Z = 3.0
SPIKE_MIN_JUMP = 20.0  # µg/m³ in one hour
ALERT_PROBABILITY = 0.5

_models: dict = {}


# ------------------------------------------------------------------ models

def _load(path):
    """PipelineModel from a folder, cached; reloaded when the folder changes (retraining)."""
    from pyspark.ml import PipelineModel

    if not path.exists():
        return None
    stamp = path.stat().st_mtime
    cached = _models.get(str(path))
    if cached and cached[0] == stamp:
        return cached[1]
    try:
        model = PipelineModel.load(str(path))
    except Exception as exc:  # half-written model folder while retraining
        print(f"  could not load {path.name} ({exc.__class__.__name__}); using the previous version if any")
        return cached[1] if cached else None
    _models[str(path)] = (stamp, model)
    return model


def _model_names() -> dict:
    import json

    path = PATHS.models / "model_info.json"
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


# ------------------------------------------------------------------ batch logic

def clean_batch(batch_df):
    """Typed, sane readings: timestamp ts, negative pollutant values -> null, no broken keys."""
    from pyspark.sql import functions as F

    df = batch_df.withColumn("ts_utc", F.date_trunc("hour", F.to_timestamp("ts_utc"))).withColumn(
        "ingested_at_utc", F.to_timestamp("ingested_at_utc"))
    for p in POLLUTANTS:
        df = df.withColumn(p, F.when(F.col(p) >= 0, F.col(p)))
    return df.filter(F.col("station_id").isNotNull() & F.col("ts_utc").isNotNull() & F.col("source").isin("live", "replay"))


def recent_history(spark, source: str, newest_ts):
    """The last WINDOW_HOURS of one source, one row per station-hour (latest ingestion wins)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    hist = spark.read.parquet(str(PATHS.live_readings)).filter(
        (F.col("source") == source)
        & (F.col("ts_utc") > F.lit(newest_ts) - F.expr(f"INTERVAL {WINDOW_HOURS} HOURS"))
        & (F.col("ts_utc") <= F.lit(newest_ts))
    )
    latest_first = Window.partitionBy("station_id", "ts_utc").orderBy(F.col("ingested_at_utc").desc())
    return hist.withColumn("_r", F.row_number().over(latest_first)).filter("_r = 1").drop("_r")


def score_stations(spark, feats, source: str):
    """feats: add_features() output for the window. Returns the prediction rows (Spark DataFrame)."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Spike statistics per station over the window (z-score of the 1-hour change).
    stats = feats.groupBy("station_id").agg(F.avg("pm25_change_1h").alias("chg_mean"),
                                            F.stddev("pm25_change_1h").alias("chg_sd"))
    newest_first = Window.partitionBy("station_id").orderBy(F.col("ts_utc").desc())
    latest = (
        feats.filter(F.col("pm25").isNotNull())
        .withColumn("_r", F.row_number().over(newest_first))
        .filter("_r = 1")
        .drop("_r")
        .join(stats, "station_id", "left")
    )

    base = latest.select(
        "*",
        F.col("pm25").alias("current_pm25"),
        category_col("pm25").alias("current_category"),
        F.row_number().over(Window.orderBy(F.col("pm25").desc(), "station_id")).alias("hotspot_rank"),
    )
    # Trend from the last 3 hours (1-hour lag as a fallback when the 3-hour one is missing)
    old = F.coalesce("pm25_lag_3h", "pm25_lag_1h")
    diff = F.col("pm25") - old
    needed = F.greatest(F.lit(TREND_MIN_CHANGE), F.abs(old) * 0.1)
    base = base.withColumn(
        "trend",
        F.when(old.isNull(), "stable").when(diff >= needed, "rising").when(diff <= -needed, "falling").otherwise("stable"),
    )
    z = (F.col("pm25_change_1h") - F.col("chg_mean")) / F.col("chg_sd")
    base = base.withColumn(
        "spike", F.coalesce((F.col("chg_sd") > 0) & (z >= SPIKE_Z) & (F.col("pm25_change_1h") >= SPIKE_MIN_JUMP), F.lit(False))
    )

    # Alert model probability (3 h), if trained
    alert_model = _load(PATHS.models / "alert_h3")
    if alert_model is not None:
        p = alert_model.transform(latest).select("station_id", vector_to_array("probability")[1].alias("p_alert"))
        base = base.join(p, "station_id", "left")
    else:
        base = base.withColumn("p_alert", F.lit(None).cast("double"))

    names = _model_names()
    per_horizon = []
    for h in HORIZONS:
        model = _load(model_path(h))
        if model is not None:
            pred = model.transform(latest).select("station_id", F.greatest(F.col("prediction"), F.lit(0.0)).alias("predicted_pm25"))
            name = names.get(f"pm25_h{h}", {}).get("model", "trained model")
        else:
            pred = latest.select("station_id", F.col("pm25").alias("predicted_pm25"))
            name = "Persistence (placeholder, no model yet)"
        per_horizon.append(
            base.join(pred, "station_id").withColumn("horizon_h", F.lit(h)).withColumn("model_name", F.lit(name))
        )
    rows = per_horizon[0]
    for part in per_horizon[1:]:
        rows = rows.unionByName(part)

    rows = rows.withColumn("predicted_pm25", F.round("predicted_pm25", 1)).withColumn(
        "predicted_category", category_col("predicted_pm25"))
    reason_forecast = F.when(
        F.col("predicted_category").isin(*ALERT_CATEGORIES),
        F.format_string("Forecast %s in %d h (PM2.5 about %.0f)", F.col("predicted_category"), F.col("horizon_h"), F.col("predicted_pm25")),
    )
    reason_spike = F.when(F.col("spike"), F.format_string("Sudden jump: +%.0f µg/m³ in the last hour", F.col("pm25_change_1h")))
    reason_model = F.when(
        (F.col("horizon_h") == 3) & (F.col("p_alert") >= ALERT_PROBABILITY),
        F.format_string("Alert model: %.0f%% chance of Very Poor or worse within 3 h", F.col("p_alert") * 100),
    )
    rows = rows.withColumn("alert_reason", F.concat_ws("; ", reason_forecast, reason_spike, reason_model))
    rows = rows.withColumn("alert", (F.length("alert_reason") > 0).cast("int"))

    iso = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    scored_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = (
        rows.withColumn("target_ts_utc", F.date_format(F.col("ts_utc") + F.expr("make_interval(0,0,0,0,horizon_h,0,0)"), iso))
        .withColumn("ts_utc", F.date_format("ts_utc", iso))
        .withColumn("current_pm25", F.round("current_pm25", 1))
        .withColumn("source", F.lit(source))
        .withColumn("scored_at_utc", F.lit(scored_at))
    )
    return rows.select(*PREDICTION_FIELDS).orderBy("hotspot_rank", "horizon_h")


def write_latest_csv(pdf) -> None:
    """Atomic replace: the dashboard sees either the old file or the new one, never half."""
    PATHS.predictions_latest.parent.mkdir(parents=True, exist_ok=True)
    tmp = PATHS.predictions_latest.with_name(f".latest.{os.getpid()}.tmp")
    pdf.to_csv(tmp, index=False)
    os.replace(tmp, PATHS.predictions_latest)


def write_latest_features(latest) -> None:
    from pipeline.build_features import write_parquet_dir_atomically

    write_parquet_dir_atomically(latest, PATHS.features_latest)


def process_batch(spark, batch_df, batch_id: int = 0):
    """Everything one micro-batch does. Returns the pandas predictions written (or None)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    started = time.time()
    readings = clean_batch(batch_df).persist()
    try:
        if readings.isEmpty():
            return None
        readings.write.mode("append").partitionBy("source").parquet(str(PATHS.live_readings))
        n_new = readings.count()
        written = None
        for source in [r.source for r in readings.select("source").distinct().collect()]:
            newest_ts = readings.filter(F.col("source") == source).agg(F.max("ts_utc")).first()[0]
            history = recent_history(spark, source, newest_ts)
            feats = add_features(history.drop("source", "ingested_at_utc"), with_targets=False).persist()

            newest_first = Window.partitionBy("station_id").orderBy(F.col("ts_utc").desc())
            latest_feats = feats.filter(F.col("pm25").isNotNull()).withColumn("_r", F.row_number().over(newest_first)).filter("_r = 1").drop("_r")
            write_latest_features(latest_feats)

            predictions = score_stations(spark, feats, source)
            pdf = predictions.toPandas()
            feats.unpersist()
            if pdf.empty:
                continue
            write_latest_csv(pdf)
            predictions_for_history = spark.createDataFrame(pdf)
            predictions_for_history.write.mode("append").parquet(str(PATHS.predictions_history))
            alerts = pdf.loc[pdf["alert"] == 1, "station_id"].nunique()
            print(f"[batch {batch_id}] {n_new} new {source} readings -> {pdf['station_id'].nunique()} stations scored, "
                  f"{alerts} with an alert, newest hour {pdf['ts_utc'].max()}, {time.time() - started:.1f} s")
            written = pdf
        return written
    finally:
        readings.unpersist()


# ------------------------------------------------------------------ streaming

def main() -> None:
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description="Spark Structured Streaming: live readings -> predictions.")
    parser.add_argument("--source", choices=["kafka", "files"], default="kafka")
    parser.add_argument("--available-now", action="store_true", help="process what is waiting, then stop")
    parser.add_argument("--trigger-seconds", type=int, default=20)
    args = parser.parse_args()

    spark = get_spark("stream_job", kafka=args.source == "kafka")
    schema = live_reading_schema()
    if args.source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 20000)
            .load()
        )
        stream = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("r")).select("r.*")
    else:
        PATHS.live_inbox.mkdir(parents=True, exist_ok=True)
        stream = spark.readStream.schema(schema).option("maxFilesPerTrigger", 200).json(str(PATHS.live_inbox))

    writer = (
        stream.writeStream.foreachBatch(lambda df, batch_id: process_batch(spark, df, batch_id))
        .option("checkpointLocation", str(PATHS.stream_checkpoint / args.source))
    )
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime=f"{args.trigger_seconds} seconds")
    query = writer.start()
    print(f"Stream job running (source: {args.source}). Predictions -> {PATHS.predictions_latest}. Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping (the checkpoint keeps our place for the next start) ...")
        query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
