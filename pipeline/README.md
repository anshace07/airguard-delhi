# Role 2: Processing Engine

**Owner:** Ansh Singh · **Branch:** `ansh/pipeline` · **Works closest with:** everyone (integration owner)

**Your job in one line:** turn raw data into clean data and features with Spark, run the live engine that scores every new reading, and make all four parts connect.

This is the big data core of the project: the part the course grades most.

---

## What you deliver

| Deliverable | Goes to | By |
|---|---|---|
| Repo set up, everyone's laptop passing `scripts/check_setup.py` | Team | **Day 1 (Fri Oct 16)** |
| Small sample of features (e.g. one month, a few stations) | Ayush | **Day 3 (Sun Oct 18)** |
| Clean readings `data/clean/readings/` | Ayush, report | Day 4 (Mon Oct 19) |
| `common/features.py` + full features `data/features/history/` | Ayush | **Day 4 (Mon Oct 19)** |
| Pandas vs Spark benchmark (numbers + screenshots) | Report | Day 5 (Tue Oct 20) |
| Streaming job reading Kafka, writing features + readings | — | Day 5 |
| Saved models plugged in, `data/predictions/latest.csv` live | Riya Katiyar | **Day 7 (Thu Oct 22)** |
| Alerts, trend, hotspot rank in predictions | Riya Katiyar | Day 7 |
| First full end-to-end run on the demo laptop | Team | **Day 8 (Fri Oct 23)** |

Formats: CONTRACT.md sections 4, 5 and 6.

---

## Steps

1. **Day 1:** repo, collaborators, everyone's setup. Agree that CONTRACT.md is final.
2. **Convert and clean** (`pipeline/clean_history.py`): OpenAQ archive files are **narrow** (one row per pollutant per hour). Pivot to **wide** (one row per station per hour, a column per pollutant). Normalise units, drop impossible values (negative, absurd spikes), remove duplicate `(station_id, ts_utc)`, join hourly weather. Write Parquet partitioned by `year`. Print a data-quality summary (rows in, rows dropped and why, % missing per column).
3. **Features** (`common/features.py`, function `add_features(df, with_targets)`): lags 1/3/6 h, rolling 3/6/24 h means, 1-hour change, IST time features, targets shifted 1/3/6 h ahead. Use Spark window functions **partitioned by station, ordered by time, with range-based windows on the timestamp**, so a missing hour gives a null lag instead of silently using the wrong row. Write to `data/features/history/`.
4. **Benchmark** (`pipeline/benchmark.py`): the same heavy job (pivot + features, or a big group-by) in pandas and in Spark on the full data. Record time and peak memory, or the pandas out-of-memory error. Screenshot both. This is the "why big data" proof for the report and viva.
5. **Streaming job** (`pipeline/stream_job.py`): Structured Streaming from Kafka `aq_live`.
   - The trap: lag features need past hours, but each micro-batch only contains the new readings.
   - The fix: in `foreachBatch`, append the new readings to `data/live/readings/`, read back the last ~30 hours for those stations, run the **same** `add_features(..., with_targets=False)`, keep only the newest row per station.
   - Then save `data/features/latest_by_station.parquet` (Ayush's `score()` reads it), load the 3 saved models, predict 1/3/6 h, add categories with `common.spark.category_col()`.
6. **Alerts, trend, hotspots** in the same batch:
   - `alert = 1` when a forecast enters Very Poor or Severe, or PM2.5 jumps unusually fast (z-score of the 1-hour change against that station's history).
   - `trend` from the last 3 hours of readings: rising / stable / falling. Pick clear thresholds and write them here.
   - `hotspot_rank` by current PM2.5, 1 = worst.
   - Write `data/predictions/latest.csv` (temp file + rename, so the dashboard never reads a half-written file) and append to `data/predictions/history/`.
7. **Integration:** run Kafka + fetcher + stream job + dashboard together on your laptop. Test with replay first, then live. Test that the job survives a restart (checkpoint in `data/live/_checkpoint`).
8. **Optional (Hadoop as a supplementary component):** run HDFS in Docker and point the clean Parquet there. The Parquet layout is already HDFS-ready; only do this if everything else works.

**Done when:** a reading enters Kafka and, without anyone touching anything, a prediction with category, trend and alert appears in `data/predictions/latest.csv` within a minute, and it keeps working after you restart the job.

---

## What you must be able to explain in the viva

- Why Spark: data size (your benchmark numbers), distributed processing, same code for batch and stream.
- What Kafka gives you (decoupling, no lost readings, replay).
- How Structured Streaming works: micro-batches, checkpoints, `foreachBatch`.
- Narrow vs wide format and why you pivoted.
- How the windowed lag features work and why they must never look at the future.
- Why training and live share one `add_features()`.
- How each alert rule works and why you chose those thresholds.

## Your report sections (write by hand)

System architecture, big data justification with benchmark, data cleaning and quality, feature engineering, streaming design, alert logic, integration and testing.

---

## Starter prompt for your AI coding tool

```
Read CLAUDE.md, CONTRACT.md and pipeline/README.md first.

Build my part (Role 2) step by step. Stop after each step, run it on real data, and explain it to me in simple words before continuing.

1) pipeline/clean_history.py: read the OpenAQ archive csv.gz files in common.config.PATHS.raw_openaq (inspect the real columns first, the format is narrow: one row per sensor/parameter per hour). Pivot to one row per station per hour with pm25, pm10, no2, so2, co, o3 (CONTRACT.md units), join station metadata from PATHS.stations_csv and hourly weather from PATHS.raw_weather, drop negatives/impossible values and duplicates on (station_id, ts_utc), write Parquet partitioned by year to PATHS.clean_readings. Print a data quality summary.

2) common/features.py: implement add_features(df, with_targets) exactly per CONTRACT.md section 5 using Spark window functions partitioned by station_id and ordered by the timestamp, with range-based windows so missing hours give nulls. Write a small pytest in tests/ with a hand-made DataFrame that checks lags, a gap, and that targets look forward only. Then pipeline/build_features.py writes PATHS.features_history. Also write a small sample (1 month, 5 stations) for my teammate.

3) pipeline/benchmark.py: run the same pivot + group-by job with pandas and with Spark on the full data; record wall time and peak memory (or the pandas MemoryError); print a comparison table.

4) pipeline/stream_job.py: Structured Streaming from Kafka (common.spark.get_spark(kafka=True), topic common.config.KAFKA_TOPIC), parse JSON with common.schemas.live_reading_schema(). In foreachBatch: append new rows to PATHS.live_readings, read the last 30 hours for those stations, call add_features(with_targets=False), keep the newest row per station, save PATHS.features_latest, load the PipelineModels from common.config.model_path(h) for h in HORIZONS, predict, add categories with common.spark.category_col, compute trend, hotspot_rank and alert as described in pipeline/README.md, write PATHS.predictions_latest atomically (temp file + rename) with exactly the CONTRACT.md section 6 columns, and append to PATHS.predictions_history. Checkpoint in PATHS.stream_checkpoint. Until the real models exist, use a stand-in "prediction = current pm25" clearly marked as a placeholder.
```
