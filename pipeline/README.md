# Role 2: Processing Engine

**Owner:** Ansh Singh · **Works closest with:** everyone (integration owner)

**Your job in one line:** turn raw data into clean data and features with Spark, run the live engine that scores every new reading, and make all four parts connect.

This is the big data core of the project: the part the course grades most.

**Status (Nov 19):** built and tested on synthetic data (`python -m pytest -q`, `bash scripts/smoke_test.sh`). Not yet run on the real data: that happens on your laptop. Read every file below and be able to explain it.

| File | What it does | Run |
|---|---|---|
| `common/features.py` | `add_features(df, with_targets)`: lags 1/3/6 h, rolling 3/6/24 h, 1-hour change, IST time features, targets. Range windows on the timestamp, so a missing hour gives a null lag. Used by BOTH batch and live. | (imported) |
| `clean_history.py` | Narrow → wide, units → µg/m³, drops negatives/zero PM/impossible values/duplicates, hourly mean (15-min data too), station names, weather join, Parquet by year, data-quality summary → `outputs/data_quality.csv` | `python -m pipeline.clean_history` |
| `build_features.py` | Features for training → `data/features/history/`, newest row per station → `latest_by_station.parquet`, `--sample` for Role 3 | `python -m pipeline.build_features --sample` |
| `benchmark.py` | pandas vs Spark, same job, 25/50/100% of the data; time + pandas peak memory → `outputs/benchmark.csv`, `outputs/charts/benchmark.png` | `python -m pipeline.benchmark` |
| `stream_job.py` | Structured Streaming from Kafka (or `--source files`), `foreachBatch`: append → read last 30 h → features → models → trend/spike/hotspot/alerts → atomic `latest.csv` + history. Checkpoint in `data/live/_checkpoint/`. | `python -m pipeline.stream_job` |

**Restart test (do it once, screenshot it):** stop the stream job with Ctrl+C while the fetcher keeps sending, start it again: it continues from the checkpoint and the new batch includes everything sent meanwhile. Nothing is lost or counted twice (re-processed messages are de-duplicated on `(station_id, ts_utc)`, latest `ingested_at_utc` wins).

**Alert rules (in `stream_job.py`, explain these):** an alert is active when (1) the forecast for that horizon is Very Poor or Severe, or (2) the last 1-hour jump is a spike (z-score ≥ 3 against the station's last 30 h, and at least +20 µg/m³), or (3) for 3 h, the alert classifier gives ≥ 50% chance of Very Poor or worse. The reason text says which. Trend: rising/falling if PM2.5 moved at least 10 µg/m³ and 10% over the last 3 hours.

**Time zones:** all data is UTC. `common/spark.get_spark()` also sets the Python process to UTC, because Python otherwise converts Spark timestamps with the laptop's time zone (IST), shifting them by 5:30. That bug was found and fixed during testing; it is a good viva story.


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

## Changing the code

The code is built. If you change something, start your AI tool with: *"Read CLAUDE.md, CONTRACT.md and this folder's README first"*, change one thing at a time, and re-run `python -m pytest -q` and `bash scripts/smoke_test.sh`.
