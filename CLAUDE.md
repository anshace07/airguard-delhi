# Instructions for AI coding tools in this repo

AirGuard Delhi-NCR: a student big data project (CSET-371). Live air quality readings flow
API → Kafka → Spark Structured Streaming → saved Spark MLlib models → predictions → Streamlit dashboard.

## Before writing any code

1. Read `CONTRACT.md`. It fixes every shared format: Kafka message, file paths, column names, units, model paths.
2. Read the README in the folder you are working in (`ingestion/`, `pipeline/`, `ml/` or `dashboard/`).
3. Use the shared helpers in `common/` instead of re-typing values:
   - `common.config`: all paths (`PATHS`), Kafka settings, `HORIZONS`, `model_path()`
   - `common.aqi`: CPCB categories, thresholds, colours (`pm25_category`, `CATEGORY_COLORS`)
   - `common.spark`: `get_spark(app_name, kafka=False)` and `category_col()` for Spark
   - `common.schemas`: column lists, `live_reading_schema()`, `validate_live_reading()`

## Rules

- **Stay in the user's folder.** Do not edit `CONTRACT.md` or `common/` unless the user says the team agreed; if a change there seems needed, say so and explain why instead of making it.
- **Spark, not pandas, for the big data.** Use PySpark DataFrame functions (`pyspark.sql.functions`, window functions). Avoid Python UDFs. Pandas is fine only for small outputs (charts, the dashboard, a few thousand rows).
- **Versions:** PySpark 3.5.9, Java 17, Python 3.10+. Start Spark only through `common.spark.get_spark()`.
- **No future data in features.** Lags and rolling windows look backwards only. Split train/test **by time**, never randomly.
- **Time is UTC** everywhere in data. Convert to IST only for display and for time-of-day features.
- **Run code from the repo root** as modules: `python -m pipeline.clean_history`, not `python pipeline/clean_history.py`. For Streamlit, add the repo root to `sys.path` at the top of `dashboard/app.py`.
- **Never commit** `data/`, `.env` or `ml/models/`. Never print or log API keys.
- **Windows users run everything inside WSL (Ubuntu).** Use Linux paths and commands.
- **Be honest in outputs.** Replayed data is labelled `replay`. If a model does not beat the persistence baseline, report that; do not hide it.
- **Do not write report chapters or report prose.** The course requires 0% AI-detected text in the report. Explaining concepts so the student can write it themselves is fine.

## How to work with the student

- Build in small steps and run each step before moving on.
- After building something, explain it in simple words: what each file does, why it is done this way, and what an evaluator might ask about it. The student must be able to explain every line in a viva.
- When something is a guess (an API field name, a rate limit, a dataset size), say so and show how to check it.
