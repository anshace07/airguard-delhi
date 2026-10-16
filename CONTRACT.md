# CONTRACT.md: the shared formats

This file is the single source of truth for how the four parts of AirGuard talk to each other.
**Every person's AI tool must read this file before writing code.**

Rules:
- Do not change anything here on your own. If a format must change, tell the whole team first, then update this file in the same pull request as the code change.
- The shared Python code in `common/` implements this contract. Import from it instead of re-typing names, paths or thresholds.

---

## 1. Conventions used everywhere

| Thing | Rule |
|---|---|
| Time | Always store **UTC** in ISO 8601 (`2026-10-16T03:00:00Z`). Only the dashboard converts to IST for display. |
| Station ID | `station_id` = the **OpenAQ location id** (integer). Same id in the archive, the live feed, features, predictions and models. |
| Hour of a reading | `ts_utc` = the hour the measurement belongs to, truncated to the full hour. Role 1 must check which OpenAQ field (period start or end) the archive and the API use, use the **same one for both**, and write the choice in `ingestion/README.md`. |
| Missing values | Use `null`. Never use `0`, `-1` or `-999` for "missing". |
| Duplicate key | One reading per `(station_id, ts_utc)`. If two arrive, keep the latest `ingested_at_utc`. |
| Horizons | `1`, `3` and `6` hours. Nothing else. (`common.config.HORIZONS`) |
| Pollutant units | µg/m³ for `pm25`, `pm10`, `no2`, `so2`, `o3`. `co` in µg/m³ too; convert if a source gives mg/m³ or ppm, and note the conversion. |
| Weather units | `temperature_c` °C, `humidity_pct` %, `wind_speed_ms` m/s (call Open-Meteo with `wind_speed_unit=ms`), `wind_dir_deg` degrees (0 = from north). |
| Source flag | `source` is `"live"` or `"replay"`. Replayed data must never be shown as live. |

---

## 2. Kafka

| Setting | Value |
|---|---|
| Broker (from your laptop) | `localhost:9092` |
| Broker (from another container) | `kafka:29092` |
| Topic | `aq_live` (3 partitions) |
| Message key | `str(station_id)` |
| Message value | one **live reading**, JSON, UTF-8 (section 3) |

Started by `infra/docker-compose.yml`. Topic is created automatically.

---

## 3. Live reading (Role 1 → Kafka → Role 2)

One JSON object per station per hour. Field names, types and units are fixed.

| Field | Type | Notes |
|---|---|---|
| `station_id` | int | OpenAQ location id |
| `station_name` | string | as given by OpenAQ |
| `city` | string | e.g. `Delhi`, `Gurugram`, `Noida` |
| `lat` | float | |
| `lon` | float | |
| `ts_utc` | string (ISO 8601, UTC) | the reading's hour |
| `pm25` | float or null | µg/m³ |
| `pm10` | float or null | µg/m³ |
| `no2` | float or null | µg/m³ |
| `so2` | float or null | µg/m³ |
| `co` | float or null | µg/m³ |
| `o3` | float or null | µg/m³ |
| `temperature_c` | float or null | |
| `humidity_pct` | float or null | |
| `wind_speed_ms` | float or null | |
| `wind_dir_deg` | float or null | |
| `source` | string | `live` or `replay` |
| `ingested_at_utc` | string (ISO 8601, UTC) | when our fetcher created the message |

Example (values are illustrative, not a real station):

```json
{
  "station_id": 12345,
  "station_name": "Example Station, Delhi",
  "city": "Delhi",
  "lat": 28.61,
  "lon": 77.21,
  "ts_utc": "2026-10-16T03:00:00Z",
  "pm25": 182.0, "pm10": 310.0, "no2": 64.1, "so2": 12.3, "co": 1450.0, "o3": 22.0,
  "temperature_c": 24.1, "humidity_pct": 71.0, "wind_speed_ms": 1.2, "wind_dir_deg": 310.0,
  "source": "live",
  "ingested_at_utc": "2026-10-16T03:07:12Z"
}
```

Spark schema: `common.schemas.live_reading_schema()`.

---

## 4. Files on disk

All data lives under `data/` and is **never committed to Git** (too big). Paths are in `common/config.py`.

| Path | Written by | Read by | What it is |
|---|---|---|---|
| `data/raw/openaq/` | Role 1 | Role 2 | OpenAQ archive files as downloaded (`csv.gz`, narrow format) |
| `data/raw/weather/` | Role 1 | Role 2 | Historical hourly weather per station |
| `data/raw/stations.csv` | Role 1 | everyone | `station_id, station_name, city, lat, lon` |
| `data/clean/readings/` | Role 2 | Role 2, Role 3 | Parquet, one row per station per hour, **same columns as the live reading** minus `source`, `ingested_at_utc`. Partitioned by `year`. |
| `data/features/history/` | Role 2 | Role 3 | Parquet, clean readings + feature columns + target columns (section 5) |
| `data/features/latest_by_station.parquet` | Role 2 (live job) | Role 3 `score()` | The newest feature row for each station. Overwritten every batch. |
| `data/live/readings/` | Role 2 (live job) | Role 2 | Every live reading received, Parquet, append only |
| `data/predictions/latest.csv` | Role 2 (live job) | Role 4 | Current snapshot, section 6. Overwritten every batch (write to a temp file, then rename). |
| `data/predictions/history/` | Role 2 (live job) | Role 4, report | Every prediction ever made, Parquet, append only |
| `data/samples/` | Role 4 | Role 4 | Fake sample files for building the dashboard early. Must say SAMPLE in the file name. |

---

## 5. Feature columns (Role 2 → Role 3)

`common/features.py` (built by Role 2) is the **only** place features are made. Batch training and the live job both call the same function, so the model always sees identical columns.

| Column | Meaning |
|---|---|
| all live-reading columns | `station_id`, `ts_utc`, pollutants, weather |
| `pm25_lag_1h`, `pm25_lag_3h`, `pm25_lag_6h` | PM2.5 that many hours earlier, same station |
| `pm10_lag_1h`, `pm10_lag_3h`, `pm10_lag_6h` | same for PM10 |
| `pm25_roll_3h`, `pm25_roll_6h`, `pm25_roll_24h` | average PM2.5 over the last N hours, including now |
| `pm25_change_1h` | `pm25 - pm25_lag_1h` |
| `hour`, `day_of_week`, `month`, `is_weekend` | time features, computed in **IST** (pollution follows local daily life) |
| `target_pm25_1h`, `target_pm25_3h`, `target_pm25_6h` | PM2.5 that many hours **later**. Training only. Null in the live job. |

Rules:
- Lags and rolling windows use **only past and present** rows. A feature may never look at the future (that is leakage and fakes good scores).
- Weather columns are the weather **at prediction time**, not the weather that happened later.
- Lags must respect gaps: if the row 3 hours earlier is missing, `pm25_lag_3h` is null (do not just take "3 rows back").

Role 2 may add more feature columns. Add them to this table in the same pull request.

---

## 6. Prediction row (Role 2 → Role 4)

`data/predictions/latest.csv`: one row per **station × horizon** (so 3 rows per station).

| Column | Type | Notes |
|---|---|---|
| `station_id` | int | |
| `station_name` | string | |
| `city` | string | |
| `lat`, `lon` | float | |
| `ts_utc` | string | time of the latest reading used |
| `horizon_h` | int | 1, 3 or 6 |
| `target_ts_utc` | string | `ts_utc + horizon_h` |
| `current_pm25` | float | |
| `current_category` | string | CPCB category of `current_pm25` |
| `predicted_pm25` | float | |
| `predicted_category` | string | CPCB category of `predicted_pm25` |
| `trend` | string | `rising`, `stable` or `falling` |
| `hotspot_rank` | int | 1 = worst station right now (by `current_pm25`) |
| `alert` | int | `1` = alert active, `0` = none |
| `alert_reason` | string | short human sentence, empty if no alert |
| `source` | string | `live` or `replay` |
| `model_name` | string | e.g. `GBTRegressor` |
| `scored_at_utc` | string | when the prediction was made |

---

## 7. Models and results (Role 3 → Role 2, Role 4)

| Path | What |
|---|---|
| `ml/models/pm25_h1/`, `pm25_h3/`, `pm25_h6/` | Best regressor per horizon, saved as a **Spark `PipelineModel`** with the `VectorAssembler` inside, so `model.transform(df)` works on a DataFrame with the named feature columns. Output column: `prediction`. |
| `ml/models/category/` | Best category classifier (PipelineModel). Output: `prediction` (index) + `category` (name). |
| `ml/models/alert_h3/` | Yes/no classifier: "Very Poor or worse in 3 hours". |
| `ml/models/feature_columns.json` | The exact list of input feature columns, in order. |
| `ml/results/results.csv` | One row per model per horizon per experiment (below) |
| `ml/results/feature_importance.csv` | `model, horizon_h, feature, importance` |
| `outputs/charts/` | All result charts as PNG (comparison bars, confusion matrix, ROC curve, feature importance) |

Model files are large and not committed. Results CSVs and charts **are** committed.

`ml/results/results.csv` columns:

`task` (`regression` / `classification` / `alert`), `model` (`Persistence`, `LinearRegression`, `DecisionTree`, `RandomForest`, `GBT`, `LogisticRegression`), `horizon_h`, `feature_set` (`pollutants_only` / `with_weather`), `mae`, `rmse`, `r2`, `accuracy`, `precision`, `recall`, `f1`, `auc`, `n_train`, `n_test`, `train_period`, `test_period`.

Leave a metric empty when it does not apply to that task.

### The what-if function (Role 3 → Role 4)

```python
# ml/score.py
def score(station_id: int, horizon_h: int, overrides: dict | None = None) -> dict:
    """Take the station's newest feature row (data/features/latest_by_station.parquet),
    replace any columns given in overrides (e.g. {"wind_speed_ms": 4.0, "humidity_pct": 40}),
    run the saved model for that horizon and return:
    {"station_id": ..., "horizon_h": ..., "predicted_pm25": ..., "predicted_category": ...}"""
```

---

## 8. AQI categories

CPCB National AQI breakpoints for 24-hour average concentration, µg/m³. Implemented in `common/aqi.py`, so **never re-type these numbers**.

| Category | AQI | PM2.5 | PM10 |
|---|---|---|---|
| Good | 0–50 | 0–30 | 0–50 |
| Satisfactory | 51–100 | 31–60 | 51–100 |
| Moderate | 101–200 | 61–90 | 101–250 |
| Poor | 201–300 | 91–120 | 251–350 |
| Very Poor | 301–400 | 121–250 | 351–430 |
| Severe | 401–500 | 250+ | 430+ |

Honest limitation, say it in the report: the official AQI uses **24-hour averages** and at least three pollutants. We apply the PM breakpoints to hourly values and hourly forecasts, so our category is an **hourly indicator**, not the official CPCB AQI. Confirm the details against CPCB's technical document before quoting them.

---

## 9. Changing this contract

1. Message the team: what changes and why.
2. Update this file and the matching code in `common/` in **one** pull request.
3. Ansh (integration owner) merges it. Everyone pulls `main` the same day.
