# Role 1: Data & Live Feed

**Owner:** Riya Maurya · **Works closest with:** Ansh (Role 2), Riya Katiyar (Role 4)

**Your job in one line:** get all the data into the system: years of old readings for training, and a live reading every hour into Kafka.

**Status (Nov 19):** all code is built and tested on synthetic data in the OpenAQ format (`python -m pytest -q`, `bash scripts/smoke_test.sh`). It has **not** been run against the real APIs yet, because that needs a laptop with internet and your API key. Do that first (commands below), read every file in this folder, and make sure you can explain each one.

---

## ⚠️ The live fetcher runs on ANSH'S laptop (agreed fix)

The demo's Kafka, stream job and dashboard all run on Ansh's laptop. Messages sent into Kafka on any other laptop **never reach the demo**. So:

- Start `fetch_live` **on Ansh's laptop**, as early as possible, and leave it running.
- Keep that laptop plugged in, with sleep turned off and Docker Desktop open.
- Handoffs between laptops are **"run my script on your laptop"**, not "send my data". Code, results CSVs and charts go through GitHub; model folders and data samples go through Google Drive if ever needed. Raw data never goes to GitHub.

---

## Files

| File | What it does |
|---|---|
| `openaq_client.py` | Small OpenAQ v3 client: `X-API-Key` header, stays under 60 calls/min, waits on HTTP 429, retries; unit conversion to µg/m³; city from the station name. Never prints the key. |
| `stations.py` | Lists stations (Delhi-NCR box or all India), reference monitors with a PM2.5 sensor → `data/raw/stations.csv` (+ `stations_detail.csv` with sensors and "reporting now"). |
| `download_archive.py` | Downloads the OpenAQ archive over plain HTTPS (S3 ListObjectsV2), **no AWS account or AWS CLI needed**. Parallel, resumable, `--dry-run` to see the size first. |
| `weather_history.py` | Hourly weather history from Open-Meteo for every station; stations within 0.1° share one weather point to save calls. |
| `fetch_live.py` | The live feed. Live mode every 15 min, `--backfill-hours`, `--replay`, `--once`, `--dry-run`, `--sink kafka/files`. |
| `sinks.py` | Where messages go: Kafka (normal), JSON files in `data/live/inbox/` (fallback if Kafka will not start), or print. |
| `eda.py` | PySpark EDA on the full history → PNG + CSV in `outputs/eda/` (the dashboard's Trends page reads the CSVs). |
| `dataset_numbers.py` | Measures the dataset (files, MB, rows, stations, period, % missing) → `outputs/dataset_numbers.md`. |

---

## How to run (in this order, from the repo root, venv active)

```bash
python -m ingestion.stations                         # Delhi-NCR stations -> data/raw/stations.csv (needs OPENAQ_API_KEY)
python -m ingestion.download_archive --years 2025 2026 --dry-run   # how many files / MB? downloads nothing
python -m ingestion.download_archive --years 2025 2026             # download (re-run to resume)
python -m ingestion.weather_history                  # hourly weather for the same years
# ... Ansh: clean_history + build_features, Ayush: training ...
python -m ingestion.eda                              # EDA charts after cleaning
python -m ingestion.dataset_numbers                  # real numbers for the report

# Live feed (on Ansh's laptop, Kafka running):
python -m ingestion.fetch_live --once --dry-run      # test: one cycle, prints messages, sends nothing
python -m ingestion.fetch_live --backfill-hours 30   # start for real: last 30 h first, then every 15 min
python -m ingestion.fetch_live --replay latest --speed 60   # demo backup: replay the last stored day
```

`scripts/run_batch.sh` runs the whole batch path in order in one go.

**Bigger dataset (big-data argument):** `python -m ingestion.stations --scope india` then the same download. All-India over several years is many GB and takes hours, so start it early or overnight. Check the size with `--dry-run` first.

---

## Facts checked against the docs on Nov 19 (re-check before quoting)

- OpenAQ API v3 free limits: **60 requests/minute, 2,000/hour**; over the limit returns HTTP 429. The fetcher paces itself at 55/min. One live cycle costs about 1 call per station.
- Endpoints used: `/v3/locations?bbox=minLon,minLat,maxLon,maxLat&monitor=true` (longitude first), `/v3/locations/{id}/latest`, `/v3/sensors/{id}/hours`.
- Archive: bucket `openaq-data-archive`, key `records/csv.gz/locationid=<id>/year=<yyyy>/month=<mm>/location-<id>-<yyyymmdd>.csv.gz`, columns `location_id,sensors_id,location,datetime,lat,lon,parameter,units,value`, datetime in ISO 8601 with a time-zone offset. Files appear about 72 hours after the end of the day.
- Open-Meteo: `wind_speed_unit=ms`, `timezone=GMT`, several coordinates per call allowed. Free for non-commercial use.

## Time convention (Riya: confirm this, then fill in docs/DATASET_NUMBERS.md)

The code labels every reading by its **timestamp truncated to the hour**, the same way for the archive (`datetime`) and the live API (`/latest` `datetime.utc`; for `/hours` it uses `period.datetimeTo`). So whatever OpenAQ's timestamps mean, history and live use the same rule.
**To confirm:** pick one station and one recent hour, compare the archive value with the `/v3/sensors/{id}/hours` value for the same hour, and write down whether OpenAQ's timestamp is the start or the end of the period. If it turns out to be the *start*, change `datetimeTo` to `datetimeFrom` in `fetch_live.backfill_messages`.

---

## What you must be able to explain in the viva

- Where the data comes from and why OpenAQ (same source for history and live, so they match).
- The 5 Vs of this dataset with **your measured numbers** (`outputs/dataset_numbers.md`).
- What PM2.5, PM10, NO₂, SO₂, CO and O₃ are, in one line each.
- What Kafka does and why the fetcher writes to Kafka instead of straight into Spark (if Spark restarts, no readings are lost; the stream job continues from its checkpoint).
- API rate limits and how the fetcher stays inside them (pacing, 429 handling, one call per station, each station-hour sent once).
- Why replay exists and why it is labelled `replay` (the dashboard shows a REPLAY banner, so stored data is never presented as live).
- Unit conversion: ppm → µg/m³ = ppm × molar mass × 1000 / 24.45 (25 °C, 1 atm).

## Your report sections (write by hand)

Data sources, dataset description with real numbers, data collection and ingestion design, EDA findings.
