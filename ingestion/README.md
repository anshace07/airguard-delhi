# Role 1: Data & Live Feed

**Owner:** Riya Maurya · **Branch:** `riya-m/ingestion` · **Works closest with:** Ansh (Role 2), Riya Katiyar (Role 4)

**Your job in one line:** get all the data into the system: years of old readings for training, and a live reading every hour into Kafka.

Everything else in the project starts from your data. If your part stops, nothing runs.

---

## What you deliver

| Deliverable | Goes to | By |
|---|---|---|
| `data/raw/stations.csv` + OpenAQ archive files in `data/raw/openaq/` | Ansh | **Day 3 (Sun Oct 18)** |
| Dataset numbers: rows, size on disk, stations, date range, % missing (write them in `docs/DATASET_NUMBERS.md`) | Report, everyone | Day 3 |
| Historical weather in `data/raw/weather/` | Ansh | Day 4 (Mon Oct 19) |
| Live fetcher sending to Kafka **and left running nonstop** | Ansh | **Day 5 (Tue Oct 20)** |
| Replay mode | Ansh, demo backup | Day 6 (Wed Oct 21) |
| EDA charts (PNG + CSV) in `outputs/eda/` | Riya Katiyar | **Day 6 (Wed Oct 21)** |

Formats: CONTRACT.md sections 3 and 4.

---

## Accounts and tools to get first (Day 1)

- **OpenAQ account + API key** (free): sign up on [explore.openaq.org](https://explore.openaq.org), copy the key into `.env` as `OPENAQ_API_KEY`.
- **AWS CLI** (to download the archive; **no AWS account needed**):
  ```bash
  sudo apt install -y unzip && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip && unzip awscliv2.zip && sudo ./aws/install
  aws s3 ls --no-sign-request s3://openaq-data-archive/records/csv.gz/ | head
  ```
- **Open-Meteo**: no key needed for normal use. Read their terms once (free for non-commercial use).
- Optional: data.gov.in key for the CPCB feed as a second live source.

---

## Steps

1. **Find the stations.** Use the OpenAQ API to list every location in India (Delhi-NCR first). Save `data/raw/stations.csv` with `station_id, station_name, city, lat, lon`. Check which Delhi stations **are still reporting today**.
2. **Download one year for Delhi first.** Measure it: rows, size, stations, missing %. Then estimate the full India multi-year size. Download the full set if the laptop disk allows (check free space first).
3. **Decide the time convention.** OpenAQ readings cover an hour; check whether the archive's time column and the API's time field mean the start or the end of that hour. Pick one, use it for both, and write it at the bottom of this README. (CONTRACT.md section 1.)
4. **Live fetcher** `ingestion/fetch_live.py`: every hour, get the latest readings for all Delhi-NCR stations in as few API calls as possible (rate limits), add current weather from Open-Meteo for each station's location, build one message per station exactly as in CONTRACT.md section 3, check it with `common.schemas.validate_live_reading()`, send to Kafka topic `aq_live` with key `str(station_id)`.
5. **Start it and leave it running from Day 5.** Use a laptop that stays on, and keep it running until the demo. More days of live data make the demo stronger.
6. **Replay mode:** `python -m ingestion.fetch_live --replay 2025-11-05` pushes one stored day from the archive into the same topic, with `source = "replay"`. Optional `--speed 60` to send one hour every minute. This is the demo backup if the API or internet fails.
7. **Historical weather** for every station's location and the full training period (Open-Meteo historical API), saved in `data/raw/weather/`.
8. **EDA charts**, made in **PySpark** on the full data (not pandas). Save each as PNG + the CSV behind it in `outputs/eda/`:
   - monthly and seasonal PM2.5 pattern
   - hour-of-day pattern
   - Diwali / late-October-November spikes
   - top 10 worst stations
   - correlation between pollutants and weather
   - missing data by station

**Done when:** the fetcher has run unattended for 24 hours with no gaps, `docker exec ... kafka-console-consumer` shows real messages, replay works, and the EDA files are in `outputs/eda/`.

---

## What you must be able to explain in the viva

- Where the data comes from and why OpenAQ (same source for history and live, so they match).
- The 5 Vs of this dataset with **your measured numbers**.
- What PM2.5, PM10, NO₂, SO₂, CO and O₃ are, in one line each.
- What Kafka does and why the fetcher writes to Kafka instead of straight into Spark (if Spark restarts, no readings are lost).
- API rate limits and how your fetcher stays inside them.
- Why replay exists and why it is labelled `replay`.

## Your report sections (write by hand)

Data sources, dataset description with real numbers, data collection and ingestion design, EDA findings.

---

## Starter prompt for your AI coding tool

Copy-paste this into Claude Code from the repo root:

```
Read CLAUDE.md, CONTRACT.md and ingestion/README.md first.

Build my part (Role 1) step by step inside ingestion/. Stop after each step, run it, and explain it to me in simple words before continuing.

1) stations.py: use the OpenAQ v3 API (key from common.config.OPENAQ_API_KEY) to list all locations in India and save data/raw/stations.csv (station_id, station_name, city, lat, lon). Mark which Delhi-NCR stations (common.config.DELHI_NCR_BBOX) reported in the last 24 hours. Check docs.openaq.org for the exact v3 endpoints; do not guess field names, inspect a real response first.

2) download_archive.py: download the OpenAQ archive (public S3 bucket openaq-data-archive, records/csv.gz/, no AWS account needed) for given station ids and years into data/raw/openaq/. Start with one year of Delhi stations. Then write a PySpark script that reports rows, size on disk, number of stations, date range and % missing, and save the numbers to docs/DATASET_NUMBERS.md.

3) fetch_live.py: every hour, fetch the latest readings for all Delhi-NCR stations using as few OpenAQ calls as possible (respect rate limits, retry with backoff on errors), fetch current weather from Open-Meteo for each station (wind_speed_unit=ms), build one message per station exactly matching CONTRACT.md section 3, validate it with common.schemas.validate_live_reading, and send it to Kafka (common.config.KAFKA_BOOTSTRAP, KAFKA_TOPIC) with confluent-kafka, key = str(station_id). Log a one-line summary every cycle. Add --once for a single cycle (for testing).

4) Add --replay YYYY-MM-DD [--speed N] to fetch_live.py: read that day from data/raw/openaq/, build the same messages with source="replay", send them in time order.

5) weather_history.py: download hourly historical weather from Open-Meteo for every station location over the archive's date range into data/raw/weather/.

6) eda.py: PySpark EDA on the full data (monthly/seasonal, hour-of-day, Oct-Nov spikes, top 10 worst stations, pollutant-weather correlations, missing data by station). Save each chart as PNG plus its CSV in outputs/eda/.
```
