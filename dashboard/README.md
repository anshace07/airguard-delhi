# Role 4: Dashboard & Demo

**Owner:** Riya Katiyar · **Branch:** `riya-k/dashboard` · **Works closest with:** Ansh (predictions), Ayush (results, `score()`), Riya Maurya (EDA)

**Your job in one line:** build everything the evaluator sees: the live map, the what-if panel and the BI dashboard. Own the demo.

You never wait for anyone: start today with fake sample data in the exact contract format, then switch to real data when it arrives.

---

## What you deliver

| Deliverable | From | By |
|---|---|---|
| Sample predictions file (fake, labelled SAMPLE) | you | **Day 1–2** |
| Streamlit live map working on sample data | you | Day 4 (Mon Oct 19) |
| BI page 1: EDA and trends (from Riya Maurya's `outputs/eda/`) | Riya Maurya | Day 7 (Thu Oct 22) |
| Live map on **real** `data/predictions/latest.csv` | Ansh | **Day 7 (Thu Oct 22)** |
| BI page 2: model performance (from Ayush's `results.csv`) | Ayush | Day 8 (Fri Oct 23) |
| What-if page calling Ayush's `score()` | Ayush | **Day 9 (Sat Oct 24)** |
| Demo script, README run table, screenshots in `outputs/screenshots/` | you | Day 10–12 |

Formats: CONTRACT.md sections 6 and 7.

---

## Which BI tool

Power BI Desktop does not run on Mac. Use **Tableau Public** (free, Mac and Windows; the rubric explicitly allows Tableau). Ask ma'am once whether Power BI specifically is required; if it is, build the Power BI pages on Ansh's Windows laptop on Days 9–10.

Note: Tableau Public saves workbooks to your public Tableau profile. That's fine here because the data is public air quality data.

---

## Steps

1. **Sample data** (`dashboard/make_sample.py`): ~30 fake stations around Delhi with fake values in the exact CONTRACT.md section 6 columns, 3 rows per station (1/3/6 h). Save to `data/samples/SAMPLE_predictions_latest.csv`. File name must say SAMPLE.
2. **Streamlit app** (`dashboard/app.py`), 2 pages:
   - **Live Map:** each station a dot coloured by its CPCB category (`common.aqi.CATEGORY_COLORS`), a legend with all 6 categories, auto-refresh every 60 s. Click/select a station: current PM2.5, category, trend arrow, the 1/3/6 h forecasts, alert text. Side panel: active alerts and top 5 worst stations. Show "last updated" time in **IST**. If `source` is `replay`, show a clear "REPLAY DATA" banner.
   - **What-if:** pick a station and horizon; sliders for wind speed, humidity and current PM2.5; call `ml.score.score()`; show the new forecast and category next to the original. Caption: "Model scenario, not proof of cause."
3. **Switch from sample to real data** with one setting (e.g. a sidebar toggle or env var), so the sample stays as a backup.
4. **BI dashboard (Tableau)**, 2 pages:
   - Page 1, EDA & trends: monthly/seasonal patterns, hour-of-day, worst stations, pollutant correlations (Riya Maurya's CSVs).
   - Page 2, model performance: baseline vs 4 models per horizon (MAE/RMSE), weather experiment, confusion matrix, feature importance (Ayush's CSVs and charts).
5. **Demo script** (3 minutes, one story) in `docs/DEMO_SCRIPT.md`: live map → worst station → alert firing → model performance → what-if. Plus the backup plan: if live fails, replay.
6. **Screenshots** of every step for submission (`outputs/screenshots/`), and keep the root README's "Running things" table up to date.

**Done when:** the map shows real live predictions and refreshes on its own, the what-if sliders change the forecast, both BI pages show real results, and the demo has been rehearsed twice.

---

## What you must be able to explain in the viva

- The 6 CPCB categories and their PM2.5 ranges.
- How the dashboard gets its data (reads the predictions file the Spark job writes every batch).
- What each chart on each page shows and what it tells a decision maker.
- Why the what-if panel is a model scenario, not proof of cause.
- Which SDGs the project serves (SDG 3 Good Health, SDG 11 Sustainable Cities) and how.

## Your report sections (write by hand)

Dashboard and visualisation design, BI insights, SDG alignment, demo/user walkthrough.

---

## Starter prompt for your AI coding tool

```
Read CLAUDE.md, CONTRACT.md and dashboard/README.md first.

Build my part (Role 4) step by step in dashboard/. Stop after each step, run it, and explain it to me in simple words before continuing.

1) make_sample.py: generate data/samples/SAMPLE_predictions_latest.csv with ~30 fake stations around Delhi (lat 28.4-28.9, lon 76.9-77.4), 3 rows per station (horizon_h 1, 3, 6), exactly the columns and order in common.schemas.PREDICTION_FIELDS, realistic values across all 6 categories, a few alerts, source="live". Name stations "Sample Station 1..30".

2) app.py: Streamlit multipage app. Put the repo root on sys.path at the top so "from common..." works. Page "Live Map": read the predictions file (sidebar toggle: sample vs real common.config.PATHS.predictions_latest), map with pydeck or plotly, dots coloured with common.aqi.CATEGORY_COLORS, legend with all 6 categories, auto-refresh every 60 s, station selector showing current PM2.5, category, trend arrow, 1/3/6 h forecasts and alert reason, sidebar with active alerts and top 5 worst stations, "last updated" in IST, a big banner if source == "replay".

3) Page "What-if": station + horizon selectors, sliders for wind_speed_ms, humidity_pct and current pm25, call ml.score.score(station_id, horizon_h, overrides). Until ml/score.py exists, use a clearly labelled placeholder function. Show original vs new forecast and category side by side, caption "Model scenario, not proof of cause."

Keep the design clean and readable on a projector: large fonts, high contrast.
```
