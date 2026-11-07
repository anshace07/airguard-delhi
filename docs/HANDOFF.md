# AirGuard Delhi-NCR: full project handoff

Everything decided in the planning chat (Oct 2026), plus the state of the GitHub repo as checked on **Sat Nov 7, 2026**.
If you're picking this project up somewhere new (a new chat, a new teammate, a new machine), this file plus the repo is all you need.

Repo: **https://github.com/anshace07/airguard-delhi** (branch `main`)

---

## 0. Status on Nov 7, 2026 (read this first)

- **Repo on GitHub:** still only the 3 setup commits from Oct 16 (`3449c35`, `54cb8b0`, `308c91a`). No role code has been pushed and there are no other branches. If anyone built code locally, it is not on GitHub yet.
- **Milestone 1** (report Ch 1–2, Oct 5–9) and **Milestone 2** (live demo + viva, Oct 26–29) dates have passed. This chat has no record of how they went.
- **Final submission:** finals week **Nov 16–20**. **Hard LMS deadline: Nov 20, 2026**, and late requests are not entertained.
- **Two repo fixes agreed but not applied yet:** see section 18.
- **Offer to build all the code from one prompt:** made on Oct 16, never confirmed. Nothing was built beyond the setup.

---

## 1. The course: CSET-371 rubric (from the official PDF)

**Course:** CSET-371 Big Data Analytics and Business Intelligence, B.Tech 3rd year, 5th semester.

**Team rules**
- Individual or up to **4** students.
- All members must be from the **same Big Data Analytics lab batch**.

**Hard technology rules**
- **Apache Spark is mandatory.**
- Hadoop, Pig and Hive are allowed **only as supplementary** components.

**What the solution must include (a–j)**

| | Requirement |
|---|---|
| a | Real-world problem with clear objectives + an authentic, **faculty-approved dataset** |
| b | Real-time / streaming data analytics where applicable |
| c | End-to-end pipeline in Spark/PySpark: ingestion, preprocessing, cleaning, transformation, feature engineering, distributed processing |
| d | **At least 4–5 ML models in PySpark**, compared with metrics (Accuracy, Precision, Recall, F1, AUC-ROC, RMSE, MAE…) and charts (bar charts, confusion matrices, ROC curves, comparison tables) |
| e | Dashboard in Power BI, Tableau, Databricks or equivalent: insights, KPIs, trends, decision support |
| f | EDA and statistical analysis: patterns, trends, correlations, anomalies, insights |
| g | Advanced tech: Kafka, Airflow, Hadoop/HDFS, Hive, Spark Structured Streaming, Databricks, Docker, Kubernetes, Elasticsearch, cloud… |
| h | Scalable, large data, real-time or near-real-time |
| i | Alignment with UN Sustainable Development Goals |
| j | Decision support and practical impact |

**Milestones**

| Milestone | Marks | When | Deliverables | Graded on |
|---|---|---|---|---|
| 1 | 6 | Oct 5–9 (lab hours) | Report, 20+ pages, Chapters 1–2 | Problem understanding and justification, review of existing systems, feasibility, objectives, methodology, relevance of algorithms |
| 2 | 12 | Oct 26–29 | Report pages 23–42 (Chapters 3–4) appended to M1 | Innovation, viva on design and implementation, code understanding, demo of modules/datasets/running code/result graphs, **10–15 min group presentation + live demo**, results and validation |
| 3 | 6+6 = 12 | Nov 16–20 (final week) | Full report Ch 1–5, **45–50 pages**; complete source code with README + inline comments; complete dataset files | Report structure and format, attendance and participation, presentation clarity, **contribution by each team member**, project management and timeliness, SDG alignment |

Total out of 30, normalised to 10 marks.

**Plagiarism (Turnitin, Chapters 1–5)**

| Similarity | Marks |
|---|---|
| < 10% | Full |
| 10–15% | 80–90% |
| 15–20% | 60–75% |
| 20–30% | Under 60% |
| > 30% | Zero, not accepted |

**AI plagiarism must be 0%.** So the report is written by hand, by the team. AI can explain things; it cannot write report paragraphs.

**Final LMS checklist (by Nov 20)**
1. Complete report, Chapters 1–5, plagiarism-free
2. All dataset files used
3. Complete source code
4. Screenshots of results and outputs
5. Plagiarism report
6. Any supporting files

---

## 2. The project in plain words

**One line:** a system that watches Delhi's air pollution live, station by station, and predicts which areas will turn dangerous in the next few hours, before it happens.

**The problem:** Delhi has dozens of government monitoring stations, each measuring every hour. People mostly see one city-wide AQI number that describes what already happened. Nobody says "Anand Vihar will hit Severe in 3 hours."

**What it does**
1. **Collect:** every hour, pull the latest readings (PM2.5, PM10, NO₂, SO₂, CO, O₃) from every Delhi-NCR station, plus weather (wind, humidity, temperature), from free public APIs.
2. **Stream:** readings flow through **Kafka** (a pipe that never loses data) into **Spark** (the engine).
3. **Learn:** models trained on years of hourly history learn patterns like "low wind + rising PM at night → very bad morning."
4. **Predict:** for each new reading, forecast that station's PM2.5 **1, 3 and 6 hours ahead** and convert it to the CPCB category (Good → Severe).
5. **Alert:** warn when a specific station is about to enter Very Poor or Severe.
6. **Show:** a live map of Delhi coloured by air quality; click a station to see its forecast and alerts; a "what-if" panel (if wind speed rises, how much does pollution fall?); and BI pages for trends and model results.

**What a user sees:** Anand Vihar is orange on the map. Click it: "PM2.5 now 180, rising. Expected Severe in 3 hours. Alert active."

It's a weather forecast for pollution, per neighbourhood instead of per city.

**SDGs:** SDG 3 (Good Health) and SDG 11 (Sustainable Cities).

---

## 3. How the idea was picked

Requirement from Ansh: simple, solid, not fancy, follows every rubric point.

| Idea | Data | Verdict |
|---|---|---|
| **Delhi AQI forecasting + alerts** | CPCB/OpenAQ stations + live API + weather | **Picked.** Real streaming, real volume argument, local relevance, scores well on novelty. |
| Mandi crop price forecasting | Agmarknet via data.gov.in | Good Indian angle, few teams pick it. Check API reliability first. Backup option. |
| NYC taxi demand and fare | NYC TLC trip records (millions of rows/month) | Obviously big data, but foreign data, no local relevance, streaming is replayed. Backup option. |
| Hospital readmission | UCI Diabetes 130-US (~100k rows) | Weak: streaming must be faked, 100k rows is thin. |
| Credit card fraud | — | **Avoid.** Half the batch picks it, and novelty is graded. |

Spark MLlib supports LR, DT, RF, GBT, Naive Bayes, LinearSVC, KMeans, ALS, FM. It does **not** natively support Isolation Forest, XGBoost or LSTM, so the model list was chosen to stay inside MLlib.

---

## 4. The friend's brief (AirGuard Delhi-NCR) and what we changed

A teammate's friend shared a project brief. Verdict: **solid, use it.** Its technical calls were right:
- forecast PM2.5 then derive AQI;
- persistence baseline;
- time-based split;
- featureImportances instead of SHAP.

**From the brief, kept as-is**
- Pitch, architecture, evaluator Q&A, 3-minute demo story, risks (sections 15–16 below).
- Fixes (a)–(g):
  - (a) use CPCB's **6** categories;
  - (b) forecast the pollutant, then derive the index;
  - (c) historical vs live data may not match;
  - (d) no future information in features, split by time;
  - (e) MLlib has no SHAP and no prediction intervals, so use global feature importance and z-score anomaly rules;
  - (f) one model per horizon, drop 12 h;
  - (g) Naive Bayes needs non-negative features, so it's optional.
- Scope:
  - **MUST:** cleaned Parquet history with data-quality step; live API → Kafka → Structured Streaming; lags, rolling means, time features; PM2.5 forecast at 1/3/6 h with baseline + 4 regressors (MAE/RMSE/R²); category classification (LR/DT/RF, confusion matrix); hotspot ranking and threshold alerts; dashboard.
  - **SHOULD:** weather experiment; trend detection; spike alerts by z-score; feature importance chart; what-if simulator; patterns page.
  - **STRETCH:** KMeans station clusters; averaging ensemble; "pollution episode" rule.
  - **CUT:** 12 h horizon; confidence intervals; per-prediction SHAP; drift monitoring; 7-page dashboard.

**What we changed or added**
1. **Deadlines are real.** The brief said there was no deadline; the rubric has Oct 26–29 for the demo and Nov 20 for final.
2. **Dashboard split.** Power BI can't call a Spark model live when a slider moves, because its what-if parameters are just DAX formulas. So we use **BI tool for EDA + model results**, and **Streamlit for the live map + what-if** (it loads the saved model directly).
   - Power BI streaming datasets may be being phased out. This is unverified; prefer scheduled refresh.
3. **4 roles, not 5.** The team cap is 4; "report and coordination" was merged into the others.
4. **EDA beefed up.** EDA is graded separately: seasonal and Diwali/stubble spikes, correlation matrix, pollutant–weather links, missing-data patterns.
5. **Dataset:** the OpenAQ archive replaces Kaggle as the main source (section 6).
6. **Alert classifier added**, to get the ROC curve and AUC the rubric asks for.

---

## 5. Why it counts as big data, and how to prove it

**The 5 Vs**
- **Volume:** years of hourly readings × many stations × 6 pollutants.
- **Velocity:** a live hourly feed through Kafka. This is the strongest claim; most student projects fake streaming.
- **Variety:** pollution + weather + station metadata, different sources and formats.
- **Veracity:** sensor gaps, impossible values and duplicates to clean.
- **Value:** neighbourhood-level health alerts.

**The weak spot was volume.** A Delhi-only Kaggle file is probably a few million rows and a few hundred MB (unverified). Pandas on a laptop handles that, so an evaluator can ask "why Spark?"

**The fix**
1. Use **all India** stations, not just Delhi.
2. Use as many **years** as exist, roughly 2016 to now (to verify).
3. Use **all 6 pollutants**.
4. **Join hourly weather** for every station.
5. **Feature engineering** (lags and rolling windows per station) multiplies the data again; Spark earns its place here.
6. The **live stream** keeps adding data, and pandas can't consume a Kafka stream.
7. Scalability argument: the same code runs on a cluster unchanged.

**Size target (rule of thumb, not an official cutoff):** tens of millions of rows, several GB raw.

**Proof that wins the viva:** run the same heavy job in **pandas vs Spark** on the full data. Record time and memory, or pandas crashing with a MemoryError, and put the screenshot side by side in the report. Put **measured** numbers in Chapter 1 (rows, size, stations, rows after features), never guesses. `docs/DATASET_NUMBERS.md` is the place to record them.

---

## 6. Data sources

| Source | Used for | Notes |
|---|---|---|
| **OpenAQ archive on AWS S3** (`s3://openaq-data-archive/records/csv.gz/`) | Historical training data (**main dataset**) | Public, **no AWS account needed** (`aws s3 cp --no-sign-request …`). Gzipped CSVs, narrow format (one row per sensor/pollutant per hour), organised by location id / year / month, updated per location about every 72 h. |
| **OpenAQ API v3** | Live hourly readings | Free API key. Rate limits: the brief says 60/min and 2,000/hour (**re-check on the docs page**). Poll all stations in as few calls as possible. |
| **Open-Meteo** | Live + historical weather | No key for normal use (as far as known); free for non-commercial use (check terms). Call with `wind_speed_unit=ms`. |
| data.gov.in (CPCB) | Optional second live source | Free key; test fields and update rate yourself. |
| Kaggle "Air Quality Data in India" | Backup only | About 2015–2020, so it doesn't match today's live stations. |

**Why OpenAQ beats Kaggle:**
1. It's bigger.
2. It's the **same source as the live feed** (same stations, same format), which removes the history-vs-live mismatch.
3. It's up to date.

**Faculty approval of the dataset is required.** Get ma'am's OK on OpenAQ and keep the message as evidence.

---

## 7. Architecture

```
LIVE PATH (every hour)
OpenAQ API + Open-Meteo ──► fetch_live.py ──► Kafka topic "aq_live"
                                                  │
                                                  ▼
                              Spark Structured Streaming (foreachBatch)
                              clean → add_features() → saved models
                              → category, trend, hotspot rank, alerts
                                                  │
                                                  ▼
                              data/predictions/latest.csv ──► Streamlit live map + what-if

BATCH PATH (training, offline)
OpenAQ archive (S3) ──► data/raw ──► Spark clean + pivot ──► Parquet
                 ──► add_features() ──► train + compare models ──► ml/models + results.csv
                                                               ──► Tableau/Power BI pages
```

- **Shared rule:** batch and live both call the **same** `add_features()`, so the model always sees identical columns.
- **Fallback:** if the live API dies, **replay** a stored day through the same Kafka topic, clearly labelled `replay`.

---

## 8. Models (all Spark MLlib)

| Task | Models | Count | Gives the rubric |
|---|---|---|---|
| PM2.5 forecast at 1, 3, 6 h | **Persistence baseline** (not ML: "later = now"), Linear Regression, Decision Tree, Random Forest, GBT | 4 × 3 = **12** | MAE, RMSE, R², comparison bars |
| CPCB category 3 h ahead | Logistic Regression, Decision Tree, Random Forest | **3** | Accuracy, precision, recall, F1, **confusion matrix** |
| Alert: "Very Poor or worse in 3 h?" (yes/no) | Best binary classifier | **1** | **ROC curve, AUC** |
| Optional | KMeans station clusters | — | Clustering |
| Rules (not ML) | Spike detection (z-score of 1-hour change), trend (rising/stable/falling from last 3 h), hotspot rank | — | Anomaly detection, decision support |

That's 16 trained models + the baseline; the rubric asks for 4–5.

**Experiments:** with vs without weather ("does weather help?"), plus feature importance from RF and GBT.

**Why each model:**
- **LR:** is the relationship roughly straight-line?
- **DT:** captures rules like "low wind + night → spike."
- **RF:** many trees averaged, stable.
- **GBT:** trees fixing each other's mistakes, usually most accurate.

---

## 9. Technical rules (do not break these)

- **Split train/test by time**, never randomly. A random split lets the model see the future and fakes good scores.
- **No future data in features.** Lags and rolling windows look backwards only, and weather means weather at prediction time.
- **Lags must respect gaps.** Use range-based windows on the timestamp, so a missing hour gives a null lag instead of "3 rows back."
- **One model per horizon.**
- **Report honestly.** If a model can't beat persistence at some horizon (often the 1-hour one), say so.
- **Time is UTC in all data.** Use IST only for display and for time-of-day features.
- **`station_id` = OpenAQ location id**, everywhere.
- **Our category is an hourly indicator, not the official CPCB AQI.** The official AQI uses 24-hour averages and at least 3 pollutants. Say this in the report.
- **The live job's lag trap.** Each micro-batch only holds new readings, so in `foreachBatch`: append them, read back the last ~30 hours for those stations, run `add_features(with_targets=False)`, keep the newest row per station.
- **Write `latest.csv` atomically** (temp file + rename), so the dashboard never reads a half-written file.
- **Spark, not pandas, for big data.** Pandas only for small outputs (charts, dashboard).
- **Never commit** `data/`, `.env` or `ml/models/`. Never print API keys.

**CPCB categories (24-h breakpoints, µg/m³), coded in `common/aqi.py`**

| Category | AQI | PM2.5 | PM10 |
|---|---|---|---|
| Good | 0–50 | 0–30 | 0–50 |
| Satisfactory | 51–100 | 31–60 | 51–100 |
| Moderate | 101–200 | 61–90 | 101–250 |
| Poor | 201–300 | 91–120 | 251–350 |
| Very Poor | 301–400 | 121–250 | 351–430 |
| Severe | 401–500 | 250+ | 430+ |

Source: the National AQI breakpoint table (Arthapedia, ies.gov.in). Confirm against CPCB's technical document before quoting.

---

## 10. Team, roles and tech

| Role | Person | Branch | Folder | Laptop |
|---|---|---|---|---|
| 1. Data & Live Feed | **Riya Maurya** | `riya-m/ingestion` | `ingestion/` | Windows (WSL) |
| 2. Processing Engine + integration | **Ansh Singh** | `ansh/pipeline` | `pipeline/` | Windows (WSL), **demo machine** |
| 3. Prediction Models | **Ayush Mishra** | `ayush/ml` | `ml/` | Windows (WSL) |
| 4. Dashboard & Demo | **Riya Katiyar** | `riya-k/dashboard` | `dashboard/` | **MacBook** |

Restaurant analogy: Role 1 sources ingredients, Role 2 runs the kitchen, Role 3 writes the recipes, Role 4 runs the dining room.

**"Frontend vs backend":**
- **Frontend** = Role 4: Streamlit, Python-only web pages with no React needed.
- **Backend** = Roles 1–3: a data pipeline, not an API server. Spark writes predictions to a file and Streamlit reads it; no server sits in the middle.
- React + FastAPI was rejected: it adds a week and earns zero marks. It's a stretch goal only, after everything works.

**Mac problem:** Power BI Desktop doesn't run on Mac. Options:
1. **Tableau Public** (recommended): free, runs on Mac and Windows, explicitly allowed by the rubric. It saves to a public profile, which is fine for public air quality data.
2. Power BI web: needs a work or school email, and building reports there is limited.
3. Build Power BI on Ansh's Windows laptop.

Ask ma'am once whether Power BI specifically is required.

**Tech per person**
- **Everyone:** Python, Git/GitHub, Java 17, PySpark 3.5.9, Docker, Claude Code; WSL Ubuntu on the Windows laptops.
- **Riya Maurya:** OpenAQ API v3, OpenAQ S3 archive via AWS CLI, Open-Meteo, Kafka in Docker, `confluent-kafka` producer, PySpark + matplotlib for EDA.
- **Ansh:** Spark DataFrames, Spark SQL, window functions, Parquet, **Spark Structured Streaming** from Kafka (`foreachBatch`, checkpoints), Spark–Kafka connector, optional HDFS in Docker (ticks "Hadoop supplementary"), pandas only for the benchmark.
- **Ayush:** MLlib Pipeline, VectorAssembler, Imputer, 4 regressors, 3 classifiers, evaluators (MAE, RMSE, R², accuracy, P/R/F1, AUC), matplotlib.
- **Riya Katiyar:** Streamlit, pydeck or plotly maps, pandas for display, Tableau Public (or Power BI).

---

## 11. Per person: what, how, handoffs, viva, report

Full step lists and ready-to-paste Claude Code prompts are in each folder's README. Summary:

### Riya Maurya: Data & Live Feed
**Builds**
- `stations.py`: list Indian stations, check which Delhi-NCR ones still report.
- `download_archive.py`: OpenAQ archive. Do one year of Delhi first, measure it, then all India.
- `docs/DATASET_NUMBERS.md`: the real measured numbers.
- **Time convention:** decide whether the archive and API timestamps mean the start or end of the hour; use one for both.
- `fetch_live.py`: hourly, all Delhi-NCR stations plus Open-Meteo weather, validated with `validate_live_reading()`, sent to Kafka. `--once` for testing.
- `--replay YYYY-MM-DD [--speed N]`: pushes a stored day labelled `replay`.
- `weather_history.py`: historical weather for every station.
- `eda.py`: PySpark EDA (monthly/seasonal, hour of day, Oct–Nov spikes, top 10 worst stations, correlations, missing data), PNG + CSV in `outputs/eda/`.

**Key rule:** the live fetcher must run **nonstop on Ansh's laptop** (the demo machine) as early as possible, with sleep off and Docker open.

**Viva:** data source and why OpenAQ, the 5 Vs with measured numbers, what each pollutant is, why Kafka (no lost readings if Spark restarts), rate limits, why replay is labelled.

**Report:** data sources, dataset description with real numbers, ingestion design, EDA findings.

### Ansh Singh: Processing Engine + integration
**Builds**
- `clean_history.py`: pivot narrow → wide (one row per station-hour), normalise units, drop impossible values and duplicates, join weather, Parquet partitioned by year, print a data-quality summary.
- `common/features.py` `add_features(df, with_targets)`: lags 1/3/6 h, rolling 3/6/24 h, 1-hour change, IST time features, targets shifted forward 1/3/6 h. Plus a pytest.
- `build_features.py`, and a small sample (1 month, 5 stations) for Ayush to start on.
- `benchmark.py`: pandas vs Spark, with screenshots.
- `stream_job.py`: Kafka → parse with `live_reading_schema()` → `foreachBatch` (append, read last 30 h, features, newest row per station, save `latest_by_station.parquet`, load models, predict, `category_col`, trend, hotspot rank, alert) → atomic `latest.csv` + history. Checkpoint in `data/live/_checkpoint`. Use placeholder predictions until the real models exist.
- **Integration:** run the whole chain on his laptop, then a restart test (no lost or duplicated readings).
- Optional HDFS.

**Viva:** why Spark (benchmark numbers), why Kafka, micro-batches/checkpoints/`foreachBatch`, narrow vs wide, windowed lags and no leakage, the shared `add_features`, alert thresholds.

**Report:** architecture, big data justification, cleaning and quality, features, streaming design, alerts, integration and testing.

### Ayush Mishra: Prediction Models
**Builds**
- `data.py`: time-based split, feature lists for `pollutants_only` and `with_weather`.
- `train_regression.py`: baseline + 4 regressors × 3 horizons as Pipelines; save the best per horizon to `ml/models/pm25_h{1,3,6}/` + `feature_columns.json`; weather experiment.
- `train_classification.py`: 3 category classifiers + the binary alert model (ROC/AUC).
- `importance.py` and `charts.py`: comparison bars with baseline, confusion matrix, ROC, feature importance, predicted vs actual for one station over a week.
- `score.py`: the what-if function. **Test:** higher wind should usually lower the forecast; if it doesn't, explain why.

**Speed tip:** tune on 1 year, train final models on full data overnight.

**Viva:** each algorithm in 2 sentences, why the time split, what each metric means (for alerts **recall** matters most: missing a Severe hour is worse than a false alarm), one model per horizon, where models beat the baseline and where they don't, why MLlib not scikit-learn.

**Report:** algorithms, training setup, metrics, results, weather experiment, feature importance, limitations.

### Riya Katiyar: Dashboard & Demo (Mac)
**Builds**
- `make_sample.py`: fake predictions in the exact contract format, file name says SAMPLE, so she never waits on anyone.
- `app.py`, Streamlit with 2 pages:
  - **Live Map:**
    - 6 category colours with legend, 60-second auto-refresh;
    - click a station for current PM2.5, trend arrow, 1/3/6 h forecast and alert;
    - sidebar with active alerts and top 5 worst stations;
    - "last updated" in IST, a big REPLAY banner when the data is replayed.
  - **What-if:**
    - sliders for wind, humidity and current PM2.5 call `score()`;
    - shows the original vs new forecast;
    - caption: "Model scenario, not proof of cause."
- A sample/real data toggle.
- **Tableau**, 2 pages: (1) EDA and trends from Riya Maurya's CSVs; (2) model performance from Ayush's `results.csv`.
- `docs/DEMO_SCRIPT.md`, presentation slides, screenshots in `outputs/screenshots/`, the README run table.
- **Compiles the final report.**

**Viva:** the 6 categories and PM2.5 ranges, how the dashboard gets data, what each chart tells a decision maker, why what-if ≠ causation, SDG 3 and 11.

**Report:** dashboard design, BI insights, SDGs, demo walkthrough, compiling + formatting.

---

## 12. The original 12-day sprint (Fri Oct 16 → Tue Oct 27, now past)

Kept for reference; re-date it if you restart the build.

| Day | Date | Riya Maurya | Ansh | Ayush | Riya Katiyar | Handoffs / meetings |
|---|---|---|---|---|---|---|
| 1 | Oct 16 | Setup, OpenAQ key, AWS CLI, list stations | Repo, collaborators, fix everyone's setup | Setup, learn the 4 algorithms (StatQuest) | Mac setup, Tableau, sample data | **All:** `check_setup.py` all OK; agree CONTRACT.md |
| 2 | Oct 17 | Download 1 year of Delhi, measure | Kafka running; read archive format; start cleaning | Time split + baseline on small data | Live map on sample | — |
| 3 | Oct 18 | Full download; numbers; time convention | Clean job done; feature sample | Regression pipeline on sample | Colours, legend, station details | Riya M → Ansh: download script. Ansh → Ayush: sample |
| 4 | Oct 19 | Live fetcher (`--once`); historical weather | `add_features()` + full features | First real model | Alerts panel, worst 5, IST, refresh | Ansh → Ayush: feature code. Riya M + Ansh: live message out of Kafka |
| 5 | Oct 20 | **Fetcher 24/7 on Ansh's laptop** | Benchmark; stream job reads Kafka | 4 models × 3 horizons | What-if with placeholder | — |
| 6 | Oct 21 | Replay mode; EDA charts | Placeholder predictions written | Weather experiment; save models | Tableau page 1 layout | Ayush → Ansh: models (Drive). Riya M → Riya K: EDA (GitHub). All: 15-min show-and-tell |
| 7 | Oct 22 | Live data quality check | Real models; alerts, trend, hotspots | Category + alert classifiers, ROC | Map on real data; Tableau page 1 | Ansh → Riya K: `latest.csv` |
| 8 | Oct 23 | Fix fetcher issues | **First full end-to-end run** | results.csv, charts, importance | Tableau page 2 | **All:** watch the run, list bugs |
| 9 | Oct 24 | Test replay end to end | Fix bugs; restart test | `score()` | What-if wired to `score()` | Ayush ↔ Riya K on Ansh's laptop |
| 10 | Oct 25 | Fresh-clone test of README | Everything on demo laptop | Final retrain; freeze numbers | Polish; demo script; slides | **FEATURE FREEZE** |
| 11 | Oct 26 | Rehearse | Rehearse | Rehearse | Run rehearsal | Full demo twice; everyone explains with no notes |
| 12 | Oct 27 | Screenshots; package data | Final run; replay backup | Save charts + numbers | Screenshots, README | Final check |

**Rules**
1. The fetcher runs nonstop once started.
2. Stuck more than 2 hours → tell the group the same day.
3. Weekly walkthrough: each person explains their module to the others; if you can't explain it, you're not done.

**Daily routine**
- Morning: `source .venv/bin/activate`, then pull `main` into your branch.
- Work in Claude Code with "Read CLAUDE.md, CONTRACT.md and my folder README. Today I'm doing step X."
- Evening: commit + push, open a PR (Ansh merges), and post one line in the group: done, or blocked on what.

**Earlier time estimates**
- About 10 days for a good, precise version with 4 people + AI; 6–7 days is the aggressive floor.
- If AI writes all the code: about 4 days of building, plus 1–2 days fixing real-data surprises.
- What doesn't speed up with AI: the hand-written report, environment setup, viva understanding, and live data needing days to pile up.

---

## 13. The GitHub repo

**What's in it (all on `main`)**

| File | What it is |
|---|---|
| `README.md` | Overview, architecture diagram, team table, setup for Windows/WSL and Mac, run commands, git rules, AI rules, data sources |
| `CONTRACT.md` | **The shared formats.** Kafka message, file paths, feature/prediction columns, models, results, `score()`, AQI table, how to change it |
| `CLAUDE.md` | Rules every teammate's Claude Code follows automatically (read the contract first, Spark not pandas, time split, UTC, don't edit `common/` alone, no report prose) |
| `common/config.py` | All paths (`PATHS`), Kafka settings, `HORIZONS = (1, 3, 6)`, `model_path()`, Delhi-NCR bounding box, Spark memory |
| `common/aqi.py` | CPCB categories, breakpoints, colours, `pm25_category()`, `pm10_category()`, `pm_aqi()`, alert categories |
| `common/schemas.py` | Column lists, `live_reading_schema()` for Spark, `validate_live_reading()` |
| `common/spark.py` | `get_spark(app, kafka=False)` (local, UTC, laptop settings, matching Kafka connector), `category_col()` (no UDF) |
| `common/features.py` | **Stub only**: `add_features()` raises NotImplementedError until Ansh builds it |
| `infra/docker-compose.yml` | Kafka 3.9.2 single node (KRaft), topic `aq_live` (3 partitions), 30-day retention, auto topic creation |
| `scripts/check_setup.py` | Day 1 checker: Python, OS/WSL, Java 17, packages, `.env`, Docker, Spark job, `--kafka` |
| `requirements.txt` | `pyspark==3.5.9`, `setuptools`, `pyarrow`, `pandas<3`, `numpy`, `confluent-kafka`, `requests`, `python-dotenv`, `matplotlib`, `plotly`, `streamlit`, `pytest` |
| `tests/test_common.py` | 20 tests for AQI, paths and schemas (all pass) |
| `ingestion/`, `pipeline/`, `ml/`, `dashboard/` `README.md` | Each owner's steps, deliverables + dates, done criteria, viva points, report sections, **ready-to-paste Claude Code prompt** |
| `docs/TIMELINE.md` | The 12-day sprint table |
| `docs/RUBRIC_CHECKLIST.md` | Every rubric item → evidence → owner, with tick boxes; who writes which report section |
| `docs/DATASET_NUMBERS.md` | Empty template for measured numbers + benchmark + time convention |
| `data/README.md` | What goes in each `data/` folder (never committed) |
| `.gitignore`, `.env.example`, `pyproject.toml` | Ignore data/models/.env; template for keys; pytest config |

**Setup (every laptop)**
- **Windows:**
  1. `wsl --install -d Ubuntu`.
  2. Docker Desktop with WSL integration on.
  3. In Ubuntu: `sudo apt install -y openjdk-17-jdk python3-venv python3-pip git`.
  4. `java -version` must say 17 (if not: `sudo update-alternatives --config java`).
  5. Clone into `~` (not `/mnt/c`).
- **Mac:** Homebrew `openjdk@17`, `python@3.12`, `git`; symlink the JDK; set `JAVA_HOME`; Docker Desktop.
- **Both:**
  ```bash
  python3 -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  cp .env.example .env            # Role 1 + demo laptop: add OPENAQ_API_KEY
  python scripts/check_setup.py   # all OK
  docker compose -f infra/docker-compose.yml up -d
  python scripts/check_setup.py --kafka
  ```

**Git rules**
1. Work on your own branch.
2. Every morning, merge `main` into your branch.
3. Push daily and open a PR into `main`; Ansh merges.
4. Stay in your folder. Changing `CONTRACT.md` or `common/` needs the whole team's agreement and goes in one PR.
5. Never commit `data/`, `.env` or `ml/models/`.

**Tested on Oct 16 (clean Linux machine)**
- ✅ 20 unit tests pass.
- ✅ `check_setup.py` all OK on Java 17.
- ✅ Spark 3.5.9 job runs.
- ✅ JSON parsing with the contract schema works.
- ✅ `category_col` works.
- ✅ MLlib Random Forest Pipeline trains, saves and reloads.
- ✅ Spark ↔ pandas conversion works on Java 17 (pandas 2.3 and 3.0, Arrow on and off).
- ❌ **Java 21:** Spark's Arrow/pandas conversion crashes, so **Java 17 is required.**
- ❌ **Python 3.12+:** Spark 3.5 needs `distutils`, so `setuptools` was added.
- ⚠️ **Not tested** (blocked in the cloud sandbox): running Kafka in Docker, downloading the Spark–Kafka connector from Maven, real OpenAQ/Open-Meteo calls. These must be confirmed on a laptop: `check_setup.py --kafka` and the first `get_spark(kafka=True)`.

---

## 14. Shared formats (summary of CONTRACT.md; the repo file wins if they differ)

**Kafka:** broker `localhost:9092` (`kafka:29092` from containers), topic `aq_live`, key `str(station_id)`, value = one JSON live reading.

**Live reading (18 fields):**
- `station_id` (int), `station_name`, `city`, `lat`, `lon`
- `ts_utc` (ISO, ends in Z)
- `pm25`, `pm10`, `no2`, `so2`, `co`, `o3` (µg/m³, null if missing)
- `temperature_c`, `humidity_pct`, `wind_speed_ms`, `wind_dir_deg`
- `source` (`live` or `replay`), `ingested_at_utc`

One reading per `(station_id, ts_utc)`; on duplicates, keep the latest `ingested_at_utc`.

**Paths**

| Path | Contents |
|---|---|
| `data/raw/openaq/`, `data/raw/weather/`, `data/raw/stations.csv` | Role 1 |
| `data/clean/readings/` | Parquet by year |
| `data/features/history/` | Training features |
| `data/features/latest_by_station.parquet` | Newest feature row per station, for `score()` |
| `data/live/readings/` | Live readings |
| `data/live/_checkpoint` | Streaming checkpoint |
| `data/predictions/latest.csv` | For the dashboard |
| `data/predictions/history/` | All predictions |
| `data/samples/` | SAMPLE files |
| `ml/models/pm25_h1/`, `pm25_h3/`, `pm25_h6/`, `category/`, `alert_h3/`, `feature_columns.json` | Saved models |
| `ml/results/results.csv`, `feature_importance.csv`, `outputs/charts/`, `outputs/eda/` | Results and charts |

**Feature columns**
- All reading columns
- `pm25_lag_1h`, `pm25_lag_3h`, `pm25_lag_6h`, `pm10_lag_1h`, `pm10_lag_3h`, `pm10_lag_6h`
- `pm25_roll_3h`, `pm25_roll_6h`, `pm25_roll_24h`, `pm25_change_1h`
- `hour`, `day_of_week`, `month`, `is_weekend` (IST)
- `target_pm25_1h`, `target_pm25_3h`, `target_pm25_6h` (training only)

**Prediction row (one per station × horizon):**
- `station_id`, `station_name`, `city`, `lat`, `lon`, `ts_utc`
- `horizon_h`, `target_ts_utc`
- `current_pm25`, `current_category`, `predicted_pm25`, `predicted_category`
- `trend`, `hotspot_rank`, `alert` (1/0), `alert_reason`
- `source`, `model_name`, `scored_at_utc`

**results.csv:**
- `task`, `model`, `horizon_h`, `feature_set`
- `mae`, `rmse`, `r2`, `accuracy`, `precision`, `recall`, `f1`, `auc`
- `n_train`, `n_test`, `train_period`, `test_period`

**What-if:** `score(station_id, horizon_h, overrides=None)` → `{station_id, horizon_h, predicted_pm25, predicted_category}`.

---

## 15. Pitch, demo, evaluator questions, risks

**30-second pitch (say it in your own words):** every winter Delhi's air turns Severe, and the warning most people get is one city-wide number that's already hours old. AirGuard treats every station as its own forecast: live readings stream through Kafka into Spark, it predicts 1, 3 and 6 hours ahead, and warns the specific neighbourhood about to turn severe. It shows which factors drive forecasts, and a what-if panel shows how wind or humidity would change the outcome. Big data isn't decoration: years of hourly station history to learn from, and a live stream to act on.

**3-minute demo, one story**
1. Live map: stations coloured by the 6 categories, updating live.
2. Click the worst station: current PM2.5, trend arrow, the 1/3/6 h forecast.
3. An alert firing for a specific neighbourhood.
4. Model performance: baseline vs 4 models per horizon. Say plainly where models help and where they don't.
5. What-if: drag wind up and watch the forecast fall. It's a model scenario, not proof of cause.

**Backup:** if live fails, replay a stored day (banner shows REPLAY).

**Questions evaluators will ask**
- *Is the streaming real?* Yes. It's a live hourly API; history is only for training; replay is labelled if used.
- *Why not "next hour = this hour"?* That's our baseline; every model is compared against it per horizon.
- *Why Spark and Kafka?* Spark for multi-station history and features at scale, with the same code for stream scoring; Kafka decouples ingestion, so a slow API or restart loses nothing.
- *What's new?* Station-level early warnings, hotspot and trend detection on a live stream, a measured 4-algorithm × 3-horizon comparison, a what-if tool, and a city-agnostic pipeline.
- *Does what-if prove causation?* No, it's the model's prediction under changed inputs.
- *Why not pandas?* Point to the benchmark numbers.

**Risks and fallbacks**

| Risk | Fallback |
|---|---|
| Dataset rejected | Mandi prices or NYC taxi; the approach transfers |
| API slow, limited or dropping stations | Cache, cap calls, replay |
| Kafka/Spark setup eats days | Docker Compose, Spark local mode, start with replay |
| History and live disagree | Retrain on a recent window, report it as a finding |
| Scope creep | Freeze MUST; everything else goes to STRETCH |

---

## 16. The report

**Who writes what (by hand; 0% AI)**

| Person | Sections |
|---|---|
| Riya Maurya | Data sources, dataset numbers, ingestion, EDA findings |
| Ansh | Architecture, big data justification + benchmark, cleaning, features, streaming, alerts, integration/testing |
| Ayush | Algorithms, training setup, metrics, results, weather experiment, feature importance, limitations |
| Riya Katiyar | Dashboard design, BI insights, SDGs, demo walkthrough, **compiles + formats everything** |

**Chapter 1, Introduction**
- Background: Delhi air quality and the health case for early warning.
- Problem statement.
- 4–5 measurable objectives.
- Why big data: the 5 Vs with **real numbers**.
- SDG 3 and 11.
- Scope and limitations.
- Report organisation.

**Chapter 2, Literature and background**
- AQI basics and CPCB categories.
- 5–8 papers on AQI/PM2.5 forecasting, each with its gap (each member finds 2).
- The big data stack (Spark/MLlib, Kafka, Structured Streaming) and why each.
- The models and why they suit the problem.
- The research gap: station-level streaming alerts and a measured comparison across horizons.

**Chapters 3–4:** implementation, results and validation. **Chapter 5:** conclusion and future work. (Use the official report format.)

**Research questions**
1. Which model forecasts best, and does it beat persistence?
2. Does weather improve accuracy over pollutants alone?
3. Can the stream catch rapid deterioration before a batch process would?
4. Which features matter most?

---

## 17. What AI can and can't do on this project

- **AI can write about 90% of the code:** all four roles, tested on fake data in the exact OpenAQ format, pushed as one PR per role so each owner reviews "their" part, and fixed from pasted errors.
- **AI can't, from the cloud chat:** run the real system (OpenAQ download, live APIs, Docker/Kafka and the Maven connector are blocked there), keep the fetcher running 24/7 (it must run on a laptop), or click together the Tableau pages (Riya Katiyar does this from the CSVs and a chart list, about 3 hours).
- **Humans only, no matter what:**
  - the **report** (0% AI);
  - the **viva** (each person explains their own module line by line; "contribution by each member" and "code understanding" are graded);
  - dataset approval;
  - the presentation.

Ansh's working style: he hands builds to Claude Code himself. Each folder README ends with a ready-to-paste Claude Code prompt, and the APIs/keys needed are listed in each README.

---

## 18. Open items

**Agreed fixes NOT yet applied to the repo**
1. The live fetcher must run on **Ansh's laptop** (the demo machine), not "a laptop that stays on". Live data collected on another laptop never reaches the demo Kafka. Update `ingestion/README.md` and `docs/TIMELINE.md`.
2. Handoffs are **"run my script on your laptop"**, not "send my data". The data is GBs and never goes on GitHub. Small files (results CSVs, charts, code) go via GitHub; model folders and samples via Google Drive.

**Things to verify before quoting in the report**
- OpenAQ v3 rate limits and exact endpoint/field names (inspect a real response).
- OpenAQ timestamp convention (period start vs end), the same for archive and API.
- How many Delhi-NCR stations report live today.
- Real dataset size (rows, GB, stations, years, missing %). Measure; don't estimate.
- `common/aqi.py` `SEVERE_TOP` (PM2.5 380, PM10 510) is an **assumption** for where Severe reaches AQI 500. It changes only the number inside Severe, never the category. Check CPCB's technical document.
- PM10 boundary at 430: the source table lists it in both Very Poor and Severe.
- Open-Meteo free-use terms; Power BI streaming status (if Power BI is used).
- Whether ma'am requires Power BI specifically or accepts Tableau.
- Faculty approval of OpenAQ as the dataset.

**Pending actions from Oct 16**
- Add the 3 teammates as GitHub collaborators (Settings → Collaborators).
- Everyone completes setup until `check_setup.py` is all OK.

---

## 19. What's left for the final submission (by Nov 20)

From the rubric, regardless of how Milestone 2 went:
1. **Working system** with screenshots of every step.
2. **Full report**, Chapters 1–5, 45–50 pages, under 10% similarity, **0% AI**, plus the plagiarism report.
3. **Complete source code** on GitHub with README and inline comments.
4. **Dataset files**, or (if too big) the download scripts + exact sources. Confirm with ma'am.
5. Final presentation; **every member able to explain their own part**.

If none of the role code exists yet, the order is: data download + fetcher first (live data needs days to pile up), then pipeline, then models, then dashboard, then freeze and write.

---

## 20. Continuing in a new chat

Paste this to start:

> I'm continuing the AirGuard Delhi-NCR project for CSET-371 (Big Data Analytics, Bennett University). Repo: github.com/anshace07/airguard-delhi. Read `docs/HANDOFF.md`, `CONTRACT.md`, `README.md` and `CLAUDE.md` first. Team: Riya Maurya (Role 1 data and live feed), Ansh Singh (Role 2 Spark engine and integration, demo laptop), Ayush Mishra (Role 3 models), Riya Katiyar (Role 4 dashboard and demo, Mac). Final submission is due Nov 20, 2026. Current status: [fill in what's built and what's not].
