# Rubric checklist

Every requirement in the CSET-371 project brief → what proves it → who owns it.
Tick a box only when the evidence (file, screenshot, number) actually exists.

## Expected solution (brief section 3)

| # | Rubric requirement | Our evidence | Owner | Done |
|---|---|---|---|---|
| a | Problem definition + faculty-approved dataset | Problem statement in report; ma'am's approval of OpenAQ (save the message/email) | Riya Maurya | [ ] |
| b | Real-time data analytics | Live OpenAQ → Kafka → Spark Structured Streaming; days of live data collected | Riya Maurya + Ansh | [ ] |
| c | End-to-end pipeline in Spark: ingestion, cleaning, transformation, features | `ingestion/download_archive.py`, `pipeline/clean_history.py` (+ `outputs/data_quality.csv`), `common/features.py`, `pipeline/build_features.py` | Ansh | [ ] |
| d | 4–5 ML models in PySpark, compared with metrics + charts (bars, confusion matrix, ROC, tables) | `ml/train_regression.py`, `ml/train_classification.py` → `ml/results/results.csv`, `outputs/charts/` (regression_comparison, confusion_matrix, roc_curve, feature_importance, weather_experiment) | Ayush | [ ] |
| e | Power BI / Tableau / Databricks dashboard with KPIs and trends | Streamlit BI pages "Trends (BI)" + "Model performance" (KPIs, trends, decision support); Tableau optional from the same CSVs | Riya Katiyar | [ ] |
| f | EDA and statistical analysis | `ingestion/eda.py` → `outputs/eda/` (seasons, daily cycle, Oct-Nov/Diwali, correlation, weather bins, missing data, spikes) + written findings | Riya Maurya | [ ] |
| g | Advanced tech integration (Kafka, Docker, Hadoop/HDFS, Structured Streaming…) | Kafka in Docker, Structured Streaming; optional HDFS | Ansh | [ ] |
| h | Scalable, large data, near-real-time | `outputs/dataset_numbers.md` + `pipeline/benchmark.py` (`outputs/benchmark.csv`) + live latency | Ansh | [ ] |
| i | SDG alignment | SDG 3 + SDG 11 section | Riya Katiyar | [ ] |
| j | Decision support and practical impact | `pipeline/stream_job.py` alerts + reasons, hotspot rank, trend; dashboard What-if page (`ml/score.py`) | Ansh + Riya Katiyar | [ ] |
| — | **Spark is mandatory**; Hadoop/Pig/Hive only supplementary | All heavy processing and ML in Spark | Ansh + Ayush | [ ] |

## Milestone 2 (Oct 26–29, 12 marks)

| Requirement | Evidence | Owner | Done |
|---|---|---|---|
| Report pages 23–42: Chapters 3 + 4 appended to Milestone 1 | Report draft | All, compiled by Riya Katiyar | [ ] |
| Innovation / novelty | Station-level alerts, live stream, what-if, horizon comparison | All | [ ] |
| Viva on design and implementation | Every person explains their module with no notes | All | [ ] |
| Code understanding + demo of modules, datasets, running code, result graphs | Live demo on Ansh's laptop | All | [ ] |
| 10–15 minute group presentation + live demo | Slides + demo script | Riya Katiyar | [ ] |
| Results and validation | Baseline vs models, per horizon | Ayush | [ ] |

## Final submission (Nov 16–20, LMS)

| Item | Owner | Done |
|---|---|---|
| Complete report, Chapters 1–5, 45–50 pages, plagiarism < 10%, **AI plagiarism 0%** | All | [ ] |
| All dataset files used (or download script + exact sources if too large; check with ma'am) | Riya Maurya | [ ] |
| Complete source code with README and inline comments | All | [ ] |
| Screenshots of results and outputs | Riya Katiyar (collects from everyone) | [ ] |
| Plagiarism report | Riya Katiyar | [ ] |
| Supplementary files | All | [ ] |

## Report: who writes what (by hand)

| Section | Writer |
|---|---|
| Data sources, dataset numbers, ingestion, EDA findings | Riya Maurya |
| Architecture, big data justification, cleaning, features, streaming, alerts | Ansh |
| Algorithms, training setup, metrics, results, weather experiment, limitations | Ayush |
| Dashboard design, BI insights, SDGs, demo walkthrough, compiling + formatting | Riya Katiyar |
