# 12-day sprint: Fri Oct 16 → Tue Oct 27, 2026

Milestone 2 (live demo + viva) is in the week of **Oct 26–29**. If your lab slot is **Oct 26**, squeeze Days 11–12 into Sun Oct 25.

The **Handoffs** column is what matters most: if yours slips, tell the group **that same day**.

| Day | Date | Riya Maurya (Data) | Ansh (Engine) | Ayush (Models) | Riya Katiyar (Dashboard) | 🤝 Handoffs / meetings |
|---|---|---|---|---|---|---|
| 1 | Fri Oct 16 | Setup. OpenAQ key. AWS CLI. List stations | Repo live, collaborators added, fix everyone's setup | Setup. Read up on the 4 algorithms | Setup (Mac). Tableau Public. Sample data file | **All 4:** everyone passes `check_setup.py`; read CONTRACT.md together and agree it's final |
| 2 | Sat Oct 17 | Download 1 year of Delhi data, measure it | Kafka running; read archive format; start cleaning job | Time split + baseline on any small data | Live map on sample data | — |
| 3 | Sun Oct 18 | Full download; `DATASET_NUMBERS.md`; time convention decided | Clean job done; feature sample | Regression pipeline on the sample | Map: colours, legend, station details | **Riya M → Ansh:** raw data. **Ansh → Ayush:** feature sample |
| 4 | Mon Oct 19 | Live fetcher sending to Kafka (`--once` works); historical weather | `add_features()` + full features | First real model on full features | Alerts panel, worst-5 list, IST time | **Ansh → Ayush:** full features. **Riya M + Ansh:** a live message comes out of Kafka |
| 5 | Tue Oct 20 | **Fetcher running 24/7 from today** | Benchmark done; streaming job reads Kafka, builds features | 4 models × 3 horizons | What-if page with placeholder | — |
| 6 | Wed Oct 21 | Replay mode; EDA charts | Streaming job writes placeholder predictions | Weather experiment; save best models | Tableau page 1 layout | **Ayush → Ansh:** saved models. **Riya M → Riya K:** EDA files. **All 4:** 15-min show-and-tell |
| 7 | Thu Oct 22 | Live data quality check (gaps, dead stations) → tell Ansh | Real models plugged in; alerts, trend, hotspot rank | Category + alert classifiers, ROC | Map on real predictions; Tableau page 1 done | **Ansh → Riya K:** live `latest.csv`. **Ansh + Riya K:** agree how alerts look |
| 8 | Fri Oct 23 | Fix fetcher issues from the full run | **First full end-to-end run** on the demo laptop | results.csv, charts, feature importance | Tableau page 2 (model results) | **All 4 together:** watch the full run, write down every bug. **Ayush → Riya K:** results |
| 9 | Sat Oct 24 | Test replay through the whole system | Fix Day 8 bugs; restart test | `score()` | What-if connected to `score()` | **Ayush ↔ Riya K:** connect sliders to the model |
| 10 | Sun Oct 25 | Fresh-clone test: can the setup guide be followed from zero? | Whole system on the demo laptop | Final retrain if needed; final numbers | Polish; demo script draft | **All 4: FEATURE FREEZE.** Nothing new after today |
| 11 | Mon Oct 26 | Rehearse | Rehearse | Rehearse | Runs the rehearsal | **All 4:** full demo twice; each person explains their part with no notes |
| 12 | Tue Oct 27 | Screenshots of ingestion; package dataset files | Final run; replay ready as backup | Save all charts and numbers | Screenshots, README, final rehearsal | Final check |

## Three rules

1. The live fetcher runs nonstop from Day 5 until the demo.
2. Stuck for more than 2 hours → tell the group the same day.
3. Walkthroughs on Day 6 and Day 11: each person explains their module to the other three. If you can't explain it, you're not done.

## Report (alongside the build, written by hand)

Milestone 2 needs Chapters 3 and 4 appended to the Milestone 1 report. Each person writes the sections listed in their folder README; Riya Katiyar compiles. The final report (Chapters 1–5, 45–50 pages) is due in the Nov 16–20 finals week.
