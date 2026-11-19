# Demo click path (3 minutes, one story)

**Owner:** Riya Katiyar. This is the order of clicks and what each one shows, not a speech: say it in your own words.

## Before the demo (10 minutes earlier, on Ansh's laptop)

- [ ] Kafka running: `python scripts/check_setup.py --kafka`
- [ ] Fetcher terminal shows recent "new readings sent" lines (or start replay, see below)
- [ ] Stream job terminal shows a recent `[batch N] ... stations scored` line
- [ ] Dashboard open at http://localhost:8501, "Use SAMPLE data" **off**
- [ ] Laptop on charger, notifications off, browser zoom so the whole map fits

## The story

| # | Click | What it proves | Rubric |
|---|---|---|---|
| 1 | **Live map**: point at the coloured stations and the "Latest reading" time | Live readings stream API → Kafka → Spark every few minutes | (b) real time, (g) Kafka + Structured Streaming |
| 2 | Station selector (worst first) → **#1 station**: PM2.5 now, trend arrow, +1/+3/+6 h forecast bars | Station-level forecast, not one city number | (j) decision support |
| 3 | **Active alerts** panel: read one reason ("Forecast Severe in 3 h ...") | Early warning before it happens | (j) |
| 4 | **Model performance**: the three "beats persistence?" cards, then the comparison chart, ROC curve | 4 models × 3 horizons vs a baseline, honest results | (d) ML + metrics |
| 5 | **What-if**: drag wind speed up, watch the forecast and category change | Model scenario, **not proof of cause** (say this) | (j) |
| 6 (if time) | **Trends (BI)**: Oct-Nov & Diwali tab, Big data & quality tab | EDA insights + data size + pandas vs Spark | (e), (f), (h) |

## Backups

| If this fails | Do this | Say |
|---|---|---|
| Live API / internet | Terminal 2: Ctrl+C, then `python -m ingestion.fetch_live --replay latest --speed 60` | "This is a stored day replayed through the same pipeline; the banner says REPLAY." |
| Kafka | Fetcher `--sink files`, stream job `--source files` | "Same Spark streaming job, reading files instead of Kafka." |
| Map stays blank | Sidebar → "Simple map (works offline)" | — |
| Everything | Sidebar → "Use SAMPLE data" + show the charts in `outputs/charts/` | "Sample data, labelled as such." |

## Questions to expect (answer from your own module)

- Is the streaming real? (live API every 15 min, history only for training, replay is labelled)
- Why not "next hour = this hour"? (that is our persistence baseline; see the Model performance cards)
- Why Spark and Kafka? (benchmark numbers; Kafka keeps readings if Spark restarts)
- Does the what-if prove causation? (no, it is the model's answer to changed inputs)
- Why is this an "hourly indicator" and not the official AQI? (official AQI uses 24 h averages of several pollutants)
