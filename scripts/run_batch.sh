#!/usr/bin/env bash
# The whole BATCH path on REAL data, in order (venv active, from the repo root).
# Safe to re-run: downloads resume and every step overwrites its own output.
#
#   bash scripts/run_batch.sh                         # Delhi-NCR, 2025 + 2026 (default)
#   YEARS="2023 2024 2025 2026" bash scripts/run_batch.sh
#   SCOPE=india bash scripts/run_batch.sh             # every Indian station (much bigger: hours)
#   QUICK=1 bash scripts/run_batch.sh                 # smaller models, about half the training time
#   SKIP_DOWNLOAD=1 bash scripts/run_batch.sh         # data already downloaded: start at cleaning
#
# Needs OPENAQ_API_KEY in .env for the first step (station list). Prints timings per step.
set -euo pipefail
cd "$(dirname "$0")/.."

YEARS="${YEARS:-2025 2026}"
SCOPE="${SCOPE:-ncr}"
QUICK_FLAG=""
[ -n "${QUICK:-}" ] && QUICK_FLAG="--quick"

step() { echo; echo "=== [$(date +%H:%M:%S)] $1"; }

if [ -z "${SKIP_DOWNLOAD:-}" ]; then
  step "1. Station list ($SCOPE)";            python -m ingestion.stations --scope "$SCOPE"
  step "2. OpenAQ archive, years: $YEARS";    python -m ingestion.download_archive --years $YEARS
  step "3. Historical weather";               python -m ingestion.weather_history
fi
step "4. Clean (narrow -> wide, units, quality)"; python -m pipeline.clean_history
step "5. Features (+ sample for Role 3)";     python -m pipeline.build_features --sample
step "6. Regression: baseline + 4 models x 3 horizons"; python -m ml.train_regression $QUICK_FLAG
step "7. Category + alert classifiers";      python -m ml.train_classification $QUICK_FLAG
step "8. Result charts";                     python -m ml.charts
step "9. EDA charts";                        python -m ingestion.eda
step "10. Dataset numbers";                  python -m ingestion.dataset_numbers
echo
echo "Batch path done. Next: Kafka + live fetcher + stream job + dashboard (README, 'Run the whole system')."
echo "Optional: python -m pipeline.benchmark   (pandas vs Spark, for the report)"
