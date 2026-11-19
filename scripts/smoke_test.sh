#!/usr/bin/env bash
# End-to-end test of the WHOLE system on SYNTHETIC data, in a throwaway copy of the repo.
# Your real data/, models, results and charts are never touched.
#
#   bash scripts/smoke_test.sh          (venv active, from the repo root; about 5-10 minutes)
#
# It runs every step a real run does (no internet, Kafka or API key needed):
# synthetic archive -> clean -> features -> train -> charts -> EDA -> replay via files ->
# stream job -> what-if -> all four dashboard pages. "SMOKE TEST PASSED" at the end = all good.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t airguard-smoke-XXXX)"
PY="${PYTHON:-python}"
echo "Throwaway working copy: $WORK"
tar -C "$REPO" --exclude=./.git --exclude=./data --exclude=./ml/models --exclude=./ml/results \
    --exclude=./outputs --exclude=./.venv --exclude=./submission -cf - . | tar -C "$WORK" -xf -
mkdir -p "$WORK/data" "$WORK/outputs" "$WORK/ml/models" "$WORK/ml/results"
cd "$WORK"
export AIRGUARD_DATA_DIR="$WORK/data"

step() { echo; echo "=== $1"; }
step "1/9 synthetic OpenAQ-format data";  $PY scripts/make_synthetic_data.py --stations 6 --days 90
step "2/9 clean history";                 $PY -m pipeline.clean_history
step "3/9 features";                      $PY -m pipeline.build_features
step "4/9 regression models (quick)";     $PY -m ml.train_regression --quick
step "5/9 classifiers (quick)";           $PY -m ml.train_classification --quick
step "6/9 charts + EDA";                  $PY -m ml.charts && $PY -m ingestion.eda
step "7/9 replay -> files -> stream job"
$PY -m ingestion.fetch_live --replay latest --speed 0 --sink files
$PY -m pipeline.stream_job --source files --available-now
test -s data/predictions/latest.csv
step "8/9 what-if score()";               $PY -m ml.score --horizon 3
step "9/9 dashboard pages (headless)"
$PY - <<'EOF'
import os
from streamlit.testing.v1 import AppTest
for page in ["Live map", "What-if", "Trends (BI)", "Model performance"]:
    at = AppTest.from_file(os.path.join(os.getcwd(), "dashboard", "app.py"), default_timeout=240)
    at.run()
    at.sidebar.radio[0].set_value(page).run()
    if at.exception:
        raise SystemExit(f"Dashboard page '{page}' failed: {at.exception[0].value}")
    print(f"  page '{page}': OK")
os._exit(0)  # skip Spark's noisy clean-up messages at interpreter exit
EOF
echo
echo "SMOKE TEST PASSED (synthetic data in $WORK). Nothing in your repo was changed."
