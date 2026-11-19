#!/usr/bin/env bash
# ONE laptop, ONE command: starts the live feed, builds everything, starts the live predictions.
#
#   bash scripts/run_everything.sh
#
# Before: Docker Desktop open, OPENAQ_API_KEY filled in .env, venv active.
# Takes about 1-2 hours. Keep the laptop plugged in and awake.
# Logs go to logs/. When it finishes, run:  streamlit run dashboard/app.py
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

grep -q '^OPENAQ_API_KEY=.\+' .env 2>/dev/null || { echo "Put your OpenAQ key in .env first (OPENAQ_API_KEY=...)"; exit 1; }

echo "=== 1/4 Starting Kafka"
docker compose -f infra/docker-compose.yml up -d
sleep 20

echo "=== 2/4 Starting the live feed in the background (log: logs/fetcher.log)"
nohup python -m ingestion.fetch_live --backfill-hours 30 > logs/fetcher.log 2>&1 &
echo $! > logs/fetcher.pid

echo "=== 3/4 Download history, clean, features, train models, charts, EDA (1-2 hours)"
QUICK=1 bash scripts/run_batch.sh 2>&1 | tee logs/batch.log

echo "=== 4/4 Starting live predictions in the background (log: logs/stream.log)"
nohup python -m pipeline.stream_job > logs/stream.log 2>&1 &
echo $! > logs/stream.pid

echo
echo "ALL RUNNING. Now open the dashboard:   streamlit run dashboard/app.py"
echo "Check progress:  tail -f logs/fetcher.log   or   tail -f logs/stream.log"
echo "Stop everything: kill \$(cat logs/fetcher.pid logs/stream.pid); docker compose -f infra/docker-compose.yml down"
