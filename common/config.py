"""Paths and settings shared by every module (see CONTRACT.md sections 2 and 4).

Usage:
    from common.config import PATHS, KAFKA_BOOTSTRAP, KAFKA_TOPIC, HORIZONS
"""

from __future__ import annotations

import os
from pathlib import Path

# Load .env if python-dotenv is installed (it is, via requirements.txt).
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:  # still works with plain environment variables
    pass

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("AIRGUARD_DATA_DIR", REPO_ROOT / "data"))


class PATHS:
    """Every shared file location. Create folders with ensure_dirs()."""

    raw_openaq = DATA_DIR / "raw" / "openaq"
    raw_weather = DATA_DIR / "raw" / "weather"
    stations_csv = DATA_DIR / "raw" / "stations.csv"

    clean_readings = DATA_DIR / "clean" / "readings"

    features_history = DATA_DIR / "features" / "history"
    features_latest = DATA_DIR / "features" / "latest_by_station.parquet"

    live_readings = DATA_DIR / "live" / "readings"
    stream_checkpoint = DATA_DIR / "live" / "_checkpoint"

    predictions_latest = DATA_DIR / "predictions" / "latest.csv"
    predictions_history = DATA_DIR / "predictions" / "history"

    samples = DATA_DIR / "samples"

    models = REPO_ROOT / "ml" / "models"
    results = REPO_ROOT / "ml" / "results"
    results_csv = REPO_ROOT / "ml" / "results" / "results.csv"
    feature_importance_csv = REPO_ROOT / "ml" / "results" / "feature_importance.csv"

    outputs_eda = REPO_ROOT / "outputs" / "eda"
    outputs_charts = REPO_ROOT / "outputs" / "charts"
    outputs_screenshots = REPO_ROOT / "outputs" / "screenshots"


def model_path(horizon_h: int) -> Path:
    """Folder of the saved PM2.5 model for one horizon, e.g. ml/models/pm25_h3."""
    if horizon_h not in HORIZONS:
        raise ValueError(f"horizon_h must be one of {HORIZONS}, got {horizon_h}")
    return PATHS.models / f"pm25_h{horizon_h}"


def ensure_dirs() -> None:
    """Create every data/output folder that does not exist yet."""
    for value in vars(PATHS).values():
        if isinstance(value, Path):
            folder = value if value.suffix == "" else value.parent
            folder.mkdir(parents=True, exist_ok=True)


# Kafka (CONTRACT.md section 2)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aq_live")

# API keys (never print these)
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
DATA_GOV_IN_API_KEY = os.getenv("DATA_GOV_IN_API_KEY", "")

# Forecast horizons in hours (CONTRACT.md section 1)
HORIZONS = (1, 3, 6)

# Time zone used for time-of-day features and for display
LOCAL_TZ = "Asia/Kolkata"

# Rough box around Delhi-NCR for picking live stations: (min_lat, min_lon, max_lat, max_lon).
# A starting point only; Role 1 should adjust it to the stations actually reporting.
DELHI_NCR_BBOX = (28.20, 76.80, 28.95, 77.60)

# Spark memory for a laptop. Raise it in .env if your machine has more RAM.
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
