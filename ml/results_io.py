"""Reading and writing ml/results/results.csv (CONTRACT.md section 7). Small files: pandas is fine."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from common.config import PATHS

RESULT_COLUMNS = [
    "task", "model", "horizon_h", "feature_set",
    "mae", "rmse", "r2", "accuracy", "precision", "recall", "f1", "auc",
    "n_train", "n_test", "train_period", "test_period",
]


def save_results(rows: list[dict], tasks: tuple[str, ...]) -> pd.DataFrame:
    """Replace the rows of the given tasks in results.csv, keep the other tasks' rows."""
    PATHS.results.mkdir(parents=True, exist_ok=True)
    new = pd.DataFrame(rows).reindex(columns=RESULT_COLUMNS)
    if PATHS.results_csv.exists():
        old = pd.read_csv(PATHS.results_csv)
        old = old[~old["task"].isin(tasks)]
        new = pd.concat([old, new], ignore_index=True)
    new = new.reindex(columns=RESULT_COLUMNS)
    new.to_csv(PATHS.results_csv, index=False, float_format="%.4f")
    return new


def save_csv(df: pd.DataFrame, name: str) -> None:
    PATHS.results.mkdir(parents=True, exist_ok=True)
    df.to_csv(PATHS.results / name, index=False, float_format="%.5f")


def update_model_info(section: str, info: dict) -> None:
    """ml/models/model_info.json: which model was saved where, with its test scores.
    The stream job reads model names from here."""
    path = PATHS.models / "model_info.json"
    PATHS.models.mkdir(parents=True, exist_ok=True)
    data = json.loads(path.read_text()) if path.exists() else {}
    data[section] = {**info, "saved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    path.write_text(json.dumps(data, indent=2))


def read_model_info() -> dict:
    path = PATHS.models / "model_info.json"
    return json.loads(path.read_text()) if path.exists() else {}
