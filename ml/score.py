"""The what-if function (CONTRACT.md section 7). Role 3: Ayush. Used by the dashboard.

    from ml.score import score
    score(12345, 3)                                   # the normal forecast
    score(12345, 3, {"wind_speed_ms": 4.0})           # "what if the wind picked up?"

It takes the station's newest feature row (data/features/latest_by_station.parquet,
written by the stream job, or by pipeline.build_features before the stream runs),
replaces the columns in `overrides`, and runs the saved model for that horizon.

This is a MODEL SCENARIO, not proof of cause: it shows what the model would predict
if only that input were different. Real wind also changes other things at once.

Quick check from the terminal (the wind test from ml/README.md):
    python -m ml.score --station <station_id> --horizon 3
"""

from __future__ import annotations

import os

# The dashboard runs its own small Spark next to the stream job: keep it light.
os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")

import argparse  # noqa: E402

from common.aqi import pm25_category  # noqa: E402
from common.config import HORIZONS, PATHS, model_path  # noqa: E402

_cache: dict = {}


def _spark():
    if "spark" not in _cache:
        from common.spark import get_spark

        _cache["spark"] = get_spark("what_if")
    return _cache["spark"]


def _model(horizon_h: int):
    """Load each saved model once; reload if it was retrained (folder changed)."""
    from pyspark.ml import PipelineModel

    path = model_path(horizon_h)
    if not path.exists():
        raise FileNotFoundError(f"No saved model at {path}. Run: python -m ml.train_regression")
    stamp = path.stat().st_mtime
    key = ("model", horizon_h)
    if key not in _cache or _cache[key][0] != stamp:
        _cache[key] = (stamp, PipelineModel.load(str(path)))
    return _cache[key][1]


def latest_features():
    """pandas DataFrame: the newest feature row of every station (a few hundred rows at most)."""
    import pandas as pd

    if not PATHS.features_latest.exists():
        raise FileNotFoundError(
            f"No {PATHS.features_latest}. Run python -m pipeline.build_features (or the stream job) first."
        )
    return pd.read_parquet(PATHS.features_latest)


def _feature_row(station_id: int):
    rows = latest_features()
    row = rows[rows["station_id"] == int(station_id)]
    if row.empty:
        raise KeyError(f"Station {station_id} is not in {PATHS.features_latest}")
    return row.iloc[[0]].copy()


def score_many(station_id: int, horizon_h: int, scenarios: list[dict]) -> list[dict]:
    """Several what-if scenarios for one station in ONE Spark job (fast for a slider sweep)."""
    import pandas as pd
    from pyspark.sql import functions as F

    if horizon_h not in HORIZONS:
        raise ValueError(f"horizon_h must be one of {HORIZONS}")
    base = _feature_row(station_id)
    known = set(base.columns)
    frames = []
    for i, overrides in enumerate(scenarios):
        unknown = set(overrides or {}) - known
        if unknown:
            raise KeyError(f"Unknown feature columns in overrides: {sorted(unknown)}")
        row = base.copy()
        for col, value in (overrides or {}).items():
            row[col] = value
        row["_scenario"] = i
        frames.append(row)
    rows = pd.concat(frames, ignore_index=True)
    # Spark cannot infer types from all-null pandas columns; send everything as float/str.
    for col in rows.columns:
        if col in ("station_name", "city"):
            rows[col] = rows[col].astype(str)
        elif col == "ts_utc":
            rows[col] = pd.to_datetime(rows[col], utc=True).dt.tz_localize(None)
        elif col != "_scenario":
            rows[col] = pd.to_numeric(rows[col], errors="coerce").astype("float64")
    df = _spark().createDataFrame(rows)
    out = (
        _model(horizon_h)
        .transform(df)
        .select("_scenario", F.greatest(F.col("prediction"), F.lit(0.0)).alias("p"))
        .toPandas()
        .sort_values("_scenario")
    )
    results = []
    for p in out["p"]:
        value = round(float(p), 1)
        results.append({"station_id": int(station_id), "horizon_h": horizon_h,
                        "predicted_pm25": value, "predicted_category": pm25_category(value)})
    return results


def score(station_id: int, horizon_h: int, overrides: dict | None = None) -> dict:
    """{"station_id", "horizon_h", "predicted_pm25", "predicted_category"} for one scenario."""
    return score_many(station_id, horizon_h, [overrides or {}])[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Try the what-if function from the terminal.")
    parser.add_argument("--station", type=int, help="station_id (default: the worst station right now)")
    parser.add_argument("--horizon", type=int, default=3, choices=HORIZONS)
    args = parser.parse_args()

    rows = latest_features()
    station = args.station or int(rows.sort_values("pm25", ascending=False).iloc[0]["station_id"])
    now = rows[rows["station_id"] == station].iloc[0]
    print(f"Station {station} ({now['station_name']}), reading at {now['ts_utc']} UTC")
    print(f"Now: PM2.5 {now['pm25']:.0f}, wind {now['wind_speed_ms']} m/s, humidity {now['humidity_pct']} %")
    base = score(station, args.horizon)
    print(f"Forecast {args.horizon} h ahead: {base['predicted_pm25']} µg/m³ ({base['predicted_category']})")
    winds = [0.5, 1, 2, 3, 4, 6, 8]
    sweep = score_many(station, args.horizon, [{"wind_speed_ms": w} for w in winds])
    print("\nWind test (only wind changed):")
    for w, r in zip(winds, sweep):
        print(f"  wind {w:>4} m/s -> {r['predicted_pm25']:>6} µg/m³ ({r['predicted_category']})")
    falling = sweep[-1]["predicted_pm25"] <= sweep[0]["predicted_pm25"]
    print("\nHigher wind lowers the forecast." if falling else
          "\nHigher wind does NOT lower the forecast here. Explain why in the report (e.g. the model "
          "leans on recent PM2.5 lags, or the station's wind data is poor) instead of hiding it.")


if __name__ == "__main__":
    main()
