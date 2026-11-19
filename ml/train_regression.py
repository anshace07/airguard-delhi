"""PM2.5 forecasts 1, 3 and 6 hours ahead: baseline + 4 models per horizon (Role 3: Ayush).

    python -m ml.train_regression                 # normal run
    python -m ml.train_regression --quick         # smaller trees, about half the time
    python -m ml.train_regression --max-train-rows 1000000   # use more data if the laptop copes

What it does, per horizon (1, 3, 6 h):
 1. Persistence baseline: "PM2.5 in h hours = PM2.5 now". No learning at all.
 2. Linear Regression, Decision Tree, Random Forest, GBT on the `with_weather` features.
 3. Scores every model on the SAME test rows: MAE, RMSE, R².
 4. Saves the best model (lowest RMSE) to ml/models/pm25_h{h}/ for the live job.
 5. Weather experiment: retrains that best model WITHOUT weather columns
    (`pollutants_only`). Does weather help? results.csv answers it either way.

Outputs: ml/results/results.csv (regression rows), feature_importance.csv,
pred_vs_actual.csv, ml/models/pm25_h*/, feature_columns.json, model_info.json.
Then run `python -m ml.charts`.

Honesty rule (CLAUDE.md): if a model does not beat persistence at some horizon,
the printout and results.csv say so. Do not hide it.
"""

from __future__ import annotations

import argparse
import json
import time

from common.config import HORIZONS, PATHS, model_path
from common.schemas import TARGET_COLUMNS
from common.spark import get_spark
from ml import modeling
from ml.data import FEATURE_SETS, cap_rows, load_features, non_empty_columns, time_split, usable_rows
from ml.results_io import save_csv, save_results, update_model_info


def evaluate(predictions, label: str) -> dict:
    """MAE, RMSE, R² of a DataFrame that has `prediction` and the label column."""
    from pyspark.ml.evaluation import RegressionEvaluator

    out = {}
    for metric in ("mae", "rmse", "r2"):
        evaluator = RegressionEvaluator(labelCol=label, predictionCol="prediction", metricName=metric)
        out[metric] = float(evaluator.evaluate(predictions))
    return out


def clamp(predictions):
    """A concentration cannot be negative (Linear Regression can predict below 0)."""
    from pyspark.sql import functions as F

    return predictions.withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))


def main() -> None:
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description="Train and compare the PM2.5 forecast models.")
    parser.add_argument("--quick", action="store_true", help="smaller models for a fast first run")
    parser.add_argument("--max-train-rows", type=int, default=400_000, help="cap on training rows per horizon (0 = all)")
    parser.add_argument("--max-test-rows", type=int, default=400_000, help="cap on test rows per horizon (0 = all)")
    parser.add_argument("--test-fraction", type=float, default=0.2, help="last share of the time range used as test")
    parser.add_argument("--cutoff", default=None, help="start of the test period, e.g. 2025-10-01 (overrides --test-fraction)")
    parser.add_argument("--experiment-models", choices=["best", "all"], default="best",
                        help="weather experiment for the best model per horizon (default) or all four")
    args = parser.parse_args()

    params = modeling.QUICK if args.quick else modeling.FULL
    spark = get_spark("train_regression")
    started = time.time()

    split = time_split(load_features(spark), args.test_fraction, args.cutoff)
    print(f"Train period: {split.train_period}\nTest period:  {split.test_period}\n")

    all_weather_cols = non_empty_columns(split.train, FEATURE_SETS["with_weather"])
    feature_cols = {
        "with_weather": all_weather_cols,
        "pollutants_only": [c for c in FEATURE_SETS["pollutants_only"] if c in all_weather_cols],
    }

    results, importance_rows, pred_rows, model_info = [], [], [], {}

    for h in HORIZONS:
        label = TARGET_COLUMNS[h]
        train, n_train = cap_rows(usable_rows(split.train, h), args.max_train_rows)
        test, n_test = cap_rows(usable_rows(split.test, h), args.max_test_rows, seed=7)
        train, test = train.persist(), test.persist()
        base = {"task": "regression", "horizon_h": h, "n_train": n_train, "n_test": n_test,
                "train_period": split.train_period, "test_period": split.test_period}
        print(f"=== Horizon {h} h: {n_train:,} train rows, {n_test:,} test rows ===")

        # 1. Persistence baseline (same rows, so the comparison is fair)
        persistence = test.withColumn("prediction", F.col("pm25"))
        scores = evaluate(persistence, label)
        for fs in ("with_weather", "pollutants_only"):
            results.append({**base, "model": "Persistence", "feature_set": fs, **scores})
        print(f"  {'Persistence':<17} MAE {scores['mae']:7.2f}  RMSE {scores['rmse']:7.2f}  R² {scores['r2']:6.3f}")
        baseline_rmse = scores["rmse"]

        # 2. Four models with weather
        fitted = {}
        for name in modeling.REGRESSORS:
            t0 = time.time()
            model = modeling.make_pipeline(feature_cols["with_weather"], modeling.regressor(name, label, params)).fit(train)
            scores = evaluate(clamp(model.transform(test)), label)
            fitted[name] = (model, scores)
            results.append({**base, "model": name, "feature_set": "with_weather", **scores})
            for feature, value in modeling.importances(model, feature_cols["with_weather"]):
                importance_rows.append({"model": name, "horizon_h": h, "feature": feature, "importance": value})
            beat = "beats" if scores["rmse"] < baseline_rmse else "does NOT beat"
            print(f"  {name:<17} MAE {scores['mae']:7.2f}  RMSE {scores['rmse']:7.2f}  R² {scores['r2']:6.3f}"
                  f"  ({beat} persistence, {time.time() - t0:.0f} s)")

        best_name = min(fitted, key=lambda n: fitted[n][1]["rmse"])
        best_model, best_scores = fitted[best_name]

        # 3. Weather experiment: same algorithm(s), pollutant columns only
        experiment = modeling.REGRESSORS if args.experiment_models == "all" else [best_name]
        for name in experiment:
            model = modeling.make_pipeline(feature_cols["pollutants_only"], modeling.regressor(name, label, params)).fit(train)
            scores = evaluate(clamp(model.transform(test)), label)
            results.append({**base, "model": name, "feature_set": "pollutants_only", **scores})
            with_w = fitted[name][1]["rmse"]
            verdict = "weather HELPS" if with_w < scores["rmse"] else "weather does NOT help"
            print(f"  {name} without weather: RMSE {scores['rmse']:.2f} vs {with_w:.2f} with weather -> {verdict}")

        # 4. Save the best model for the live job
        best_model.write().overwrite().save(str(model_path(h)))
        model_info[f"pm25_h{h}"] = {
            "model": best_name, "feature_set": "with_weather", **{k: round(v, 3) for k, v in best_scores.items()},
            "persistence_rmse": round(baseline_rmse, 3), "beats_persistence": best_scores["rmse"] < baseline_rmse,
            "train_period": split.train_period, "test_period": split.test_period,
        }
        print(f"  -> saved {best_name} to {model_path(h)}"
              + ("" if best_scores["rmse"] < baseline_rmse else "  (NOTE: it does not beat persistence; say so in the report)"))

        # 5. One week of predicted vs actual at the station with the most test rows (for a chart)
        station = test.groupBy("station_id").count().orderBy(F.desc("count")).first()["station_id"]
        week = (
            clamp(best_model.transform(split.test.filter((F.col("station_id") == station) & F.col(label).isNotNull())))
            .select("station_id", "station_name", "ts_utc", F.col("pm25").alias("persistence"),
                    F.col(label).alias("actual"), F.col("prediction").alias("predicted"))
            .orderBy("ts_utc")
            .limit(24 * 7)
            .toPandas()
        )
        week.insert(0, "model", best_name)
        week.insert(0, "horizon_h", h)
        pred_rows.append(week)
        train.unpersist()
        test.unpersist()
        print()

    import pandas as pd

    save_results(results, tasks=("regression",))
    save_csv(pd.DataFrame(importance_rows), "feature_importance.csv")
    save_csv(pd.concat(pred_rows, ignore_index=True), "pred_vs_actual.csv")
    (PATHS.models / "feature_columns.json").write_text(json.dumps(feature_cols["with_weather"], indent=2))
    for key, info in model_info.items():
        update_model_info(key, info)

    print(f"Done in {(time.time() - started) / 60:.1f} min. Results: {PATHS.results_csv}")
    print("Next: python -m ml.train_classification, then python -m ml.charts")
    spark.stop()


if __name__ == "__main__":
    main()
