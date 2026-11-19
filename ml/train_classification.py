"""CPCB category 3 hours ahead + the yes/no alert model (Role 3: Ayush).

    python -m ml.train_classification            # normal run
    python -m ml.train_classification --quick    # smaller models

Part A, category (6 classes, Good ... Severe), 3 models:
    Logistic Regression, Decision Tree, Random Forest.
    Metrics: accuracy, weighted precision / recall / F1, confusion matrix.
    Best (by F1) saved to ml/models/category/ with an extra stage that turns the
    class index into the category name (column `category`).

Part B, alert: "will PM2.5 be Very Poor or worse (> 120 µg/m³) in 3 hours?" (yes/no), 3 models:
    Logistic Regression, Random Forest, GBT.
    Metrics: AUC (area under the ROC curve), accuracy, and precision / recall / F1 for "yes".
    Best (by AUC) saved to ml/models/alert_h3/. ROC points saved for the chart.

For alerts, RECALL matters most: missing a Severe hour is worse than a false alarm.
The threshold here is 0.5; lowering it trades more false alarms for higher recall.
"""

from __future__ import annotations

import argparse
import time

import numpy as np
import pandas as pd

from common.aqi import CATEGORIES, PM25_UPPER
from common.config import PATHS
from common.spark import get_spark
from ml import modeling
from ml.data import FEATURE_SETS, cap_rows, load_features, non_empty_columns, time_split, usable_rows
from ml.results_io import save_csv, save_results, update_model_info

HORIZON = 3
TARGET = "target_pm25_3h"
ALERT_THRESHOLD = PM25_UPPER[3]  # above 120 µg/m³ = Very Poor or Severe


def category_index_col(column: str):
    """0 = Good ... 5 = Severe, from the shared breakpoints (no UDF)."""
    from pyspark.sql import functions as F

    expr = F.when(F.col(column).isNull(), F.lit(None))
    for index, upper in enumerate(PM25_UPPER):
        expr = expr.when(F.col(column) <= upper, F.lit(float(index)))
    return expr.otherwise(F.lit(float(len(PM25_UPPER))))


def roc_points(scores: np.ndarray, labels: np.ndarray, max_points: int = 200) -> pd.DataFrame:
    """ROC curve: for every threshold, false-positive rate vs true-positive rate."""
    order = np.argsort(-scores)
    labels = labels[order]
    tp = np.cumsum(labels)
    fp = np.cumsum(1 - labels)
    tpr = np.concatenate([[0], tp / max(tp[-1], 1)])
    fpr = np.concatenate([[0], fp / max(fp[-1], 1)])
    keep = np.unique(np.linspace(0, len(tpr) - 1, min(max_points, len(tpr))).astype(int))
    return pd.DataFrame({"fpr": fpr[keep], "tpr": tpr[keep]})


def main() -> None:
    from pyspark.ml import PipelineModel
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.feature import IndexToString
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description="Train the category and alert classifiers.")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--max-train-rows", type=int, default=400_000)
    parser.add_argument("--max-test-rows", type=int, default=400_000)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument("--cutoff", default=None)
    args = parser.parse_args()

    params = modeling.QUICK if args.quick else modeling.FULL
    spark = get_spark("train_classification")
    started = time.time()

    split = time_split(load_features(spark), args.test_fraction, args.cutoff)
    cols = non_empty_columns(split.train, FEATURE_SETS["with_weather"])

    def labelled(df):
        return usable_rows(df, HORIZON).withColumn("label_category", category_index_col(TARGET)).withColumn(
            "label_alert", (F.col(TARGET) > ALERT_THRESHOLD).cast("double")
        )

    train, n_train = cap_rows(labelled(split.train), args.max_train_rows)
    test, n_test = cap_rows(labelled(split.test), args.max_test_rows, seed=7)
    train, test = train.persist(), test.persist()
    base = {"horizon_h": HORIZON, "feature_set": "with_weather", "n_train": n_train, "n_test": n_test,
            "train_period": split.train_period, "test_period": split.test_period}
    print(f"Train {n_train:,} rows ({split.train_period}), test {n_test:,} rows ({split.test_period})")
    share = test.groupBy("label_category").count().orderBy("label_category").toPandas()
    print("Test rows per category: " + ", ".join(
        f"{CATEGORIES[int(r.label_category)]} {int(r['count'])}" for _, r in share.iterrows()))

    # ---------- Part A: category ----------
    results, confusion_rows = [], []
    best = (None, -1.0, None)
    print("\nCategory 3 h ahead (6 classes)")
    for name in modeling.CATEGORY_CLASSIFIERS:
        t0 = time.time()
        model = modeling.make_pipeline(cols, modeling.classifier(name, "label_category", params)).fit(train)
        pred = model.transform(test)
        metrics = {}
        for key, metric in (("accuracy", "accuracy"), ("precision", "weightedPrecision"),
                            ("recall", "weightedRecall"), ("f1", "f1")):
            metrics[key] = float(MulticlassClassificationEvaluator(
                labelCol="label_category", predictionCol="prediction", metricName=metric).evaluate(pred))
        results.append({**base, "task": "classification", "model": name, **metrics})
        print(f"  {name:<19} accuracy {metrics['accuracy']:.3f}  precision {metrics['precision']:.3f}  "
              f"recall {metrics['recall']:.3f}  F1 {metrics['f1']:.3f}  ({time.time() - t0:.0f} s)")
        counts = pred.groupBy("label_category", "prediction").count().toPandas()
        for _, r in counts.iterrows():
            confusion_rows.append({"model": name, "actual": CATEGORIES[int(r.label_category)],
                                   "predicted": CATEGORIES[int(r.prediction)], "count": int(r["count"])})
        if metrics["f1"] > best[1]:
            best = (name, metrics["f1"], model)

    name, f1, model = best
    with_names = PipelineModel(stages=model.stages + [
        IndexToString(inputCol="prediction", outputCol="category", labels=list(CATEGORIES))])
    with_names.write().overwrite().save(str(PATHS.models / "category"))
    update_model_info("category", {"model": name, "f1": round(f1, 3), "horizon_h": HORIZON})
    print(f"  -> saved {name} to ml/models/category")

    # ---------- Part B: alert yes/no ----------
    print(f"\nAlert: Very Poor or worse (> {ALERT_THRESHOLD:.0f} µg/m³) in 3 h")
    roc_frames = []
    best = (None, -1.0, None)
    positives = test.filter("label_alert = 1").count()
    print(f"  {positives:,} of {n_test:,} test rows are 'yes' ({positives / max(n_test, 1):.1%})")
    for name in modeling.ALERT_CLASSIFIERS:
        t0 = time.time()
        model = modeling.make_pipeline(cols, modeling.classifier(name, "label_alert", params, binary=True)).fit(train)
        pred = model.transform(test).withColumn("p_yes", vector_to_array("probability")[1])
        auc = float(BinaryClassificationEvaluator(labelCol="label_alert", rawPredictionCol="rawPrediction",
                                                  metricName="areaUnderROC").evaluate(pred))
        c = pred.agg(
            F.sum(((F.col("prediction") == 1) & (F.col("label_alert") == 1)).cast("int")).alias("tp"),
            F.sum(((F.col("prediction") == 1) & (F.col("label_alert") == 0)).cast("int")).alias("fp"),
            F.sum(((F.col("prediction") == 0) & (F.col("label_alert") == 1)).cast("int")).alias("fn"),
            F.sum(((F.col("prediction") == 0) & (F.col("label_alert") == 0)).cast("int")).alias("tn"),
        ).first()
        tp, fp, fn, tn = (c["tp"] or 0), (c["fp"] or 0), (c["fn"] or 0), (c["tn"] or 0)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        accuracy = (tp + tn) / max(tp + fp + fn + tn, 1)
        results.append({**base, "task": "alert", "model": name, "accuracy": accuracy, "precision": precision,
                        "recall": recall, "f1": f1, "auc": auc})
        print(f"  {name:<19} AUC {auc:.3f}  accuracy {accuracy:.3f}  precision {precision:.3f}  "
              f"recall {recall:.3f}  F1 {f1:.3f}  ({time.time() - t0:.0f} s)")
        sample = pred.select("p_yes", "label_alert").sample(fraction=min(1.0, 200_000 / max(n_test, 1)), seed=1).toPandas()
        roc = roc_points(sample["p_yes"].to_numpy(), sample["label_alert"].to_numpy())
        roc.insert(0, "auc", auc)
        roc.insert(0, "model", name)
        roc_frames.append(roc)
        if auc > best[1]:
            best = (name, auc, model)

    name, auc, model = best
    model.write().overwrite().save(str(PATHS.models / "alert_h3"))
    update_model_info("alert_h3", {"model": name, "auc": round(auc, 3), "threshold_pm25": ALERT_THRESHOLD})
    print(f"  -> saved {name} to ml/models/alert_h3")

    save_results(results, tasks=("classification", "alert"))
    save_csv(pd.DataFrame(confusion_rows), "confusion_matrix.csv")
    save_csv(pd.concat(roc_frames, ignore_index=True), "roc_points.csv")
    train.unpersist()
    test.unpersist()
    print(f"\nDone in {(time.time() - started) / 60:.1f} min. Next: python -m ml.charts")
    spark.stop()


if __name__ == "__main__":
    main()
