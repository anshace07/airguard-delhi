"""All result charts as PNG in outputs/charts/ (Role 3: Ayush).

    python -m ml.charts

Reads the small CSVs in ml/results/ (written by the training scripts), so it runs in
seconds and never needs Spark. Charts:
    regression_comparison.png    RMSE and MAE per model per horizon, baseline in grey
    weather_experiment.png       best model with vs without weather, per horizon
    classification_comparison.png  accuracy / precision / recall / F1 of the 3 category models
    confusion_matrix.png         best category model, rows = actual, normalised to 100%
    roc_curve.png                alert models, with AUC in the legend
    feature_importance_h{1,3,6}.png  top 15 features of the tree models
    pred_vs_actual_h{1,3,6}.png  one week at one station: actual vs model vs persistence
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from common.aqi import CATEGORIES  # noqa: E402
from common.config import HORIZONS, PATHS  # noqa: E402

MODEL_COLORS = {
    "Persistence": "#9e9e9e",
    "LinearRegression": "#4e79a7",
    "LogisticRegression": "#4e79a7",
    "DecisionTree": "#f28e2b",
    "RandomForest": "#59a14f",
    "GBT": "#b07aa1",
}
REG_ORDER = ["Persistence", "LinearRegression", "DecisionTree", "RandomForest", "GBT"]


def _save(fig, name: str) -> None:
    PATHS.outputs_charts.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(PATHS.outputs_charts / name, dpi=150)
    plt.close(fig)
    print(f"  {name}")


def regression_comparison(results: pd.DataFrame) -> None:
    reg = results[(results["task"] == "regression") & (results["feature_set"] == "with_weather")]
    if reg.empty:
        return
    fig, axes = plt.subplots(2, len(HORIZONS), figsize=(13, 7), sharey="row")
    for row, metric in enumerate(("rmse", "mae")):
        for col, h in enumerate(HORIZONS):
            ax = axes[row][col]
            part = reg[reg["horizon_h"] == h].set_index("model").reindex([m for m in REG_ORDER if m in set(reg["model"])])
            values = part[metric].to_numpy()
            colors = [MODEL_COLORS.get(m, "#666") for m in part.index]
            bars = ax.bar(range(len(values)), values, color=colors)
            ax.bar_label(bars, fmt="%.1f", fontsize=8)
            if "Persistence" in part.index:
                ax.axhline(part.loc["Persistence", metric], color="#555", linestyle="--", linewidth=1)
            ax.set_xticks(range(len(values)), [m.replace("Regression", "Reg.") for m in part.index], rotation=30, ha="right", fontsize=8)
            ax.set_title(f"{metric.upper()}, {h} h ahead", fontsize=10)
            if col == 0:
                ax.set_ylabel(f"{metric.upper()} (µg/m³, lower is better)")
    fig.suptitle("PM2.5 forecast error by model and horizon (dashed line = persistence baseline)")
    _save(fig, "regression_comparison.png")


def weather_experiment(results: pd.DataFrame) -> None:
    reg = results[(results["task"] == "regression") & (results["model"] != "Persistence")]
    pairs = reg.pivot_table(index=["horizon_h", "model"], columns="feature_set", values="rmse").dropna()
    if pairs.empty or "pollutants_only" not in pairs:
        return
    pairs = pairs.reset_index()
    labels = [f"{r.model}\n{r.horizon_h} h" for r in pairs.itertuples()]
    x = np.arange(len(pairs))
    fig, ax = plt.subplots(figsize=(max(6, 1.6 * len(pairs)), 4.5))
    b1 = ax.bar(x - 0.2, pairs["pollutants_only"], 0.4, label="pollutants only", color="#bab0ac")
    b2 = ax.bar(x + 0.2, pairs["with_weather"], 0.4, label="with weather", color="#4e79a7")
    ax.bar_label(b1, fmt="%.1f", fontsize=8)
    ax.bar_label(b2, fmt="%.1f", fontsize=8)
    ax.set_xticks(x, labels, fontsize=8)
    ax.set_ylabel("RMSE (µg/m³, lower is better)")
    ax.set_title("Does weather help? Same model, with and without weather columns")
    ax.legend()
    _save(fig, "weather_experiment.png")


def classification_comparison(results: pd.DataFrame) -> None:
    cls = results[results["task"] == "classification"].set_index("model")
    if cls.empty:
        return
    metrics = ["accuracy", "precision", "recall", "f1"]
    x = np.arange(len(metrics))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    width = 0.8 / len(cls)
    for i, (model, row) in enumerate(cls.iterrows()):
        bars = ax.bar(x + i * width - 0.4 + width / 2, row[metrics].astype(float), width, label=model,
                      color=MODEL_COLORS.get(model, "#666"))
        ax.bar_label(bars, fmt="%.2f", fontsize=7)
    ax.set_xticks(x, ["Accuracy", "Precision (w)", "Recall (w)", "F1 (w)"])
    ax.set_ylim(0, 1.05)
    ax.set_title("CPCB category 3 h ahead: classifier comparison (w = weighted)")
    ax.legend(fontsize=8)
    _save(fig, "classification_comparison.png")


def confusion_matrix(results: pd.DataFrame) -> None:
    path = PATHS.results / "confusion_matrix.csv"
    cls = results[results["task"] == "classification"]
    if not path.exists() or cls.empty:
        return
    best = cls.sort_values("f1", ascending=False).iloc[0]["model"]
    cm = pd.read_csv(path)
    cm = cm[cm["model"] == best].pivot_table(index="actual", columns="predicted", values="count", aggfunc="sum")
    cm = cm.reindex(index=CATEGORIES, columns=CATEGORIES).fillna(0)
    share = cm.div(cm.sum(axis=1).replace(0, np.nan), axis=0).fillna(0) * 100
    fig, ax = plt.subplots(figsize=(7.5, 6))
    im = ax.imshow(share.to_numpy(), cmap="Blues", vmin=0, vmax=100)
    for i in range(len(CATEGORIES)):
        for j in range(len(CATEGORIES)):
            v = share.iat[i, j]
            ax.text(j, i, f"{v:.0f}%\n({int(cm.iat[i, j])})", ha="center", va="center", fontsize=7,
                    color="white" if v > 55 else "black")
    ax.set_xticks(range(6), CATEGORIES, rotation=30, ha="right")
    ax.set_yticks(range(6), CATEGORIES)
    ax.set_xlabel("Predicted category")
    ax.set_ylabel("Actual category")
    ax.set_title(f"Confusion matrix, {best} (row % and counts)")
    fig.colorbar(im, ax=ax, label="% of actual row")
    _save(fig, "confusion_matrix.png")


def roc_curve() -> None:
    path = PATHS.results / "roc_points.csv"
    if not path.exists():
        return
    roc = pd.read_csv(path)
    fig, ax = plt.subplots(figsize=(6, 6))
    for model, part in roc.groupby("model"):
        ax.plot(part["fpr"], part["tpr"], label=f"{model} (AUC {part['auc'].iloc[0]:.3f})",
                color=MODEL_COLORS.get(model, None), linewidth=2)
    ax.plot([0, 1], [0, 1], linestyle="--", color="#999", label="random guess (AUC 0.5)")
    ax.set_xlabel("False positive rate (false alarms)")
    ax.set_ylabel("True positive rate (recall)")
    ax.set_title("ROC: 'Very Poor or worse in 3 h' alert")
    ax.legend(loc="lower right", fontsize=8)
    _save(fig, "roc_curve.png")


def feature_importance() -> None:
    path = PATHS.feature_importance_csv
    if not path.exists():
        return
    fi = pd.read_csv(path)
    for h in HORIZONS:
        part = fi[fi["horizon_h"] == h]
        if part.empty:
            continue
        models = list(part["model"].unique())
        fig, axes = plt.subplots(1, len(models), figsize=(6 * len(models), 5.5), squeeze=False)
        for ax, model in zip(axes[0], models):
            top = part[part["model"] == model].nlargest(15, "importance").iloc[::-1]
            ax.barh(top["feature"], top["importance"], color=MODEL_COLORS.get(model, "#666"))
            ax.set_title(f"{model}, {h} h ahead")
            ax.set_xlabel("Importance (share of total)")
        fig.suptitle("Which inputs drive the forecast? (global feature importance)")
        _save(fig, f"feature_importance_h{h}.png")


def pred_vs_actual() -> None:
    path = PATHS.results / "pred_vs_actual.csv"
    if not path.exists():
        return
    df = pd.read_csv(path, parse_dates=["ts_utc"])
    for h in HORIZONS:
        part = df[df["horizon_h"] == h]
        if part.empty:
            continue
        fig, ax = plt.subplots(figsize=(11, 4))
        # The row's ts is the forecast time; the value it forecasts belongs to ts + h.
        when = part["ts_utc"] + pd.to_timedelta(h, unit="h")
        ax.plot(when, part["actual"], label="actual", color="#222", linewidth=1.6)
        ax.plot(when, part["predicted"], label=f"{part['model'].iloc[0]} forecast", color="#e15759", linewidth=1.2)
        ax.plot(when, part["persistence"], label="persistence", color="#9e9e9e", linewidth=1, linestyle="--")
        ax.set_ylabel("PM2.5 (µg/m³)")
        ax.set_title(f"{part['station_name'].iloc[0]}: {h} h ahead forecast vs actual (first test week, UTC)")
        ax.legend(fontsize=8)
        _save(fig, f"pred_vs_actual_h{h}.png")


def main() -> None:
    if not PATHS.results_csv.exists():
        raise SystemExit("No ml/results/results.csv yet. Run python -m ml.train_regression first.")
    results = pd.read_csv(PATHS.results_csv)
    print(f"Writing charts to {PATHS.outputs_charts}")
    regression_comparison(results)
    weather_experiment(results)
    classification_comparison(results)
    confusion_matrix(results)
    roc_curve()
    feature_importance()
    pred_vs_actual()


if __name__ == "__main__":
    main()
