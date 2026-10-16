# Role 3: Prediction Models

**Owner:** Ayush Mishra · **Branch:** `ayush/ml` · **Works closest with:** Ansh (Role 2), Riya Katiyar (Role 4)

**Your job in one line:** build and compare the models that predict PM2.5 1, 3 and 6 hours ahead, and prove which one is best, honestly.

Every number in the results chapter comes from you.

---

## What you deliver

| Deliverable | Goes to | By |
|---|---|---|
| Training code working on Ansh's sample features | — | Day 3 (Sun Oct 18) |
| Baseline + first real model on full features | — | Day 5 (Tue Oct 20) |
| Best saved model per horizon in `ml/models/pm25_h{1,3,6}/` + `feature_columns.json` | Ansh | **Day 6 (Wed Oct 21)** |
| Category classifiers + alert classifier (ROC curve) | Ansh, report | Day 7 (Thu Oct 22) |
| `ml/results/results.csv`, `feature_importance.csv`, charts in `outputs/charts/` | Riya Katiyar | **Day 8 (Fri Oct 23)** |
| `ml/score.py` for the what-if panel | Riya Katiyar | **Day 9 (Sat Oct 24)** |

Formats: CONTRACT.md section 7.

---

## Steps

1. **Start on Day 2 with any small data** (Ansh's sample on Day 3) so your code is ready before the full features arrive on Day 4.
2. **Split by time, never randomly.** Train on older data, test on the most recent period (e.g. the last full year, or last 6 months). A random split lets the model peek at the future and fakes good scores. Write the exact periods in `results.csv`.
3. **Baseline (Persistence):** "PM2.5 in h hours = PM2.5 now". Every model must be compared against it, per horizon.
4. **Regression, 4 models × 3 horizons = 12 models:** Linear Regression, Decision Tree, Random Forest, GBT. Target: `target_pm25_1h/3h/6h`. Metrics: MAE, RMSE, R².
5. **Weather experiment:** train the best model twice, `pollutants_only` vs `with_weather`. Does weather help? Report the answer either way.
6. **Category classifiers (3):** Logistic Regression, Decision Tree, Random Forest predicting the CPCB category 3 hours ahead (label from `common.aqi`). Metrics: accuracy, precision, recall, F1 (weighted), confusion matrix chart.
7. **Alert classifier:** yes/no "Very Poor or worse in 3 hours". This gives the **ROC curve and AUC** the rubric asks for.
8. **Feature importance** from Random Forest and GBT → `feature_importance.csv` + bar chart.
9. **Save** the best regressor per horizon as a `PipelineModel` with the `VectorAssembler` inside, plus `ml/models/feature_columns.json`. Ansh only calls `model.transform(df)`.
10. **`ml/score.py`:** the what-if function in CONTRACT.md section 7. Test it: higher wind speed should usually lower the forecast; if it doesn't, explain why in the report instead of hiding it.
11. **Charts** in `outputs/charts/`: model comparison bars per horizon (with baseline), confusion matrix, ROC curve, feature importance, predicted vs actual for one station over a week.

**Speed tips for a laptop:** tune on a sample (one year, Delhi stations), then train the final models on the full data. Keep tree depth and number of trees modest. Cache the training DataFrame.

**Done when:** `results.csv` has every row filled, the 3 saved models load and score a DataFrame, and `score()` returns a sensible forecast.

---

## What you must be able to explain in the viva

- How each of the 4 algorithms works, in two sentences each.
- Why the split is by time, and what leakage is.
- What MAE, RMSE, R², precision, recall, F1 and AUC mean, and which matters most for alerts (missing a Severe hour is worse than a false alarm → recall).
- Why there is one model per horizon.
- Where the models beat the baseline and where they don't (usually the 1-hour forecast is hard to beat; longer horizons show more gain).
- Why MLlib (it trains on the full distributed data; scikit-learn would need it all in memory).

## Your report sections (write by hand)

Algorithms and why they suit the problem, training setup, evaluation metrics, results and comparison, weather experiment, feature importance, limitations.

---

## Starter prompt for your AI coding tool

```
Read CLAUDE.md, CONTRACT.md and ml/README.md first.

Build my part (Role 3) step by step with PySpark MLlib (start Spark with common.spark.get_spark). Stop after each step, run it, and explain it to me in simple words before continuing.

1) ml/data.py: load features from common.config.PATHS.features_history; time-based train/test split (configurable cut-off date); feature column lists for "pollutants_only" and "with_weather"; drop rows with a null target.

2) ml/train_regression.py: Persistence baseline plus LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor for each horizon in common.config.HORIZONS (target_pm25_1h/3h/6h). Each model is a Pipeline (Imputer or null-handling, VectorAssembler, model). Evaluate MAE, RMSE, R2 on the test period. Run the weather experiment for the best model. Append rows to PATHS.results_csv exactly in the CONTRACT.md section 7 format. Save the best model per horizon to common.config.model_path(h) and the feature list to ml/models/feature_columns.json.

3) ml/train_classification.py: label = CPCB category of target_pm25_3h using common.aqi; LogisticRegression, DecisionTreeClassifier, RandomForestClassifier; accuracy, weighted precision, recall, F1, confusion matrix. Plus a binary classifier for "Very Poor or worse in 3h" with ROC curve and AUC. Append to results.csv; save the best ones to ml/models/category/ and ml/models/alert_h3/.

4) ml/importance.py: feature importances from the RF and GBT models into PATHS.feature_importance_csv.

5) ml/charts.py: comparison bar chart per horizon including the baseline, confusion matrix, ROC curve, feature importance, predicted vs actual for one station for one week. Save PNGs in PATHS.outputs_charts.

6) ml/score.py: score(station_id, horizon_h, overrides=None) per CONTRACT.md section 7, reading PATHS.features_latest. Cache the Spark session and loaded models so repeated calls are fast. Add a small test.
```
