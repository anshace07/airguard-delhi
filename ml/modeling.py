"""Model pipelines shared by the training scripts and the live job (Role 3: Ayush).

Every saved model is a Spark PipelineModel that takes a DataFrame with the NAMED
feature columns and adds a `prediction` column (CONTRACT.md section 7):

    SQLTransformer   cast every feature to double (hour, month... are ints)
    Imputer          fill missing values with the TRAINING median of that column
    VectorAssembler  pack the columns into one `features` vector
    estimator        the algorithm (Linear Regression, Random Forest, ...)

Because the cleaning steps live inside the saved model, the live job only calls
model.transform(df). It cannot forget a step or use different fill values.
"""

from __future__ import annotations

SEED = 42

# Laptop-friendly settings. --quick halves the work for a fast first run.
FULL = {
    "dt_depth": 10,
    "rf_trees": 40,
    "rf_depth": 10,
    "gbt_iter": 40,
    "gbt_depth": 6,
}
QUICK = {
    "dt_depth": 8,
    "rf_trees": 20,
    "rf_depth": 8,
    "gbt_iter": 20,
    "gbt_depth": 5,
}

REGRESSORS = ["LinearRegression", "DecisionTree", "RandomForest", "GBT"]
CATEGORY_CLASSIFIERS = ["LogisticRegression", "DecisionTree", "RandomForest"]
ALERT_CLASSIFIERS = ["LogisticRegression", "RandomForest", "GBT"]


def prep_stages(feature_cols: list[str]):
    """Cast -> impute -> assemble. Returns the stages and the name of the vector column."""
    from pyspark.ml.feature import Imputer, SQLTransformer, VectorAssembler

    casts = ", ".join(f"CAST(`{c}` AS DOUBLE) AS `f__{c}`" for c in feature_cols)
    cast = SQLTransformer(statement=f"SELECT *, {casts} FROM __THIS__")
    imputer = Imputer(
        inputCols=[f"f__{c}" for c in feature_cols],
        outputCols=[f"i__{c}" for c in feature_cols],
        strategy="median",
    )
    assembler = VectorAssembler(
        inputCols=[f"i__{c}" for c in feature_cols], outputCol="features", handleInvalid="keep"
    )
    return [cast, imputer, assembler]


def regressor(name: str, label: str, params: dict):
    from pyspark.ml.regression import (
        DecisionTreeRegressor,
        GBTRegressor,
        LinearRegression,
        RandomForestRegressor,
    )

    common = {"featuresCol": "features", "labelCol": label, "predictionCol": "prediction"}
    if name == "LinearRegression":
        return LinearRegression(maxIter=50, regParam=0.01, elasticNetParam=0.0, **common)
    if name == "DecisionTree":
        return DecisionTreeRegressor(maxDepth=params["dt_depth"], minInstancesPerNode=20, seed=SEED, **common)
    if name == "RandomForest":
        return RandomForestRegressor(
            numTrees=params["rf_trees"],
            maxDepth=params["rf_depth"],
            minInstancesPerNode=10,
            subsamplingRate=0.7,
            featureSubsetStrategy="onethird",
            seed=SEED,
            **common,
        )
    if name == "GBT":
        return GBTRegressor(
            maxIter=params["gbt_iter"], maxDepth=params["gbt_depth"], stepSize=0.1, subsamplingRate=0.8, seed=SEED, **common
        )
    raise ValueError(name)


def classifier(name: str, label: str, params: dict, binary: bool = False):
    from pyspark.ml.classification import (
        DecisionTreeClassifier,
        GBTClassifier,
        LogisticRegression,
        RandomForestClassifier,
    )

    common = {"featuresCol": "features", "labelCol": label, "predictionCol": "prediction"}
    if name == "LogisticRegression":
        return LogisticRegression(
            maxIter=80, regParam=0.001, family="binomial" if binary else "multinomial", **common
        )
    if name == "DecisionTree":
        return DecisionTreeClassifier(maxDepth=params["dt_depth"], minInstancesPerNode=20, seed=SEED, **common)
    if name == "RandomForest":
        return RandomForestClassifier(
            numTrees=params["rf_trees"], maxDepth=params["rf_depth"], minInstancesPerNode=10, subsamplingRate=0.7, seed=SEED, **common
        )
    if name == "GBT":
        return GBTClassifier(maxIter=params["gbt_iter"], maxDepth=params["gbt_depth"], stepSize=0.1, seed=SEED, **common)
    raise ValueError(name)


def make_pipeline(feature_cols: list[str], estimator):
    from pyspark.ml import Pipeline

    return Pipeline(stages=prep_stages(feature_cols) + [estimator])


def importances(pipeline_model, feature_cols: list[str]) -> list[tuple[str, float]]:
    """featureImportances of a tree model, mapped back to column names (largest first)."""
    last = pipeline_model.stages[-1]
    if not hasattr(last, "featureImportances"):
        return []
    values = last.featureImportances.toArray()
    pairs = sorted(zip(feature_cols, values), key=lambda p: -p[1])
    return [(name, float(v)) for name, v in pairs]
