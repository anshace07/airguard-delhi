"""Loading the feature table and splitting it by time (Role 3: Ayush).

The split is by TIME, never random: train on the older period, test on the most
recent one. A random split would put hours from the same day in both sets, so the
model would "see the future" and score better than it really is.

We also leave an EMBARGO gap of 6 hours between train and test: a training row's
6-hour target looks 6 hours ahead, so without the gap the last training labels
would be test-period values.
"""

from __future__ import annotations

from dataclasses import dataclass

from common.config import PATHS
from common.features import CHANGE_COLUMNS, LAG_COLUMNS, ROLL_COLUMNS, TIME_FEATURES
from common.schemas import POLLUTANTS, TARGET_COLUMNS, WEATHER

POLLUTANT_FEATURES = POLLUTANTS + LAG_COLUMNS + ROLL_COLUMNS + CHANGE_COLUMNS + TIME_FEATURES
FEATURE_SETS = {
    "pollutants_only": POLLUTANT_FEATURES,
    "with_weather": POLLUTANT_FEATURES + WEATHER,
}
EMBARGO_HOURS = 6


@dataclass
class Split:
    train: object  # Spark DataFrame
    test: object
    train_period: str
    test_period: str


def load_features(spark, path=None):
    return spark.read.parquet(str(path or PATHS.features_history))


def time_split(df, test_fraction: float = 0.2, cutoff: str | None = None) -> Split:
    """Train = everything before the cutoff (minus the embargo), test = cutoff onwards.

    cutoff: a date like "2025-10-01". If not given, the last `test_fraction` of the
    time range becomes the test set.
    """
    from pyspark.sql import functions as F

    bounds = df.agg(F.min("ts_utc").alias("lo"), F.max("ts_utc").alias("hi")).first()
    lo, hi = bounds["lo"], bounds["hi"]
    if cutoff:
        from datetime import datetime

        cut = datetime.fromisoformat(cutoff)
    else:
        cut = lo + (hi - lo) * (1 - test_fraction)
        cut = cut.replace(minute=0, second=0, microsecond=0)
    from datetime import timedelta

    train_end = cut - timedelta(hours=EMBARGO_HOURS)
    train = df.filter(F.col("ts_utc") < F.lit(train_end))
    test = df.filter(F.col("ts_utc") >= F.lit(cut))
    fmt = "%Y-%m-%d %H:%M"
    return Split(
        train=train,
        test=test,
        train_period=f"{lo:{fmt}} to {train_end:{fmt}} UTC",
        test_period=f"{cut:{fmt}} to {hi:{fmt}} UTC",
    )


def usable_rows(df, horizon_h: int):
    """Rows where both the current PM2.5 and the target exist.

    Every model (and the persistence baseline) is scored on exactly these rows,
    so the comparison is fair.
    """
    from pyspark.sql import functions as F

    target = TARGET_COLUMNS[horizon_h]
    return df.filter(F.col("pm25").isNotNull() & F.col(target).isNotNull())


def cap_rows(df, max_rows: int | None, seed: int = 42):
    """Randomly keep about max_rows rows (within one period only, so no leakage)."""
    if not max_rows:
        return df, df.count()
    n = df.count()
    if n <= max_rows:
        return df, n
    sampled = df.sample(withReplacement=False, fraction=max_rows / n, seed=seed)
    return sampled, sampled.count()


def non_empty_columns(df, columns: list[str]) -> list[str]:
    """Drop feature columns that are entirely null (the imputer cannot learn a fill value)."""
    from pyspark.sql import functions as F

    counts = df.agg(*[F.count(F.col(c)).alias(c) for c in columns]).first().asDict()
    kept = [c for c in columns if counts[c] > 0]
    dropped = [c for c in columns if counts[c] == 0]
    if dropped:
        print(f"  Note: no values at all for {dropped}; left out of the model.")
    return kept
