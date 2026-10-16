"""One way for everyone to start Spark, so versions and settings never drift.

    from common.spark import get_spark
    spark = get_spark("clean_history")              # batch jobs
    spark = get_spark("stream_job", kafka=True)     # anything that reads Kafka
"""

from __future__ import annotations

from common.aqi import CATEGORIES, PM25_UPPER
from common.config import SPARK_DRIVER_MEMORY


def _kafka_package() -> str:
    """The Spark-Kafka connector must match the installed PySpark version exactly."""
    import pyspark

    version = pyspark.__version__
    scala = "2.13" if version.startswith("4.") else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"


def get_spark(app_name: str = "airguard", kafka: bool = False):
    """Local Spark session using all CPU cores, UTC time, laptop-friendly settings.

    kafka=True downloads the Kafka connector the first time (needs internet once).
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if kafka:
        builder = builder.config("spark.jars.packages", _kafka_package())
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def category_col(column_name: str):
    """Spark column with the CPCB category name for a PM2.5 column (no UDF)."""
    from pyspark.sql import functions as F

    expr = F.when(F.col(column_name).isNull(), F.lit(None))
    for name, upper in zip(CATEGORIES, PM25_UPPER):
        expr = expr.when(F.col(column_name) <= upper, F.lit(name))
    return expr.otherwise(F.lit(CATEGORIES[-1]))
