"""Why Spark? The same heavy job in pandas and in Spark, on growing shares of the data (Role 2: Ansh).

    python -m pipeline.benchmark                         # 25%, 50%, 100% of the downloaded archive
    python -m pipeline.benchmark --fractions 0.1 0.5 1   # other sizes
    python -m pipeline.benchmark --pandas-memory-limit-gb 4   # give pandas the same RAM budget as Spark

The job (identical logic in both): read every archive CSV -> keep the 6 pollutants with
valid values -> hourly mean per station/pollutant -> pivot narrow to wide -> 24-hour
rolling mean of PM2.5 per station. That is the core of our cleaning + feature step.

pandas runs in a separate process so its peak memory (RSS) is measured cleanly. pandas
must hold ALL rows in memory at once; Spark streams through the files in partitions
inside a fixed memory cap (SPARK_DRIVER_MEMORY). If pandas runs out of memory, that is
recorded as the result.

Output: outputs/benchmark.csv and outputs/charts/benchmark.png. Report the numbers as
they come out, including any size where pandas is faster: on small data it usually is,
because Spark has a fixed start-up cost. The point is how each one scales.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time

from common.config import PATHS, SPARK_DRIVER_MEMORY
from common.schemas import POLLUTANTS


# ------------------------------------------------------------------ pandas version

def pandas_job(files: list[str]) -> dict:
    import pandas as pd

    started = time.time()
    frames = [pd.read_csv(f, usecols=["location_id", "datetime", "parameter", "units", "value"]) for f in files]
    df = pd.concat(frames, ignore_index=True)
    rows_in = len(df)
    df = df[df["parameter"].isin(POLLUTANTS) & (df["value"] >= 0)]
    df["ts"] = pd.to_datetime(df["datetime"], utc=True, format="ISO8601", errors="coerce").dt.floor("h")
    df = df.dropna(subset=["ts"])
    wide = df.pivot_table(index=["location_id", "ts"], columns="parameter", values="value", aggfunc="mean").reset_index()
    wide = wide.sort_values(["location_id", "ts"])
    wide["pm25_roll_24h"] = (
        wide.groupby("location_id").rolling("24h", on="ts")["pm25"].mean().reset_index(level=0, drop=True).to_numpy()
        if "pm25" in wide else float("nan")
    )
    return {"rows_in": rows_in, "rows_out": len(wide), "seconds": time.time() - started}


def pandas_worker(file_list: str, mem_limit_gb: float) -> None:
    """Runs in its own process; prints one JSON line."""
    import resource

    if mem_limit_gb > 0:
        limit = int(mem_limit_gb * 1024**3)
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    files = open(file_list).read().split("\n")
    try:
        out = pandas_job([f for f in files if f])
        out["status"] = "ok"
    except MemoryError:
        out = {"status": "out of memory (MemoryError)"}
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    out["peak_mb"] = peak / 1024 / 1024 if platform.system() == "Darwin" else peak / 1024
    print(json.dumps(out))


def run_pandas(files: list[str], mem_limit_gb: float) -> dict:
    list_path = PATHS.outputs / ".benchmark_files.txt"
    list_path.write_text("\n".join(files))
    proc = subprocess.run([sys.executable, "-m", "pipeline.benchmark", "--pandas-worker", str(list_path),
                           "--pandas-memory-limit-gb", str(mem_limit_gb)], capture_output=True, text=True)
    list_path.unlink(missing_ok=True)
    if proc.returncode != 0:
        killed = proc.returncode in (-9, 137)
        return {"status": "killed by the OS (out of memory)" if killed else f"crashed: {proc.stderr.strip().splitlines()[-1:]}"}
    return json.loads(proc.stdout.strip().splitlines()[-1])


# ------------------------------------------------------------------ Spark version

def spark_job(spark, files: list[str]) -> dict:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    started = time.time()
    df = spark.read.option("header", "true").csv(files)
    rows_in = df.count()
    df = (
        df.select(F.col("location_id").cast("long"), F.lower("parameter").alias("parameter"),
                  F.col("value").cast("double"), F.date_trunc("hour", F.to_timestamp("datetime")).alias("ts"))
        .filter(F.col("parameter").isin(*POLLUTANTS) & (F.col("value") >= 0) & F.col("ts").isNotNull())
    )
    wide = df.groupBy("location_id", "ts").pivot("parameter", POLLUTANTS).agg(F.avg("value"))
    w = Window.partitionBy("location_id").orderBy(F.col("ts").cast("long")).rangeBetween(-23 * 3600, 0)
    wide = wide.withColumn("pm25_roll_24h", F.avg("pm25").over(w))
    rows_out = wide.agg(F.count(F.lit(1)).alias("n"), F.avg("pm25_roll_24h")).first()["n"]
    return {"rows_in": rows_in, "rows_out": rows_out, "seconds": time.time() - started, "status": "ok"}


# ------------------------------------------------------------------ main

def main() -> None:
    parser = argparse.ArgumentParser(description="pandas vs Spark on the same job.")
    parser.add_argument("--fractions", type=float, nargs="+", default=[0.25, 0.5, 1.0])
    parser.add_argument("--pandas-memory-limit-gb", type=float, default=0, help="0 = no limit")
    parser.add_argument("--pandas-worker", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.pandas_worker:
        pandas_worker(args.pandas_worker, args.pandas_memory_limit_gb)
        return

    import pandas as pd

    from common.spark import get_spark

    files = sorted(str(p) for p in PATHS.raw_openaq.rglob("*.csv.gz"))
    if not files:
        raise SystemExit("No archive files in data/raw/openaq/. Download them first.")
    spark = get_spark("benchmark")
    spark_job(spark, files[: max(1, len(files) // 50)])  # warm-up so JVM start-up is not counted

    machine = f"{platform.system()}, {os.cpu_count()} cores"
    rows = []
    for fraction in args.fractions:
        subset = files[: max(1, int(len(files) * fraction))]
        print(f"\n{fraction:.0%} of the archive: {len(subset):,} files")
        p = run_pandas(subset, args.pandas_memory_limit_gb)
        print(f"  pandas: {p.get('seconds', float('nan')):.1f} s, peak {p.get('peak_mb', float('nan')):,.0f} MB, {p['status']}")
        s = spark_job(spark, subset)
        print(f"  Spark:  {s['seconds']:.1f} s within a {SPARK_DRIVER_MEMORY} memory cap, {s['rows_in']:,} rows in")
        rows.append({"fraction": fraction, "files": len(subset), "rows_in": s["rows_in"], "rows_out": s["rows_out"],
                     "pandas_seconds": p.get("seconds"), "pandas_peak_mb": p.get("peak_mb"), "pandas_status": p["status"],
                     "spark_seconds": s["seconds"], "spark_memory_cap": SPARK_DRIVER_MEMORY,
                     "pandas_memory_limit_gb": args.pandas_memory_limit_gb or "none", "machine": machine})
    spark.stop()

    result = pd.DataFrame(rows)
    PATHS.outputs.mkdir(parents=True, exist_ok=True)
    result.to_csv(PATHS.outputs / "benchmark.csv", index=False, float_format="%.2f")

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    millions = result["rows_in"] / 1e6
    ax1.plot(millions, result["pandas_seconds"], "o-", color="#e15759", label="pandas time")
    ax1.plot(millions, result["spark_seconds"], "s-", color="#4e79a7", label="Spark time")
    ax1.set_xlabel("Raw rows (millions)")
    ax1.set_ylabel("Seconds")
    ax2 = ax1.twinx()
    ax2.plot(millions, result["pandas_peak_mb"], "o--", color="#f28e2b", label="pandas peak memory")
    ax2.set_ylabel("pandas peak memory (MB)")
    lines = ax1.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
    labels = ax1.get_legend_handles_labels()[1] + ax2.get_legend_handles_labels()[1]
    ax1.legend(lines, labels, loc="upper left", fontsize=8)
    ax1.set_title(f"Same job, pandas vs Spark (Spark capped at {SPARK_DRIVER_MEMORY}); {machine}")
    fig.tight_layout()
    PATHS.outputs_charts.mkdir(parents=True, exist_ok=True)
    fig.savefig(PATHS.outputs_charts / "benchmark.png", dpi=150)
    print("\nSaved outputs/benchmark.csv and outputs/charts/benchmark.png")
    print(result[["files", "rows_in", "pandas_seconds", "pandas_peak_mb", "pandas_status", "spark_seconds"]].to_string(index=False))


if __name__ == "__main__":
    main()
