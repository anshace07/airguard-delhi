"""Exploratory data analysis in PySpark on the full clean history (Role 1: Riya Maurya).

    python -m ingestion.eda

Every heavy step is a Spark aggregation over all station-hours. Only the small results
(a few hundred rows) go to pandas/matplotlib for the picture. Each chart is saved as
PNG plus the CSV behind it in outputs/eda/; the dashboard's Trends page reads the CSVs.

    summary.csv            KPI numbers: rows, stations, period, mean PM2.5, % hours Very Poor or worse
    monthly_trend          mean PM2.5 / PM10 per month over the years
    month_of_year          seasonality: which months are worst
    hour_of_day            daily cycle (IST), winter vs the rest of the year
    weekday_weekend        does the weekend differ?
    oct_nov_daily          the Oct-Nov season day by day, Diwali dates marked
    top_stations           10 worst stations by mean PM2.5
    category_share         share of hours in each CPCB category per year
    correlation            pollutants and weather, Pearson r (pairwise, one Spark pass)
    weather_bins           mean PM2.5 by wind speed and by humidity band
    missing                % missing per column per year
    spikes                 hours with a sudden jump (z-score of the 1-hour change >= 3), per station
"""

from __future__ import annotations

import itertools

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from common.aqi import CATEGORIES, CATEGORY_COLORS, PM25_UPPER  # noqa: E402
from common.config import LOCAL_TZ, PATHS  # noqa: E402
from common.schemas import POLLUTANTS, WEATHER  # noqa: E402
from common.spark import category_col, get_spark  # noqa: E402

# Lakshmi Puja dates. Check these against a calendar before quoting them in the report.
DIWALI = {2018: "2018-11-07", 2019: "2019-10-27", 2020: "2020-11-14", 2021: "2021-11-04", 2022: "2022-10-24",
          2023: "2023-11-12", 2024: "2024-10-31", 2025: "2025-10-20", 2026: "2026-11-08"}
OUT = PATHS.outputs_eda


def save(df: pd.DataFrame, name: str, fig=None) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / f"{name}.csv", index=False, float_format="%.3f")
    if fig is not None:
        fig.tight_layout()
        fig.savefig(OUT / f"{name}.png", dpi=150)
        plt.close(fig)
    print(f"  {name}")


def category_bands(ax) -> None:
    """Light CPCB colour bands behind a PM2.5 axis."""
    lows = (0,) + PM25_UPPER
    highs = PM25_UPPER + (1000,)
    for name, lo, hi in zip(CATEGORIES, lows, highs):
        ax.axhspan(lo, hi, color=CATEGORY_COLORS[name], alpha=0.08, linewidth=0)


def main() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    spark = get_spark("eda")
    df = spark.read.parquet(str(PATHS.clean_readings))
    local = F.from_utc_timestamp("ts_utc", LOCAL_TZ)
    df = (
        df.withColumn("local_ts", local)
        .withColumn("local_date", F.to_date(local))
        .withColumn("ym", F.date_format(local, "yyyy-MM"))
        .withColumn("month", F.month(local))
        .withColumn("hour", F.hour(local))
        .withColumn("dow", F.dayofweek(local))
        .withColumn("lyear", F.year(local))
        .withColumn("winter", F.col("month").isin(11, 12, 1, 2))
        .cache()
    )
    print(f"Writing EDA to {OUT}")

    # KPIs
    k = df.agg(
        F.count(F.lit(1)).alias("station_hours"),
        F.countDistinct("station_id").alias("stations"),
        F.min("ts_utc").alias("first_hour_utc"),
        F.max("ts_utc").alias("last_hour_utc"),
        F.avg("pm25").alias("mean_pm25"),
        (F.avg((F.col("pm25") > PM25_UPPER[3]).cast("double")) * 100).alias("pct_hours_very_poor_or_worse"),
        (F.avg((F.col("pm25") > PM25_UPPER[4]).cast("double")) * 100).alias("pct_hours_severe"),
        (F.avg(F.col("pm25").isNull().cast("double")) * 100).alias("pct_pm25_missing"),
    ).toPandas()
    save(k, "summary")

    # Monthly trend
    m = df.groupBy("ym").agg(F.avg("pm25").alias("pm25"), F.avg("pm10").alias("pm10"), F.count("pm25").alias("n")).orderBy("ym").toPandas()
    fig, ax = plt.subplots(figsize=(12, 4))
    category_bands(ax)
    ax.plot(m["ym"], m["pm25"], marker="o", label="PM2.5", color="#e15759")
    ax.plot(m["ym"], m["pm10"], marker="o", label="PM10", color="#4e79a7", alpha=0.7)
    ax.set_ylim(0, max(60, np.nanmax(m[["pm25", "pm10"]].to_numpy()) * 1.1))
    step = max(1, len(m) // 18)
    ax.set_xticks(range(0, len(m), step), m["ym"].iloc[::step], rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("µg/m³ (monthly mean)")
    ax.set_title("Monthly mean PM2.5 and PM10, all stations (background = CPCB PM2.5 bands)")
    ax.legend()
    save(m, "monthly_trend", fig)

    # Month of year
    moy = df.groupby("month").agg(F.avg("pm25").alias("mean_pm25"), F.expr("percentile_approx(pm25, 0.9)").alias("p90_pm25")).orderBy("month").toPandas()
    fig, ax = plt.subplots(figsize=(9, 4))
    colors = [CATEGORY_COLORS[_cat(v)] for v in moy["mean_pm25"]]
    ax.bar(moy["month"], moy["mean_pm25"], color=colors, edgecolor="#333", linewidth=0.4, label="mean")
    ax.plot(moy["month"], moy["p90_pm25"], "k--", marker=".", label="90th percentile")
    ax.set_xticks(range(1, 13), ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    ax.set_ylabel("PM2.5 (µg/m³)")
    ax.set_title("Seasonality: PM2.5 by month (bar colour = CPCB category of the mean)")
    ax.legend()
    save(moy, "month_of_year", fig)

    # Hour of day, winter vs rest
    hod = df.groupBy("hour", "winter").agg(F.avg("pm25").alias("mean_pm25")).toPandas()
    hod = hod.pivot(index="hour", columns="winter", values="mean_pm25").rename(columns={True: "winter_nov_feb", False: "rest_of_year"}).reset_index()
    fig, ax = plt.subplots(figsize=(9, 4))
    for col, color in (("winter_nov_feb", "#e15759"), ("rest_of_year", "#59a14f")):
        if col in hod:
            ax.plot(hod["hour"], hod[col], marker="o", label=col.replace("_", " "), color=color)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("Hour of day (IST)")
    ax.set_ylabel("Mean PM2.5 (µg/m³)")
    ax.set_title("Daily cycle of PM2.5")
    ax.legend()
    save(hod, "hour_of_day", fig)

    # Weekday vs weekend
    wk = df.withColumn("day_type", F.when(F.col("dow").isin(1, 7), "weekend").otherwise("weekday")).groupBy("day_type").agg(
        F.avg("pm25").alias("mean_pm25"), F.avg("no2").alias("mean_no2")).toPandas()
    fig, ax = plt.subplots(figsize=(5, 4))
    x = np.arange(len(wk))
    ax.bar(x - 0.2, wk["mean_pm25"], 0.4, label="PM2.5", color="#e15759")
    ax.bar(x + 0.2, wk["mean_no2"], 0.4, label="NO₂ (traffic marker)", color="#4e79a7")
    ax.set_xticks(x, wk["day_type"])
    ax.set_ylabel("µg/m³")
    ax.set_title("Weekday vs weekend")
    ax.legend()
    save(wk, "weekday_weekend", fig)

    # Oct-Nov season, day by day, Diwali marked
    on = df.filter(F.col("month").isin(10, 11)).groupBy("lyear", "local_date").agg(F.avg("pm25").alias("mean_pm25")).orderBy("local_date").toPandas()
    if not on.empty:
        on["day_of_season"] = (pd.to_datetime(on["local_date"]) - pd.to_datetime(on["lyear"].astype(str) + "-10-01")).dt.days
        fig, ax = plt.subplots(figsize=(12, 4.5))
        category_bands(ax)
        for year, part in on.groupby("lyear"):
            line = ax.plot(part["day_of_season"], part["mean_pm25"], label=str(year))[0]
            if year in DIWALI:
                d = (pd.Timestamp(DIWALI[year]) - pd.Timestamp(f"{year}-10-01")).days
                ax.axvline(d, color=line.get_color(), linestyle=":", linewidth=1)
        ax.set_xticks([0, 15, 31, 46, 60], ["1 Oct", "16 Oct", "1 Nov", "16 Nov", "30 Nov"])
        ax.set_ylim(0, max(150, on["mean_pm25"].max() * 1.1))
        ax.set_ylabel("Daily mean PM2.5 (µg/m³)")
        ax.set_title("October-November (stubble burning + Diwali season); dotted line = Diwali")
        ax.legend(fontsize=8)
        save(on, "oct_nov_daily", fig)

    # Top 10 stations
    top = (
        df.groupBy("station_id", "station_name", "city").agg(F.avg("pm25").alias("mean_pm25"), F.count("pm25").alias("hours"))
        .filter(F.col("hours") >= 24 * 30).orderBy(F.desc("mean_pm25")).limit(10).toPandas()
    )
    if not top.empty:
        fig, ax = plt.subplots(figsize=(9, 5))
        part = top.iloc[::-1]
        ax.barh(part["station_name"].str.slice(0, 45), part["mean_pm25"], color=[CATEGORY_COLORS[_cat(v)] for v in part["mean_pm25"]])
        ax.set_xlabel("Mean PM2.5 (µg/m³), stations with 30+ days of data")
        ax.set_title("The 10 most polluted stations")
        save(top, "top_stations", fig)

    # Category share per year
    cs = df.filter(F.col("pm25").isNotNull()).withColumn("category", category_col("pm25")).groupBy("lyear", "category").count().toPandas()
    if not cs.empty:
        cs["share_pct"] = cs["count"] / cs.groupby("lyear")["count"].transform("sum") * 100
        table = cs.pivot(index="lyear", columns="category", values="share_pct").reindex(columns=CATEGORIES).fillna(0)
        fig, ax = plt.subplots(figsize=(9, 4))
        bottom = np.zeros(len(table))
        for c in CATEGORIES:
            ax.bar(table.index.astype(str), table[c], bottom=bottom, color=CATEGORY_COLORS[c], label=c)
            bottom += table[c].to_numpy()
        ax.set_ylabel("% of station-hours")
        ax.set_title("How many hours fall in each CPCB category (hourly PM2.5 indicator)")
        ax.legend(fontsize=8, bbox_to_anchor=(1.01, 1), loc="upper left")
        save(cs, "category_share", fig)

    # Correlation matrix: pairwise complete, all pairs in ONE Spark pass
    cols = POLLUTANTS + WEATHER
    aggs = []
    for a, b in itertools.combinations(cols, 2):
        both = F.col(a).isNotNull() & F.col(b).isNotNull()
        x = F.when(both, F.col(a))
        y = F.when(both, F.col(b))
        aggs += [F.count(x).alias(f"n|{a}|{b}"), F.sum(x).alias(f"sx|{a}|{b}"), F.sum(y).alias(f"sy|{a}|{b}"),
                 F.sum(x * x).alias(f"sxx|{a}|{b}"), F.sum(y * y).alias(f"syy|{a}|{b}"), F.sum(x * y).alias(f"sxy|{a}|{b}")]
    s = df.agg(*aggs).first().asDict()
    corr = pd.DataFrame(np.eye(len(cols)), index=cols, columns=cols)
    for a, b in itertools.combinations(cols, 2):
        n = s[f"n|{a}|{b}"] or 0
        if n > 2:
            sx, sy, sxx, syy, sxy = (s[f"{k}|{a}|{b}"] for k in ("sx", "sy", "sxx", "syy", "sxy"))
            den = np.sqrt(max(n * sxx - sx * sx, 0) * max(n * syy - sy * sy, 0))
            r = (n * sxy - sx * sy) / den if den else np.nan
        else:
            r = np.nan
        corr.loc[a, b] = corr.loc[b, a] = r
    fig, ax = plt.subplots(figsize=(8, 6.5))
    im = ax.imshow(corr.to_numpy(dtype=float), cmap="RdBu_r", vmin=-1, vmax=1)
    for i in range(len(cols)):
        for j in range(len(cols)):
            v = corr.iat[i, j]
            ax.text(j, i, "" if np.isnan(v) else f"{v:.2f}", ha="center", va="center", fontsize=7)
    ax.set_xticks(range(len(cols)), cols, rotation=45, ha="right")
    ax.set_yticks(range(len(cols)), cols)
    ax.set_title("Correlation between pollutants and weather (Pearson r)")
    fig.colorbar(im, ax=ax)
    save(corr.reset_index().rename(columns={"index": "variable"}), "correlation", fig)

    # PM2.5 vs wind and humidity
    wind = df.filter(F.col("wind_speed_ms").isNotNull()).withColumn("bin", F.least(F.floor("wind_speed_ms"), F.lit(8))).groupBy("bin").agg(
        F.avg("pm25").alias("mean_pm25"), F.count("pm25").alias("n")).orderBy("bin").toPandas()
    hum = df.filter(F.col("humidity_pct").isNotNull()).withColumn("bin", F.floor(F.col("humidity_pct") / 10) * 10).groupBy("bin").agg(
        F.avg("pm25").alias("mean_pm25"), F.count("pm25").alias("n")).orderBy("bin").toPandas()
    if not wind.empty or not hum.empty:
        wind.insert(0, "variable", "wind_speed_ms")
        hum.insert(0, "variable", "humidity_pct")
        fig, axes = plt.subplots(1, 2, figsize=(11, 4))
        axes[0].bar(wind["bin"].astype(int).astype(str) + "-" + (wind["bin"].astype(int) + 1).astype(str), wind["mean_pm25"], color="#4e79a7")
        axes[0].set_xlabel("Wind speed (m/s); last bar = 8+")
        axes[0].set_ylabel("Mean PM2.5 (µg/m³)")
        axes[0].set_title("Calm air traps pollution")
        axes[1].bar(hum["bin"].astype(int).astype(str) + "%", hum["mean_pm25"], color="#76b7b2")
        axes[1].set_xlabel("Relative humidity band")
        axes[1].set_title("PM2.5 by humidity")
        save(pd.concat([wind, hum], ignore_index=True), "weather_bins", fig)

    # Missing data per year
    miss = df.groupBy("lyear").agg(*[(F.avg(F.col(c).isNull().cast("double")) * 100).alias(c) for c in cols]).orderBy("lyear").toPandas()
    fig, ax = plt.subplots(figsize=(10, 3.5))
    im = ax.imshow(miss[cols].to_numpy(dtype=float), cmap="Oranges", vmin=0, vmax=100, aspect="auto")
    for i in range(len(miss)):
        for j, c in enumerate(cols):
            ax.text(j, i, f"{miss.iloc[i][c]:.0f}", ha="center", va="center", fontsize=8)
    ax.set_xticks(range(len(cols)), cols, rotation=30, ha="right")
    ax.set_yticks(range(len(miss)), miss["lyear"].astype(str))
    ax.set_title("% missing per column and year (of the station-hours that exist)")
    fig.colorbar(im, ax=ax)
    save(miss, "missing", fig)

    # Spikes: z-score of the 1-hour change, per station
    w = Window.partitionBy("station_id").orderBy(F.col("ts_utc").cast("long")).rangeBetween(-3600, -3600)
    ch = df.withColumn("change", F.col("pm25") - F.first("pm25").over(w))
    stats = ch.groupBy("station_id").agg(F.avg("change").alias("mu"), F.stddev("change").alias("sd"))
    spikes = (
        ch.join(stats, "station_id")
        .withColumn("z", (F.col("change") - F.col("mu")) / F.col("sd"))
        .groupBy("station_id", "station_name")
        .agg(F.sum((F.col("z") >= 3).cast("int")).alias("spike_hours"), F.count("change").alias("hours_with_change"))
        .orderBy(F.desc("spike_hours"))
        .toPandas()
    )
    if not spikes.empty:
        spikes["spike_pct"] = spikes["spike_hours"] / spikes["hours_with_change"].clip(lower=1) * 100
        top_s = spikes.head(10).iloc[::-1]
        fig, ax = plt.subplots(figsize=(9, 4.5))
        ax.barh(top_s["station_name"].str.slice(0, 45), top_s["spike_hours"], color="#f28e2b")
        ax.set_xlabel("Hours with a sudden jump (1-hour change, z-score >= 3)")
        ax.set_title("Anomalies: stations with the most sudden PM2.5 spikes")
        save(spikes, "spikes", fig)

    df.unpersist()
    spark.stop()


def _cat(value: float) -> str:
    for name, upper in zip(CATEGORIES, PM25_UPPER):
        if value <= upper:
            return name
    return CATEGORIES[-1]


if __name__ == "__main__":
    main()
