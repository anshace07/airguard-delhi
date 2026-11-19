"""AirGuard Delhi-NCR dashboard (Role 4: Riya Katiyar).

    streamlit run dashboard/app.py

Four pages (sidebar):
  Live map           stations coloured by CPCB category, alerts, worst stations, 1/3/6 h forecast
                     per station. Refreshes itself every 60 s. REPLAY / SAMPLE banners when relevant.
  What-if            sliders for wind, humidity and current PM2.5 call ml.score.score().
  Trends (BI)        EDA insights + KPIs from outputs/eda/ (seasonality, Diwali, daily cycle, weather...).
  Model performance  baseline vs 4 models per horizon, weather experiment, confusion matrix, ROC, importance.

Where the data comes from (all files, no server in between):
  data/predictions/latest.csv        written by pipeline.stream_job every micro-batch
  data/samples/SAMPLE_predictions_latest.csv   fake data from dashboard.make_sample (backup)
  data/features/latest_by_station.parquet + ml/models/   for the what-if page
  outputs/eda/*.csv, ml/results/*.csv, outputs/*.csv     for the BI pages
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Run from anywhere: make the repo root importable (CLAUDE.md), and keep the
# dashboard's own Spark (what-if page only) small so it fits next to the stream job.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("SPARK_DRIVER_MEMORY", "1g")

import json  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from common.aqi import CATEGORIES, CATEGORY_COLORS, PM25_UPPER, pm25_category  # noqa: E402
from common.config import HORIZONS, LOCAL_TZ, PATHS  # noqa: E402

SAMPLE_FILE = PATHS.samples / "SAMPLE_predictions_latest.csv"
TREND_ARROW = {"rising": "↑ rising", "stable": "→ stable", "falling": "↓ falling"}
CATEGORY_RANGES = {name: f"{lo:.0f}-{hi:.0f}" for name, lo, hi in zip(CATEGORIES, (0,) + tuple(u + 1 for u in PM25_UPPER), PM25_UPPER)}
CATEGORY_RANGES["Severe"] = f">{PM25_UPPER[-1]:.0f}"

st.set_page_config(page_title="AirGuard Delhi-NCR", page_icon="🌫️", layout="wide")


# ----------------------------------------------------------------- helpers

def ist(series: pd.Series, fmt: str = "%d %b %H:%M") -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_convert(LOCAL_TZ).dt.strftime(fmt) + " IST"


def mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


@st.cache_data(show_spinner=False)
def read_csv(path: str, stamp: float) -> pd.DataFrame | None:
    """Cached on the file's modification time, so a new file is picked up immediately."""
    try:
        return pd.read_csv(path)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return None


def csv(path: Path) -> pd.DataFrame | None:
    return read_csv(str(path), mtime(path))


def chip(category: str | None) -> str:
    if not category or category not in CATEGORY_COLORS:
        return "–"
    text_color = "#000" if category in ("Satisfactory", "Moderate", "Poor") else "#fff"
    return (f"<span style='background:{CATEGORY_COLORS[category]};color:{text_color};padding:2px 8px;"
            f"border-radius:10px;font-size:0.85em;white-space:nowrap'>{category}</span>")


def legend_html() -> str:
    return "<br>".join(f"{chip(c)} <span style='font-size:0.8em;color:#666'>{CATEGORY_RANGES[c]} µg/m³</span>"
                       for c in CATEGORIES)


MODEL_COLORS = {"Persistence": "#9e9e9e", "LinearRegression": "#4e79a7", "LogisticRegression": "#4e79a7",
                "DecisionTree": "#f28e2b", "RandomForest": "#59a14f", "GBT": "#b07aa1"}


def need(message: str, command: str) -> None:
    st.info(f"{message}\n\n```bash\n{command}\n```")


# ----------------------------------------------------------------- sidebar

st.sidebar.title("🌫️ AirGuard Delhi-NCR")
page = st.sidebar.radio("Page", ["Live map", "What-if", "Trends (BI)", "Model performance"], label_visibility="collapsed")
st.sidebar.divider()
real_exists = PATHS.predictions_latest.exists()
use_sample = st.sidebar.toggle(
    "Use SAMPLE data", value=not real_exists,
    help="Fake predictions from dashboard/make_sample.py, for building or as a last-resort backup.",
)
simple_map = st.sidebar.toggle(
    "Simple map (works offline)", value=False,
    help="Use this if the street map stays blank (no internet for map tiles, or no WebGL in the browser).",
)
st.sidebar.caption("Hourly PM2.5 indicator using CPCB breakpoints. The official AQI uses 24-hour averages "
                   "of several pollutants, so this is an indicator, not the official AQI.")
st.sidebar.markdown(legend_html(), unsafe_allow_html=True)


def load_predictions() -> tuple[pd.DataFrame | None, bool]:
    path = SAMPLE_FILE if use_sample else PATHS.predictions_latest
    return csv(path), use_sample


# ----------------------------------------------------------------- page: live map

def live_page() -> None:
    df, is_sample = load_predictions()
    st.title("Live PM2.5 forecasts, station by station")
    if df is None or df.empty:
        if use_sample:
            need("No sample file yet.", "python -m dashboard.make_sample")
        else:
            need("No live predictions yet. Start the fetcher and the stream job (README, 'Run the whole system').",
                 "python -m pipeline.stream_job")
        return

    if is_sample:
        st.warning("**SAMPLE DATA**: made-up stations and numbers for testing the dashboard. Not real air quality.", icon="⚠️")
    if (df["source"] == "replay").any():
        st.error("**REPLAY MODE**: this is a stored past day being replayed through the live pipeline, not live data.", icon="⏪")

    now_rows = df[df["horizon_h"] == HORIZONS[0]].copy()
    newest = pd.to_datetime(df["ts_utc"], utc=True).max()
    age_h = (pd.Timestamp.now(tz="UTC") - newest).total_seconds() / 3600
    if not is_sample and (df["source"] == "live").all() and age_h > 3:
        st.info(f"The newest reading is {age_h:.0f} hours old. Is the live fetcher running?")

    alerts = df[df["alert"] == 1]
    alert_stations = alerts["station_id"].nunique()
    worst = now_rows.sort_values("hotspot_rank").iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stations reporting", len(now_rows))
    c2.metric("Stations with an alert", alert_stations)
    c3.metric("Worst station now", f"{worst['current_pm25']:.0f} µg/m³")
    c3.caption(worst["station_name"])
    c4.metric("Latest reading", ist(pd.Series([newest]), "%d %b %H:%M").iloc[0])
    c4.caption("Forecast made " + ist(df["scored_at_utc"].iloc[[0]], "%d %b %H:%M").iloc[0])

    left, right = st.columns([2.2, 1])
    with left:
        h3 = df[df["horizon_h"] == 3][["station_id", "predicted_pm25", "predicted_category"]].rename(
            columns={"predicted_pm25": "forecast_3h", "predicted_category": "category_3h"})
        m = now_rows.merge(h3, on="station_id", how="left")
        m["size"] = np.clip(m["current_pm25"], 30, 400)
        m["trend_text"] = m["trend"].map(TREND_ARROW)
        hover = {"current_pm25": ":.0f", "trend_text": True, "forecast_3h": ":.0f", "category_3h": True,
                 "lat": False, "lon": False, "size": False, "current_category": True}
        kwargs = dict(lat="lat", lon="lon", color="current_category", color_discrete_map=CATEGORY_COLORS,
                      category_orders={"current_category": list(CATEGORIES)}, size="size", size_max=22,
                      hover_name="station_name", hover_data=hover, zoom=9, height=560,
                      center={"lat": float(m["lat"].mean()), "lon": float(m["lon"].mean())})
        if simple_map:
            # Plain longitude/latitude scatter: no map tiles, no WebGL, always renders.
            for key in ("zoom", "center", "lat", "lon"):
                kwargs.pop(key)
            fig = px.scatter(m, x="lon", y="lat", text=m["station_name"].str.split(",").str[0], **kwargs,
                             labels={"current_category": "Now", "lon": "longitude", "lat": "latitude"})
            fig.update_traces(textposition="top center", textfont_size=9)
            fig.update_yaxes(scaleanchor="x", scaleratio=1.14)  # ~equal km per degree at Delhi's latitude
        else:
            try:
                fig = px.scatter_map(m, map_style="carto-positron", **kwargs, labels={"current_category": "Now"})
            except AttributeError:  # older plotly
                fig = px.scatter_mapbox(m, mapbox_style="carto-positron", **kwargs, labels={"current_category": "Now"})
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend=dict(yanchor="top", y=0.98, xanchor="left", x=0.01,
                          bgcolor="rgba(255,255,255,0.8)"))
        st.plotly_chart(fig, width="stretch")

    with right:
        st.subheader(f"🚨 Active alerts ({alert_stations})")
        if alerts.empty:
            st.success("No station is forecast to reach Very Poor or Severe.")
        else:
            first = alerts.sort_values(["hotspot_rank", "horizon_h"]).groupby("station_id", sort=False).first().reset_index()
            for r in first.head(8).itertuples():
                st.markdown(f"**{r.station_name}**  \n{r.alert_reason}")
            if len(first) > 8:
                st.caption(f"... and {len(first) - 8} more")
        st.subheader("Worst 5 right now")
        top5 = now_rows.sort_values("hotspot_rank").head(5)
        st.markdown("\n".join(
            f"{int(r.hotspot_rank)}. {r.station_name[:34]}: **{r.current_pm25:.0f}** {chip(r.current_category)} {TREND_ARROW.get(r.trend, '')}"
            for r in top5.itertuples()), unsafe_allow_html=True)

    st.divider()
    st.subheader("Station forecast")
    order = now_rows.sort_values("hotspot_rank")
    labels = {int(r.station_id): f"#{int(r.hotspot_rank)} {r.station_name} ({r.current_pm25:.0f})" for r in order.itertuples()}
    station = st.selectbox("Station (sorted worst first)", list(labels), format_func=labels.get, key="live_station")
    srows = df[df["station_id"] == station].sort_values("horizon_h")
    s0 = srows.iloc[0]
    a, b = st.columns([1, 2])
    with a:
        st.metric("PM2.5 now", f"{s0['current_pm25']:.0f} µg/m³", TREND_ARROW.get(s0["trend"], s0["trend"]),
                  delta_color="inverse" if s0["trend"] != "stable" else "off")
        st.markdown(f"Now: {chip(s0['current_category'])}  \nReading: {ist(srows['ts_utc'].iloc[[0]]).iloc[0]}",
                    unsafe_allow_html=True)
        for r in srows.itertuples():
            st.markdown(f"**+{r.horizon_h} h** ({ist(pd.Series([r.target_ts_utc]), '%H:%M').iloc[0]}): "
                        f"{r.predicted_pm25:.0f} µg/m³ {chip(r.predicted_category)}", unsafe_allow_html=True)
        reasons = [r for r in srows["alert_reason"].fillna("") if r]
        if reasons:
            st.error("**Alert:** " + " | ".join(dict.fromkeys(reasons)))
        else:
            st.success("No alert for this station.")
        st.caption(f"Model: {', '.join(srows['model_name'].astype(str).unique())}")
    with b:
        bars = pd.DataFrame({"when": ["now"] + [f"+{h} h" for h in srows["horizon_h"]],
                             "pm25": [s0["current_pm25"]] + list(srows["predicted_pm25"])})
        bars["category"] = bars["pm25"].map(pm25_category)
        fig = px.bar(bars, x="when", y="pm25", color="category", color_discrete_map=CATEGORY_COLORS,
                     category_orders={"category": list(CATEGORIES)}, text_auto=".0f", height=320)
        for name, upper in zip(CATEGORIES, PM25_UPPER):
            fig.add_hline(y=upper, line_dash="dot", line_color=CATEGORY_COLORS[name], opacity=0.6)
        fig.update_layout(yaxis_title="PM2.5 (µg/m³)", xaxis_title="", margin=dict(t=10, b=10), showlegend=True)
        st.plotly_chart(fig, width="stretch")


# ----------------------------------------------------------------- page: what-if

@st.cache_resource(show_spinner=False)
def what_if_module():
    from ml import score as score_module

    return score_module


@st.cache_data(show_spinner=False)
def wind_sweep(station: int, horizon: int, humidity: float, pm25: float, change: float | None, stamp: float) -> pd.DataFrame:
    module = what_if_module()
    winds = [0.5, 1, 1.5, 2, 3, 4, 5, 6, 8, 10]
    extra = {"humidity_pct": humidity, "pm25": pm25}
    if change is not None:
        extra["pm25_change_1h"] = change
    res = module.score_many(station, horizon, [{"wind_speed_ms": w, **extra} for w in winds])
    return pd.DataFrame({"wind_speed_ms": winds, "predicted_pm25": [r["predicted_pm25"] for r in res]})


def what_if_page() -> None:
    st.title("What-if: how would the forecast change?")
    st.caption("**Model scenario, not proof of cause.** It shows what the trained model predicts if only these "
               "inputs were different. In reality, wind also changes other things at the same time.")
    if not any((PATHS.models / f"pm25_h{h}").exists() for h in HORIZONS):
        need("No trained models yet.", "python -m ml.train_regression")
        return
    if not PATHS.features_latest.exists():
        need("No feature snapshot yet (written by build_features and by the stream job).", "python -m pipeline.build_features")
        return

    rows = pd.read_parquet(PATHS.features_latest).sort_values("pm25", ascending=False)
    labels = {int(r.station_id): f"{r.station_name} (PM2.5 {r.pm25:.0f})" for r in rows.itertuples()}
    c1, c2 = st.columns([3, 1])
    station = c1.selectbox("Station", list(labels), format_func=labels.get)
    horizon = c2.radio("Forecast horizon", [h for h in HORIZONS if (PATHS.models / f"pm25_h{h}").exists()],
                       index=1 if len(HORIZONS) > 1 else 0, horizontal=True, format_func=lambda h: f"{h} h")
    row = rows[rows["station_id"] == station].iloc[0]
    st.caption(f"Inputs from the station's newest reading: {ist(pd.Series([row['ts_utc']])).iloc[0]}")

    def val(col, default):
        v = row.get(col)
        return float(default if v is None or pd.isna(v) else v)

    wind0, hum0, pm0 = val("wind_speed_ms", 2.0), val("humidity_pct", 60.0), val("pm25", 100.0)
    s1, s2, s3 = st.columns(3)
    wind = s1.slider("Wind speed (m/s)", 0.0, 12.0, float(min(12.0, round(wind0, 1))), 0.1, help=f"now {wind0:.1f}")
    hum = s2.slider("Humidity (%)", 5.0, 100.0, float(min(100.0, max(5.0, round(hum0)))), 1.0, help=f"now {hum0:.0f}")
    pm = s3.slider("Current PM2.5 (µg/m³)", 0.0, 999.0, float(min(999.0, round(pm0))), 1.0, help=f"now {pm0:.0f}")

    lag1 = row.get("pm25_lag_1h")
    change = None if lag1 is None or pd.isna(lag1) else pm - float(lag1)
    overrides = {"wind_speed_ms": wind, "humidity_pct": hum, "pm25": pm}
    if change is not None:
        overrides["pm25_change_1h"] = change

    try:
        with st.spinner("Running the model (the first run starts Spark, about 10-20 s) ..."):
            module = what_if_module()
            base = module.score(station, horizon)
            new = module.score(station, horizon, overrides)
            sweep = wind_sweep(station, horizon, hum, pm, change, mtime(PATHS.features_latest))
    except Exception as exc:  # show the problem instead of a stack trace in the demo
        st.error(f"The what-if model could not run: {exc}")
        return

    a, b, c = st.columns(3)
    a.metric(f"Normal forecast, {horizon} h ahead", f"{base['predicted_pm25']:.0f} µg/m³")
    a.markdown(chip(base["predicted_category"]), unsafe_allow_html=True)
    b.metric("Your scenario", f"{new['predicted_pm25']:.0f} µg/m³",
             f"{new['predicted_pm25'] - base['predicted_pm25']:+.0f} µg/m³", delta_color="inverse")
    b.markdown(chip(new["predicted_category"]), unsafe_allow_html=True)
    changed = new["predicted_category"] != base["predicted_category"]
    c.markdown("**Category changes** from " + chip(base["predicted_category"]) + " to " + chip(new["predicted_category"])
               if changed else "Category stays the same.", unsafe_allow_html=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sweep["wind_speed_ms"], y=sweep["predicted_pm25"], mode="lines+markers", name="forecast"))
    fig.add_trace(go.Scatter(x=[wind], y=[new["predicted_pm25"]], mode="markers", marker=dict(size=14, color="#e15759"),
                             name="your scenario"))
    for name, upper, above in zip(CATEGORIES, PM25_UPPER, CATEGORIES[1:]):
        fig.add_hline(y=upper, line_dash="dot", line_color=CATEGORY_COLORS[above], opacity=0.6,
                      annotation_text=f"{above} above {upper:.0f}", annotation_position="top left")
    fig.update_layout(height=380, xaxis_title="Wind speed (m/s), other inputs as set above",
                      yaxis_title=f"PM2.5 forecast, {horizon} h ahead", margin=dict(t=30))
    st.plotly_chart(fig, width="stretch")
    falling = sweep["predicted_pm25"].iloc[-1] <= sweep["predicted_pm25"].iloc[0]
    st.caption("Higher wind lowers this forecast, as expected (wind disperses pollution)." if falling else
               "Here the model does not lower the forecast with more wind. That is worth explaining honestly in the "
               "report (for example, the model leans mostly on recent PM2.5 values).")


# ----------------------------------------------------------------- page: trends (BI)

def trends_page() -> None:
    st.title("Trends and insights (BI)")
    eda = PATHS.outputs_eda
    summary = csv(eda / "summary.csv")
    if summary is None:
        need("No EDA results yet.", "python -m ingestion.eda")
        return
    s = summary.iloc[0]
    c = st.columns(5)
    c[0].metric("Station-hours analysed", f"{int(s['station_hours']):,}")
    c[1].metric("Stations", int(s["stations"]))
    c[2].metric("Mean PM2.5", f"{s['mean_pm25']:.0f} µg/m³")
    c[3].metric("Hours Very Poor or worse", f"{s['pct_hours_very_poor_or_worse']:.1f}%")
    c[4].metric("Hours Severe", f"{s['pct_hours_severe']:.1f}%")
    st.caption(f"Period: {s['first_hour_utc']} to {s['last_hour_utc']} (UTC)")

    tabs = st.tabs(["Seasons", "Daily cycle", "Oct-Nov & Diwali", "Stations", "Weather", "Big data & quality"])
    with tabs[0]:
        m = csv(eda / "monthly_trend.csv")
        if m is not None:
            fig = px.line(m, x="ym", y=["pm25", "pm10"], markers=True, height=360, labels={"value": "µg/m³", "ym": "month"})
            st.plotly_chart(fig, width="stretch")
        a, b = st.columns(2)
        moy = csv(eda / "month_of_year.csv")
        if moy is not None:
            moy["category"] = moy["mean_pm25"].map(pm25_category)
            fig = px.bar(moy, x="month", y="mean_pm25", color="category", color_discrete_map=CATEGORY_COLORS,
                         category_orders={"category": list(CATEGORIES)}, height=340, title="Mean PM2.5 by month")
            a.plotly_chart(fig, width="stretch")
        cs = csv(eda / "category_share.csv")
        if cs is not None:
            fig = px.bar(cs, x="lyear", y="share_pct", color="category", color_discrete_map=CATEGORY_COLORS,
                         category_orders={"category": list(CATEGORIES)}, height=340, title="Share of hours per category",
                         labels={"lyear": "year", "share_pct": "% of hours"})
            b.plotly_chart(fig, width="stretch")
        st.markdown("**For a decision maker:** the worst months are where early warnings matter most; "
                    "plan restrictions (GRAP) and health advisories ahead of them.")
    with tabs[1]:
        hod = csv(eda / "hour_of_day.csv")
        if hod is not None:
            fig = px.line(hod, x="hour", y=[c for c in hod.columns if c != "hour"], markers=True, height=380,
                          labels={"value": "mean PM2.5 (µg/m³)", "hour": "hour of day (IST)"})
            st.plotly_chart(fig, width="stretch")
        wk = csv(eda / "weekday_weekend.csv")
        if wk is not None:
            st.dataframe(wk, hide_index=True)
    with tabs[2]:
        on = csv(eda / "oct_nov_daily.csv")
        if on is not None:
            on["lyear"] = on["lyear"].astype(str)
            fig = px.line(on, x="day_of_season", y="mean_pm25", color="lyear", height=420,
                          labels={"day_of_season": "days since 1 October", "mean_pm25": "daily mean PM2.5", "lyear": "year"})
            st.plotly_chart(fig, width="stretch")
            st.caption("Diwali dates are marked on outputs/eda/oct_nov_daily.png.")
    with tabs[3]:
        a, b = st.columns(2)
        top = csv(eda / "top_stations.csv")
        if top is not None:
            top["category"] = top["mean_pm25"].map(pm25_category)
            fig = px.bar(top.iloc[::-1], x="mean_pm25", y="station_name", orientation="h", color="category",
                         color_discrete_map=CATEGORY_COLORS, height=420, title="10 most polluted stations")
            a.plotly_chart(fig, width="stretch")
        spikes = csv(eda / "spikes.csv")
        if spikes is not None:
            fig = px.bar(spikes.head(10).iloc[::-1], x="spike_hours", y="station_name", orientation="h", height=420,
                         title="Most sudden spikes (anomalies)")
            b.plotly_chart(fig, width="stretch")
    with tabs[4]:
        a, b = st.columns(2)
        wb = csv(eda / "weather_bins.csv")
        if wb is not None:
            wind = wb[wb["variable"] == "wind_speed_ms"]
            fig = px.bar(wind, x="bin", y="mean_pm25", height=360, title="PM2.5 by wind speed (m/s)")
            a.plotly_chart(fig, width="stretch")
            hum = wb[wb["variable"] == "humidity_pct"]
            fig = px.bar(hum, x="bin", y="mean_pm25", height=360, title="PM2.5 by humidity band (%)")
            b.plotly_chart(fig, width="stretch")
        corr = csv(eda / "correlation.csv")
        if corr is not None:
            corr = corr.set_index("variable")
            fig = px.imshow(corr, text_auto=".2f", color_continuous_scale="RdBu_r", zmin=-1, zmax=1, height=480,
                            title="Correlation (Pearson r)")
            st.plotly_chart(fig, width="stretch")
    with tabs[5]:
        numbers = PATHS.outputs / "dataset_numbers.json"
        if numbers.exists():
            n = json.loads(numbers.read_text())
            cols = st.columns(4)
            for col, (key, label) in zip(cols, [("raw_rows_narrow", "Raw rows"), ("raw_files", "Raw files"),
                                                 ("raw_size_mb_compressed", "Raw MB (gz)"), ("clean_rows_station_hours", "Station-hours")]):
                if key in n:
                    col.metric(label, f"{n[key]:,}" if isinstance(n[key], int) else n[key])
        bench = csv(PATHS.outputs / "benchmark.csv")
        if bench is not None:
            st.markdown("**pandas vs Spark, same job** (outputs/benchmark.csv)")
            st.dataframe(bench, hide_index=True)
        dq = csv(PATHS.outputs / "data_quality.csv")
        if dq is not None:
            st.markdown("**Data quality** (pipeline.clean_history)")
            st.dataframe(dq, hide_index=True)
        missing = csv(eda / "missing.csv")
        if missing is not None:
            st.markdown("**% missing per column and year**")
            st.dataframe(missing.round(1), hide_index=True)


# ----------------------------------------------------------------- page: models

def models_page() -> None:
    st.title("Model performance")
    results = csv(PATHS.results_csv)
    if results is None:
        need("No results yet.", "python -m ml.train_regression && python -m ml.train_classification")
        return
    reg = results[results["task"] == "regression"]
    ww = reg[reg["feature_set"] == "with_weather"]
    if not ww.empty:
        st.subheader("Does a model beat 'later = now' (persistence)?")
        cols = st.columns(len(HORIZONS))
        for col, h in zip(cols, HORIZONS):
            part = ww[ww["horizon_h"] == h]
            if part.empty:
                continue
            base = part[part["model"] == "Persistence"]["rmse"].iloc[0]
            best = part[part["model"] != "Persistence"].sort_values("rmse").iloc[0]
            gain = (base - best["rmse"]) / base * 100
            col.metric(f"{h} h ahead: best = {best['model']}", f"RMSE {best['rmse']:.1f}",
                       f"{gain:+.1f}% vs persistence ({base:.1f})", delta_color="normal")
        st.caption("RMSE in µg/m³, lower is better. If a model does not beat persistence, we report it.")
        long = ww.melt(id_vars=["model", "horizon_h"], value_vars=["rmse", "mae"], var_name="metric")
        long["horizon"] = long["horizon_h"].astype(str) + " h"
        fig = px.bar(long, x="horizon", y="value", color="model", barmode="group", facet_col="metric", height=400,
                     category_orders={"model": ["Persistence", "LinearRegression", "DecisionTree", "RandomForest", "GBT"]},
                     color_discrete_map=MODEL_COLORS, labels={"value": "µg/m³"})
        fig.update_yaxes(matches=None, showticklabels=True)
        st.plotly_chart(fig, width="stretch")

        exp = reg[reg["model"] != "Persistence"].pivot_table(index=["horizon_h", "model"], columns="feature_set", values="rmse").dropna()
        if "pollutants_only" in exp:
            st.subheader("Weather experiment: does weather help?")
            exp = exp.reset_index()
            exp["weather helps?"] = np.where(exp["with_weather"] < exp["pollutants_only"], "yes", "no")
            st.dataframe(exp.round(2), hide_index=True)

    cls = results[results["task"] == "classification"]
    if not cls.empty:
        st.subheader("CPCB category 3 h ahead")
        st.dataframe(cls[["model", "accuracy", "precision", "recall", "f1"]].round(3), hide_index=True)
        cm = csv(PATHS.results / "confusion_matrix.csv")
        if cm is not None:
            model = st.selectbox("Confusion matrix for", list(cls.sort_values("f1", ascending=False)["model"]))
            table = cm[cm["model"] == model].pivot_table(index="actual", columns="predicted", values="count", aggfunc="sum")
            table = table.reindex(index=CATEGORIES, columns=CATEGORIES).fillna(0)
            pct = table.div(table.sum(axis=1).replace(0, np.nan), axis=0).fillna(0) * 100
            fig = px.imshow(pct, text_auto=".0f", color_continuous_scale="Blues", height=460,
                            labels={"x": "predicted", "y": "actual", "color": "% of row"})
            st.plotly_chart(fig, width="stretch")

    alert = results[results["task"] == "alert"]
    roc = csv(PATHS.results / "roc_points.csv")
    if not alert.empty:
        st.subheader("Alert: Very Poor or worse within 3 h")
        a, b = st.columns([1, 1.3])
        a.dataframe(alert[["model", "auc", "accuracy", "precision", "recall", "f1"]].round(3), hide_index=True)
        a.caption("For alerts, recall matters most: missing a Severe hour is worse than a false alarm.")
        if roc is not None:
            roc["label"] = roc["model"] + " (AUC " + roc["auc"].round(3).astype(str) + ")"
            fig = px.line(roc, x="fpr", y="tpr", color="label", height=400,
                          labels={"fpr": "false positive rate", "tpr": "true positive rate (recall)"})
            fig.add_shape(type="line", x0=0, y0=0, x1=1, y1=1, line=dict(dash="dot", color="grey"))
            b.plotly_chart(fig, width="stretch")

    fi = csv(PATHS.feature_importance_csv)
    if fi is not None and not fi.empty:
        st.subheader("Which inputs drive the forecast?")
        c1, c2 = st.columns(2)
        h = c1.radio("Horizon", sorted(fi["horizon_h"].unique()), horizontal=True, format_func=lambda x: f"{x} h", key="fi_h")
        model = c2.radio("Model", sorted(fi["model"].unique()), horizontal=True, key="fi_m")
        top = fi[(fi["horizon_h"] == h) & (fi["model"] == model)].nlargest(15, "importance").iloc[::-1]
        st.plotly_chart(px.bar(top, x="importance", y="feature", orientation="h", height=440), width="stretch")

    pva = csv(PATHS.results / "pred_vs_actual.csv")
    if pva is not None and not pva.empty:
        st.subheader("Forecast vs actual, one week at one station")
        h = st.radio("Horizon ", sorted(pva["horizon_h"].unique()), horizontal=True, format_func=lambda x: f"{x} h", key="pva_h")
        part = pva[pva["horizon_h"] == h].copy()
        part["time"] = pd.to_datetime(part["ts_utc"]) + pd.to_timedelta(h, unit="h")
        fig = px.line(part, x="time", y=["actual", "predicted", "persistence"], height=380,
                      title=f"{part['station_name'].iloc[0]} ({part['model'].iloc[0]})", labels={"value": "PM2.5 (µg/m³)"})
        st.plotly_chart(fig, width="stretch")

    with st.expander("All results (ml/results/results.csv)"):
        st.dataframe(results, hide_index=True)


# ----------------------------------------------------------------- router

if page == "Live map":
    if hasattr(st, "fragment"):
        st.fragment(run_every=60)(live_page)()
    else:
        live_page()
elif page == "What-if":
    what_if_page()
elif page == "Trends (BI)":
    trends_page()
else:
    models_page()
