"""Build the LMS upload: code + results + charts + a data sample + sources, in one zip.

    python scripts/package_submission.py
    python scripts/package_submission.py --sample-days 31     # size of the data sample

Writes submission/AirGuard_submission_<date>.zip (the submission/ folder is not committed).
Add the report PDF and the plagiarism report to the LMS yourselves.

What goes in:
  code/        every file tracked by git (source, README, CONTRACT, docs, tests)
  results/     ml/results/*.csv, outputs/charts/*.png, outputs/eda/*, outputs/*.csv|md|json
  screenshots/ outputs/screenshots/* (take them during the final run)
  data_sample/ the last N days of the clean readings (CSV) + stations.csv + latest predictions
  DATA_SOURCES.md  where the full dataset comes from and the exact commands to re-download it
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from common.config import PATHS  # noqa: E402

SOURCES = """# Data sources and how to get the full dataset

The full dataset is too large for the LMS, so this zip has a sample plus the exact way to rebuild it.

| Data | Source | Licence / terms |
|---|---|---|
| Historical hourly air quality (training) | OpenAQ archive on AWS, bucket `openaq-data-archive`, `records/csv.gz/` | OpenAQ open data (check each provider's licence on openaq.org) |
| Live readings | OpenAQ API v3 (https://api.openaq.org/v3), free API key | as above |
| Weather, historical + live | Open-Meteo (archive-api.open-meteo.com, api.open-meteo.com) | free for non-commercial use |

Rebuild everything (from the repo root, Python venv active, OPENAQ_API_KEY in .env):

    bash scripts/run_batch.sh                 # stations -> archive -> weather -> clean -> features -> models -> charts -> EDA

Measured size of the dataset we used: see results/dataset_numbers.md.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Zip the project for the LMS.")
    parser.add_argument("--sample-days", type=int, default=31)
    args = parser.parse_args()

    out_dir = REPO / "submission"
    out_dir.mkdir(exist_ok=True)
    target = out_dir / f"AirGuard_submission_{date.today():%Y%m%d}.zip"
    tracked = subprocess.run(["git", "ls-files"], cwd=REPO, capture_output=True, text=True, check=True).stdout.split()

    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        for rel in tracked:
            if (REPO / rel).is_file():
                z.write(REPO / rel, f"code/{rel}")
        for folder, pattern, dest in [(PATHS.results, "*.csv", "results/ml"), (PATHS.outputs_charts, "*.png", "results/charts"),
                                      (PATHS.outputs_eda, "*", "results/eda"), (PATHS.outputs, "*.csv", "results"),
                                      (PATHS.outputs, "*.md", "results"), (PATHS.outputs, "*.json", "results"),
                                      (PATHS.outputs_screenshots, "*", "screenshots")]:
            for f in sorted(folder.glob(pattern)) if folder.exists() else []:
                if f.is_file() and not f.name.startswith("."):
                    z.write(f, f"{dest}/{f.name}")
        z.writestr("DATA_SOURCES.md", SOURCES)
        for f in (PATHS.stations_csv, PATHS.predictions_latest):
            if f.exists():
                z.write(f, f"data_sample/{f.name}")
        if PATHS.clean_readings.exists():
            from pyspark.sql import functions as F

            from common.spark import get_spark

            spark = get_spark("package_submission")
            clean = spark.read.parquet(str(PATHS.clean_readings))
            last = clean.agg(F.max("ts_utc")).first()[0]
            sample = clean.filter(F.col("ts_utc") > F.lit(last) - F.expr(f"INTERVAL {args.sample_days} DAYS")).drop("year")
            sample = sample.withColumn("ts_utc", F.date_format("ts_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'")).orderBy("station_id", "ts_utc")
            csv_path = out_dir / f"clean_readings_last_{args.sample_days}_days.csv"
            sample.toPandas().to_csv(csv_path, index=False)
            spark.stop()
            z.write(csv_path, f"data_sample/{csv_path.name}")
            csv_path.unlink()
    print(f"Wrote {target} ({target.stat().st_size / 1e6:.1f} MB). Add the report PDF and plagiarism report on the LMS.")


if __name__ == "__main__":
    main()
