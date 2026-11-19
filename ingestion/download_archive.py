"""Step 2: download the OpenAQ history from the public AWS archive (Role 1: Riya Maurya).

    python -m ingestion.download_archive --years 2024 2025 --dry-run   # list + size only, downloads nothing
    python -m ingestion.download_archive --years 2024 2025             # download (resumes if stopped)

Which stations: every station in data/raw/stations.csv (run ingestion.stations first;
--scope india there gives the big all-India set). Or pass --station-ids 2178 8118.

Where the data is (OpenAQ docs, "AWS quick start"):
    bucket openaq-data-archive, key records/csv.gz/locationid=<id>/year=<yyyy>/month=<mm>/location-<id>-<yyyymmdd>.csv.gz
    one gzipped CSV per station per day: location_id,sensors_id,location,datetime,lat,lon,parameter,units,value
The bucket is public, so this script uses plain HTTPS (S3's ListObjectsV2 API). No AWS
account and no AWS CLI needed. Files land in data/raw/openaq/ with the same folder layout.
Files that are already there with the right size are skipped, so you can stop and re-run.
"""

from __future__ import annotations

import argparse
import csv
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests

from common.config import PATHS

BUCKET_URL = "https://openaq-data-archive.s3.amazonaws.com"
PREFIX = "records/csv.gz/"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

_session = requests.Session()


def list_keys(station_id: int, year: int) -> list[tuple[str, int]]:
    """All (key, size) for one station and year, following S3's pagination."""
    keys, token = [], None
    while True:
        params = {"list-type": "2", "prefix": f"{PREFIX}locationid={station_id}/year={year}/"}
        if token:
            params["continuation-token"] = token
        for attempt in range(4):
            try:
                response = _session.get(BUCKET_URL + "/", params=params, timeout=60)
                response.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt * 2)
        root = ET.fromstring(response.content)
        for item in root.findall("s3:Contents", NS):
            keys.append((item.find("s3:Key", NS).text, int(item.find("s3:Size", NS).text)))
        if root.findtext("s3:IsTruncated", default="false", namespaces=NS) == "true":
            token = root.findtext("s3:NextContinuationToken", namespaces=NS)
        else:
            return keys


def download(key: str, size: int) -> str:
    """'skipped', 'downloaded' or raises after retries. Writes to a temp file, then renames."""
    target = PATHS.raw_openaq / key[len(PREFIX):]
    if target.exists() and target.stat().st_size == size:
        return "skipped"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".part")
    for attempt in range(4):
        try:
            with _session.get(f"{BUCKET_URL}/{key}", timeout=120, stream=True) as response:
                response.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in response.iter_content(1 << 16):
                        fh.write(chunk)
            tmp.replace(target)
            return "downloaded"
        except requests.RequestException:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt * 2)
    return "failed"


def station_ids_from_csv() -> list[int]:
    if not PATHS.stations_csv.exists():
        raise SystemExit("No data/raw/stations.csv yet. Run: python -m ingestion.stations (or pass --station-ids).")
    with open(PATHS.stations_csv) as fh:
        return [int(row["station_id"]) for row in csv.DictReader(fh)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Download OpenAQ archive files over HTTPS.")
    this_year = date.today().year
    parser.add_argument("--years", type=int, nargs="+", default=[this_year - 1, this_year])
    parser.add_argument("--station-ids", type=int, nargs="*", help="default: all of data/raw/stations.csv")
    parser.add_argument("--max-stations", type=int, default=0, help="only the first N stations (quick test)")
    parser.add_argument("--workers", type=int, default=16, help="parallel downloads")
    parser.add_argument("--dry-run", action="store_true", help="list files and total size, download nothing")
    args = parser.parse_args()

    ids = args.station_ids or station_ids_from_csv()
    if args.max_stations:
        ids = ids[: args.max_stations]
    jobs = [(s, y) for s in ids for y in args.years]
    print(f"Listing archive files for {len(ids)} stations x {len(args.years)} years ...")

    started = time.time()
    files: list[tuple[str, int]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(list_keys, s, y): (s, y) for s, y in jobs}
        for future in as_completed(futures):
            try:
                files.extend(future.result())
            except requests.RequestException as exc:
                print(f"  could not list station {futures[future]}: {exc.__class__.__name__}")
    total_bytes = sum(size for _, size in files)
    with_data = len({k.split("/")[2] for k, _ in files})
    print(f"Found {len(files):,} files, {total_bytes / 1e6:,.1f} MB compressed, "
          f"{with_data} of {len(ids)} stations have data in {args.years} ({time.time() - started:.0f} s)")
    if args.dry_run or not files:
        return

    done = {"downloaded": 0, "skipped": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(download, k, s) for k, s in files]
        for i, future in enumerate(as_completed(futures), 1):
            try:
                done[future.result()] += 1
            except requests.RequestException:
                done["failed"] += 1
            if i % 500 == 0 or i == len(files):
                print(f"  {i:,}/{len(files):,} files  ({done['downloaded']:,} new, {done['skipped']:,} already there, "
                      f"{done['failed']:,} failed)  {time.time() - started:.0f} s")
    on_disk = sum(p.stat().st_size for p in PATHS.raw_openaq.rglob("*.csv.gz"))
    print(f"Done. data/raw/openaq/ now holds {on_disk / 1e6:,.1f} MB."
          + (" Some files failed: just run the same command again." if done["failed"] else ""))


if __name__ == "__main__":
    main()
