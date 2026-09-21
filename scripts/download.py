"""Download and extract monthly Citi Bike trip-data archives.

Used by ``dags/citibike_batch_dag.py`` as the first step of the batch pipeline.
"""

import requests
import zipfile
import os
import time
import logging

logger = logging.getLogger(__name__)

CITIBIKE_BASE_URL = "https://s3.amazonaws.com/tripdata"


def _download_with_retry(url: str, dest_path: str, max_retries: int = 3, backoff_seconds: int = 10) -> str:
    """Retry-with-backoff on download failure — answers 'what if the endpoint is flaky.'"""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=30)
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return dest_path
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt}/{max_retries} failed for {url}: {e}")
            if attempt == max_retries:
                raise
            time.sleep(backoff_seconds * attempt)


def download_monthly_zips(year_month: str, work_dir: str = "/tmp/citibike") -> dict:
    """Downloads NYC and JC zips for year_month (e.g. '202606'). Returns {region: zip_path}."""
    os.makedirs(work_dir, exist_ok=True)
    sources = {
        "nyc": f"{CITIBIKE_BASE_URL}/{year_month}-citibike-tripdata.zip",
        "jc": f"{CITIBIKE_BASE_URL}/JC-{year_month}-citibike-tripdata.csv.zip",
    }
    downloaded = {}
    for region, url in sources.items():
        dest = os.path.join(work_dir, f"{region}_{year_month}.zip")
        logger.info(f"Downloading {region} → {url}")
        _download_with_retry(url, dest)
        downloaded[region] = dest
    return downloaded


def extract_zip(zip_path: str, extract_to: str) -> list:
    """Extracts a zip (may contain multiple CSVs for high-volume months). Returns CSV paths."""
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        names = [n for n in z.namelist() if n.endswith(".csv") and "__MACOSX" not in n]
        z.extractall(extract_to, members=names)
    return [os.path.join(extract_to, n) for n in names]


def download_and_extract(year_month: str, work_dir: str = "/tmp/citibike") -> dict:
    """Full step: returns {region: [local_csv_paths]}."""
    zips = download_monthly_zips(year_month, work_dir)
    extracted = {}
    for region, zip_path in zips.items():
        extract_dir = os.path.join(work_dir, f"{region}_extracted")
        extracted[region] = extract_zip(zip_path, extract_dir)
        logger.info(f"{region}: extracted {len(extracted[region])} CSV(s)")
    return extracted