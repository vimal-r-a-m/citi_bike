"""Refresh the PostgreSQL station-reference table from Citi Bike GBFS data.

Used by ``dags/station_reference_dag.py`` on its daily schedule to maintain
the mapping between GBFS station IDs and historical station names.
"""

import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import time
import logging

logger = logging.getLogger(__name__)

STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
PG_CONN_PARAMS = dict(
    host="postgres-warehouse", port=5432,
    dbname="bikeshare", user="warehouse", password="warehouse"
)


def _fetch_with_retry(url: str, max_retries: int = 3, backoff_seconds: int = 10) -> dict:
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("Attempt %d/%d failed: %s", attempt, max_retries, e)
            if attempt == max_retries:
                raise
            time.sleep(backoff_seconds * attempt)


def refresh_station_reference() -> int:
    payload = _fetch_with_retry(STATION_INFO_URL)
    stations = payload["data"]["stations"]
    now = datetime.now(timezone.utc)

    rows = [(s["station_id"], s.get("short_name"), s.get("name"), now) for s in stations]

    conn = psycopg2.connect(**PG_CONN_PARAMS)
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO station_reference (gbfs_station_id, short_name, station_name, last_refreshed)
                VALUES %s
                ON CONFLICT (gbfs_station_id)
                DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    station_name = EXCLUDED.station_name,
                    last_refreshed = EXCLUDED.last_refreshed
            """, rows)
        conn.commit()
    finally:
        conn.close()

    return len(rows)


if __name__ == "__main__":
    count = refresh_station_reference()
    logger.info("Refreshed %d station reference rows", count)