"""Poll Citi Bike GBFS station status and publish records to Redpanda.

Run as the producer service defined in ``docker-compose.yml``; its
``gbfs_station_status`` topic is consumed by
``spark_jobs/station_status_stream.py``.
"""

"""
The poller for gbfs station status data. It fetches the data from the Citi Bike GBFS endpoint and publishes it to a Kafka topic.
"""

import json
import time
import logging
import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
KAFKA_BROKER = "redpanda:9092"   # use "redpanda:9092" if this script runs inside docker-compose
TOPIC = "gbfs_station_status"
POLL_INTERVAL_SECONDS = 30


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def poll_once(producer: KafkaProducer) -> int:
    resp = requests.get(STATION_STATUS_URL, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    stations = payload["data"]["stations"]
    fetched_at = payload["last_updated"]

    for station in stations:
        record = {
            "station_id": station["station_id"],
            "num_bikes_available": station.get("num_bikes_available"),
            "num_docks_available": station.get("num_docks_available"),
            "is_renting": station.get("is_renting"),
            "last_reported": station.get("last_reported"),
            "fetched_at": fetched_at,
        }
        producer.send(TOPIC, key=record["station_id"], value=record)

    producer.flush()
    return len(stations)

def poll_once_with_retry(producer: KafkaProducer, max_retries: int = 3, backoff_seconds: int = 5) -> int:
    for attempt in range(1, max_retries + 1):
        try:
            return poll_once(producer)
        except Exception as e:
            logger.warning("Poll attempt %d/%d failed: %s", attempt, max_retries, e)
            if attempt == max_retries:
                logger.error("All retries exhausted for this poll cycle — skipping to next interval")
                return 0
            time.sleep(backoff_seconds * attempt)


def run():
    producer = make_producer()
    logger.info("GBFS producer started, polling every %s seconds", POLL_INTERVAL_SECONDS)
    while True:
        try:
            count = poll_once_with_retry(producer)
            logger.info("Published %d station records", count)
        except Exception as e:
            logger.error("Poll failed: %s", e)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()