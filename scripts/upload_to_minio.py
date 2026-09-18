import boto3
import os
import logging
from utils.connect_s3 import _get_s3_client

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "bronze"


def upload_extracted_csvs(extracted: dict, year_month: str) -> list:
    """extracted: {region: [local_csv_paths]} from download_and_extract. Returns list of uploaded S3 keys."""
    s3 = _get_s3_client()
    year, month = year_month[:4], year_month[4:6]
    uploaded_keys = []

    for region, paths in extracted.items():
        for local_path in paths:
            filename = os.path.basename(local_path)
            key = f"citibike_extracted/{year}/{month}/{filename}"
            logger.info(f"Uploading {local_path} → s3://{BRONZE_BUCKET}/{key}")
            s3.upload_file(local_path, BRONZE_BUCKET, key)
            uploaded_keys.append(key)

    return uploaded_keys