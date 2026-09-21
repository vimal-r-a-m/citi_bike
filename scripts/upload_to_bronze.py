"""Upload extracted trip CSV files to the MinIO bronze bucket.

Used as a local/manual upload utility; the scheduled batch DAG uses
``upload_to_minio.py`` instead.
"""

# The raw data of compressed file has been uploaded to the bronze layer already, by manually on CLI
import boto3
from pathlib import Path

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# Create buckets if they don't exist
for bucket in ['bronze', 'silver']:
    try:
        s3.create_bucket(Bucket=bucket)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

# Map your local files to their S3 keys
# Adjust the local paths based on where your CSVs are stored
uploads = {
    "data/extracted/JC-202605-citibike-tripdata.csv/JC-202605-citibike-tripdata.csv": "2026/05/JC-202605-citibike-tripdata",
    "data/extracted/202605-citibike-tripdata/202605-citibike-tripdata_1.csv": "2026/05/202605-citibike-tripdata_1",
    "data/extracted/202605-citibike-tripdata/202605-citibike-tripdata_2.csv": "2026/05/202605-citibike-tripdata_2",
    "data/extracted/202605-citibike-tripdata/202605-citibike-tripdata_3.csv": "2026/05/202605-citibike-tripdata_3",
    "data/extracted/202605-citibike-tripdata/202605-citibike-tripdata_4.csv": "2026/05/202605-citibike-tripdata_4",
    "data/extracted/202605-citibike-tripdata/202605-citibike-tripdata_5.csv": "2026/05/202605-citibike-tripdata_5"
# Add the rest of your NYC files here...
}

for local_file, s3_key in uploads.items():
    if Path(local_file).exists():
        print(f"Uploading {local_file} to s3://bronze/citibike_extracted/{s3_key}")
        s3.upload_file(local_file, 'bronze', f"citibike_extracted/{s3_key}")
    else:
        print(f"File {local_file} not found locally, skipping.")