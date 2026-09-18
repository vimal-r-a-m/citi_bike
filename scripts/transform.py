import duckdb
from utils.connect_minio import get_minio_connection


def inspect_schema(con, source_glob: str):
    print(f"--- Schema for {source_glob} ---")
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source_glob}')").fetchdf()
    print(df[['column_name', 'column_type']])


def clean_trips(con, source_glob: str, source_month: str) -> None:
    # Note: hive_partitioning=true pulls the 'region' column from the folder structure
    con.execute(f"""
        CREATE OR REPLACE TABLE trips_silver AS
        SELECT
            ride_id,
            rideable_type,
            started_at,
            ended_at,
            start_station_name,
            CAST(start_station_id AS VARCHAR) AS start_station_id,
            end_station_name,
            CAST(end_station_id AS VARCHAR) AS end_station_id,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            member_casual,
            CASE WHEN filename LIKE '%JC%' THEN 'jc' ELSE 'nyc' END AS region,
            strftime(started_at, '%Y%m') AS trip_month,   -- real calendar month, for analytics
            '{source_month}' AS source_month              -- which ingestion run owns this write
        FROM read_csv_auto(
            '{source_glob}',
            union_by_name=true,
            filename=true,
            types={{'start_station_id': 'VARCHAR', 'end_station_id': 'VARCHAR'}}
        )
        WHERE started_at IS NOT NULL
        AND ended_at IS NOT NULL
        AND ended_at > started_at
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) = 1
    """)    

def write_silver(con):
    con.execute("""
        COPY trips_silver TO 's3://silver/citibike/'
        (FORMAT PARQUET, PARTITION_BY (region, source_month), OVERWRITE_OR_IGNORE true)
    """)

if __name__ == "__main__":
    con = get_minio_connection()
    # Step B: Inspect to ensure JC and NYC match
    # inspect_schema(con, 's3://bronze/citibike_extracted/2026/05/**')
    
    # Step D & F: Clean and Write
    print("Cleaning trips...")
    clean_trips(con, 's3://bronze/citibike_extracted/2026/05/**', "202605")
    print("Writing to Silver...")
    write_silver(con)
    print("Done.")