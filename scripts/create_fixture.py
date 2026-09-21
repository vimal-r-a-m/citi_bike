import os
import duckdb
# run python -m scripts.create_fixture
# Create test fixture directory
os.makedirs("citibike_dbt/test_fixtures", exist_ok=True)

# Temporary DuckDB — warehouse.duckdb is NOT needed
con = duckdb.connect(":memory:")

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Configure DuckDB to talk to MinIO
con.execute("""
    SET s3_endpoint = 'localhost:9000';
    SET s3_access_key_id = 'minioadmin';
    SET s3_secret_access_key = 'minioadmin';
    SET s3_use_ssl = false;
    SET s3_url_style = 'path';
""")

# Read rows from MinIO and create a local test fixture using a clean subquery + WHERE
con.execute("""
    COPY (
        SELECT * EXCLUDE (rn) 
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY region, source_month) AS rn
            FROM read_parquet('s3://silver/citibike/**/*.parquet', hive_partitioning=true)
        ) 
        WHERE rn <= 150
    ) TO 'citibike_dbt/test_fixtures/sample_trips.parquet' (FORMAT PARQUET);
""")

print("Fixture created successfully!")

# Verify data BEFORE closing the connection
check = con.execute("""
    SELECT region, source_month, COUNT(*) 
    FROM read_parquet('citibike_dbt/test_fixtures/sample_trips.parquet')
    GROUP BY region, source_month 
    ORDER BY region, source_month
""").fetchdf()

print(check)

con.close()