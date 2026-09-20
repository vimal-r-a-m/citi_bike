"""
## Observation Summary
A significant visual anomaly is present during the May 24-25 timeframe, characterized by an abrupt network-wide decline in daily trip volume from a stable baseline of approximately 180k trips down to ~50k trips.

## Root Cause Evaluation
While May 25 corresponds to Memorial Day—a holiday known to alter standard commuter patterns—a 70% system-wide drop is unusually steep for holiday behavior alone, which typically redistributes rather than erases ridership. This indicates two primary possibilities:

### Environmental Disruption: A severe weather event (such as torrential rainfall) that significantly suppressed regional travel demand.

###Data Artifact: A potential data pipeline failure, incomplete logging, or missing ingestion window during this period.
"""

from utils.connect_minio import get_minio_connection

con_gold = get_minio_connection("citibike_dbt/warehouse.duckdb")
con = get_minio_connection('citibike_dbt/warehouse.duckdb')

### Finding: Trip volume dips May 23–24 (2026), then partially recovers May 25 before returning 
# to normal weekday levels May 26 onward.

print("1. Check for Missing Hours:\n")
# If the data source was truncated or a file 
# -failed to download completely, you might be missing half a day's worth of records.
df = con_gold.execute("""
    PIVOT (
        SELECT 
            extract(hour from started_at) as hour,
            start_date_key,
            COUNT(*) as trip_count
        FROM fact_trips 
        WHERE start_date_key IN ('2026-05-23', '2026-05-24', '2026-05-25') 
        GROUP BY hour, start_date_key
    ) 
    ON start_date_key 
    USING sum(trip_count)
    ORDER BY hour;  
""")
print(df.fetchdf())

print("2. Check the Regional Split:\n")
# Since you ingested both NYC and JC (Jersey City) data, it is possible one of the CSV files 
# had a formatting error or a missing date range. NYC accounts for the vast majority of trips.

df = con_gold.execute("""
        SELECT 
            start_date_key, 
            region, 
            COUNT(*) as trip_count
        FROM fact_trips 
        WHERE start_date_key BETWEEN '2026-05-23' AND '2026-05-27' 
        GROUP BY start_date_key, region 
        ORDER BY start_date_key, region;    
""")
print(df.fetchdf())

print("3. Check if the dataset actually contains only the may month trip data only:\n")
con_silver = get_minio_connection()
df = con_silver.execute("""
        SELECT 
            region, 
            trip_month, 
            COUNT(*) AS trip_count
    FROM read_parquet('s3://silver/citibike/**/*.parquet', hive_partitioning=true, filename=true)
    GROUP BY region, trip_month
    ORDER BY trip_month, region;
""").fetchdf()
    
print(df)

# Checks if the fact_trips has all the month table exists in the Gold layer
print("--- GOLD LAYER (fact_trips) ---")
df_gold = con_gold.execute("""
    SELECT 
        region, 
        strftime(started_at, '%Y%m') AS year_month, 
        COUNT(*) AS trip_count 
    FROM fact_trips 
    GROUP BY region, year_month 
    ORDER BY year_month, region;
""").df()
print(df_gold)


## checking for overlap between the fact_trips and the station_rolling_availability table station_id's
con_gold.execute("INSTALL postgres; LOAD postgres;")
# Attach to the Postgres container (using port 5433 mapped to your host)
con_gold.execute("ATTACH 'dbname=bikeshare user=warehouse password=warehouse host=localhost port=5433' AS livedb (TYPE postgres);")
# sample a few IDs from each side, side by side
live_sample = con.execute("SELECT DISTINCT station_id FROM livedb.station_rolling_availability LIMIT 10").fetchdf()
batch_sample = con.execute("SELECT DISTINCT station_id FROM dim_station LIMIT 10").fetchdf()
print("LIVE:\n", live_sample)
print("BATCH:\n", batch_sample)

## conclusion: No overlap

import requests
info = requests.get("https://gbfs.citibikenyc.com/gbfs/en/station_information.json").json()
short_names = {s["short_name"] for s in info["data"]["stations"]}

batch_ids = con_gold.execute("SELECT DISTINCT station_id FROM dim_station").fetchdf()["station_id"].tolist()

overlap = set(batch_ids) & short_names
print(f"{len(overlap)} of {len(batch_ids)} batch station_ids found in live short_name")



