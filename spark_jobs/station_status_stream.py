"""Aggregate live GBFS station-status events and upsert rolling metrics.

Run as the Spark streaming job from ``docker-compose.yml``. It consumes the
``gbfs_station_status`` Redpanda topic produced by
``scripts/gbfs_producer.py`` and writes to PostgreSQL.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, LongType
import psycopg2
from psycopg2.extras import execute_values

PG_CONN_PARAMS = dict(
    host="warehouse_postgres", port=5432,
    dbname="bikeshare", user="warehouse", password="warehouse"
)
spark = SparkSession.builder.appName("GBFSStationStatusStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("station_id", StringType()) \
    .add("num_bikes_available", IntegerType()) \
    .add("num_docks_available", IntegerType()) \
    .add("is_renting", IntegerType()) \
    .add("last_reported", LongType()) \
    .add("fetched_at", LongType())

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "gbfs_station_status") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("data"))
             .select("data.*")
             .withColumn("event_time", to_timestamp(col("last_reported"))))

parsed_with_watermark = parsed.withWatermark("event_time", "10 minutes")

rolling = parsed_with_watermark.groupBy(
    window(col("event_time"), "5 minutes"),
    col("station_id")
).agg(avg("num_bikes_available").alias("avg_bikes_available"))


"""
this trades Spark's built-in JDBC writer (fast, but append-only) for .collect() + psycopg2,
which pulls the micro-batch to the driver. That's fine at this data volume
(a few thousand stations per 5-minute window) but wouldn't scale to a much larger fleet
without partitioning the write
"""
def write_to_postgres(batch_df, batch_id):
    rows = (batch_df
            .selectExpr("station_id", "window.start as window_start",
                        "window.end as window_end", "avg_bikes_available")
            .collect())

    if not rows:
        return

    values = [(r.station_id, r.window_start, r.window_end, r.avg_bikes_available) for r in rows]

    conn = psycopg2.connect(**PG_CONN_PARAMS)
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO station_rolling_availability
                    (station_id, window_start, window_end, avg_bikes_available)
                VALUES %s
                ON CONFLICT (station_id, window_start)
                DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    avg_bikes_available = EXCLUDED.avg_bikes_available
            """, values)
        conn.commit()
    finally:
        conn.close()


query = (rolling.writeStream
         .outputMode("update")
         .foreachBatch(write_to_postgres)
         .option("checkpointLocation", "/tmp/spark_checkpoints/station_status")
         .start())

query.awaitTermination()