## for months with more than 1 million trips, trip data is split into multiple CSVs within the same compressed file.

<!-- We decided to Use Jersey City(JC) data along with the NYC dataset. by partitioned by the region while storing the silver bucket for quicker inference in paraquet(column storage form) -->

## Bug Flag/Flaw in bronze to silver (Extraction): <!-- region is inferred from filename substring rather than passed explicitly — trades interface simplicity for coupling to naming convention; a truly unknown region silently falls through to 'nyc' rather than erroring, which is a known limitation." That kind of self-aware trade-off note is exactly the habit we talked about earlier. -->

citibike_dbt: Building the dim_station:
<!-- Station coordinates can drift slightly due to GPS jitter. We need to pick one definitive coordinate per station. DuckDB's arg_max function is perfect here: it lets us select the name and coordinates from the most recent time a station was used. -->
<!-- arg_max() is not "adding the latest station name." It's choosing the station name (and coordinates) from the most recent observation for that station ID. -->

we kept the dim_date despite it doesn't store any info from the citi_bike dataset
<!-- decouples date-derived attributes (weekday, holiday, month) from fact rows, enabling clean day-of-week/seasonality analysis without recomputing calendar logic per query — this is what powered the Memorial Day ridership investigation -->
#Imp logic mistake, we didn't how the silver need to be loaded, like the date wise
i.e in the silver bucket the data parquest is not loaded accroding to the date.

## Finding: Trip volume dips May 23–24 (2026), then partially recovers May 25 before returning to normal weekday levels May 26 onward.
<!-- Root cause: Day-of-week effect, not a data quality issue. May 23–24 are Sat/Sun; May 25 is Memorial Day (US holiday). This is a commuter-dominated system (81.5% member trips), so weekend/holiday ridership is structurally lower than weekdays. Hour-of-day distribution on May 25 shows an afternoon/evening-skewed pattern (leisure riding) rather than the AM/PM double-peak seen on workdays, further supporting a holiday explanation over a pipeline defect.

Ruled out: Initially hypothesized this was linked to Citi Bike's May 29, 2026 per-minute rate increase for Jersey City/Hoboken. Rejected — the price change postdates the dip by several days and only applies to JC riders, while the NYC region (the majority of trip volume) shows the same drop independently. Timing and geography both rule this out.

Follow-up (optional): worth checking JC-only trip counts for May 29–31 specifically, since that's the actual window the rate hike could plausibly affect — separate from this dip.
Observation on follow-up: There is not an unusual change on the trips per day, as the drop in the numbers are pretty common on weekends. -->

Docker Compose: <!--this is a separate Postgres instance for Airflow's own metadata, distinct from any warehouse Postgres you add later-->


while re running the mannual the may month data, july month data was also found.
i.e monthly data leaks across the boundary.
  region  year_month  trip_count
0     jc      202604          18
1    nyc      202604         512
2     jc      202605       95332
3    nyc      202605     4694366

this caused the data of the prev month to get clobbered

--- GOLD LAYER (fact_trips) ---
  region year_month  trip_count
0     jc     202604          18
1    nyc     202604         512
2     jc     202605           9
3    nyc     202605         635
4     jc     202606          10
5    nyc     202606         869
6     jc     202607      109085
7    nyc     202607     4992268


Incident: Partitioned Silver writes used year_month derived from started_at as the partition key. Citi Bike's monthly source files contain a small number of boundary-crossing trips (e.g. a few hundred rows in June's file with started_at in May). Because DuckDB's PARTITION_BY + OVERWRITE_OR_IGNORE replaces the entire partition file rather than merging, processing June's file overwrote May's full partition with just the leaked rows — silent data loss, only caught by manually inspecting row counts.
Fix: Separated trip_month (derived from started_at, used for calendar-accurate analytics) from source_month (the ingestion run's target month, passed explicitly, used only for partitioning). Each run's write is now scoped to its own partition regardless of any boundary-leak rows it contains.
Lesson: a derived/business-meaning column and a write-partitioning key look interchangeable but serve different purposes — collapsing them into one field creates a silent overwrite hazard whenever upstream data isn't perfectly bounded by the field you're partitioning on.

knowledge: Always validate the assumptions about the data

ADR: Silver Layer Partition Strategy for Handling Boundary Date Leaks

Context
When ingesting append-only or monthly source files (e.g., Citi Bike CSVs), files frequently contain boundary rows—a small number of rides from late-May present inside a June dataset due to timezone offsets or system lag.

Our current Silver layer pipeline uses a naive partition strategy based on the event timestamp (year_month = strftime(started_at, '%Y%m')). When executing an overwrite (OVERWRITE_OR_IGNORE = true), DuckDB evaluates these boundary rows, determines they belong to a different month, and overwrites the entire target partition folder. Because the incoming file only contains a fraction of the boundary data, this results in catastrophic data loss, wiping out millions of legitimate historical records in adjacent partitions.

Options Considered
Option A: Strict Boundary Filter (Easiest)
Keep the partition logic tied to started_at, but introduce an explicit filter before writing to drop out-of-month rows.

Implementation: WHERE strftime(started_at, '%Y%m'] = '202606'

Pros: Simple to implement; keeps physical directory structures strictly aligned with calendar months.

Cons: Results in data loss at the source level, permanently discarding legitimate boundary rides.

Option B: Decouple Ingestion Partition from Analytical Date (Most Robust)
Partition the Silver layer strictly by the file ingestion batch or source month (source_month='202606'), while relying on the raw timestamp (started_at) downstream in the Gold layer for analytics.

Implementation: Partition Silver files by the nominal month of the file being processed. Boundary rows are safely isolated inside their originating source folder without affecting other partitions.

Pros: Zero data loss; fully preserves raw ingestion fidelity; downstream Gold models (fact_trips) can still aggregate accurately using started_at.

Cons: Adds a minor abstraction layer where physical folder structure differs slightly from event timestamp structure.

After implementig the robust option using two month_yr col
    1) source_date[for silver bucket/transaction]
    2) trip_date[for analytical process on warehouse.duckdb]

before is in the screenshot.
after implenting the solun.
--- GOLD LAYER (fact_trips) ---
  region year_month  trip_count
0     jc     202604          18
1    nyc     202604         512
2     jc     202605       95341
3    nyc     202605     4695001
4     jc     202606      109898
5    nyc     202606     5384702
6     jc     202607      109085
7    nyc     202607     4992268

Validated idempotency by re-running the full DAG for June (already-processed month) — row counts remained identical, confirming CREATE OR REPLACE + OVERWRITE_OR_IGNORE prevent duplication on rerun. Validated the full download→extract→upload→clean→dbt chain end-to-end, unattended, on July (first-time month) — first clean run since the RAM/partitioning fixes.

Confirmed fix: reprocessed May→June→July from Bronze after separating trip_month (calendar-accurate, from started_at) from source_month (ingestion-run partition key). All three months now show correct full-scale row counts (nyc: 4.69M/5.38M/4.99M; jc: 95K/110K/109K) with no cross-month overwrite. April's boundary-leak rows (18 jc / 512 nyc) persist as a known, harmless artifact of Citi Bike's file boundaries — real trips misfiled by their own export process, correctly retained under their true trip_month rather than discarded or duplicated.

Batch is now genuinely solid: proven idempotent, proven on a fresh month, and this partition bug is fixed and documented with a clear before/after story

todo: rn in the warhouse.duckdb, when every time dbt run is executed table are rematerialzed from scratch based on whatever is in silver bucker parquet files
    work: make it incremental materialization for the dbt run.
todo: the connect dag py for clean_trips. make sure later to refactor utils connect_minio it for inter-docker connection rather than localhost only.

todo: create an .env file for global varables like crediatials, file/bucker location. and connect ti with docker compose

todo: docker compose container for airflow webserver is starting very late, we have to reticy it later.

todo: chnage the warehouse.duckdb into posgres db

todo: in the dag, we have make the inpurt minth update dynamically, and check and understand how it pull for each month start and does it change the monthe it pulls automatically?

todo: add the station info pull to airflow.

todo: modify the streamlit dashboard aesthetically and use html or css for dashboard.

todo: how would the stream data change for the truck redistrubte at night?


redpanda: <!--Set a 48h retention window on the GBFS topic — real-time availability data has no long-term analytical value beyond feeding rolling aggregates, so unbounded retention would just accumulate disk cost.-->

GBFS station_id values can be large numeric-looking strings (observed up to 19 digits) or UUIDs (seen in the same feed — e.g. 5f798c51-5d8b-...). Both must be treated strictly as VARCHAR/string everywhere in the pipeline — Spark schema, Postgres sink, and any join back to the batch dim_station table. Numeric types would silently truncate precision on the larger IDs (confirmed: JS-based tooling already demonstrated this exact failure in Redpanda Console's display).

so we made this:
<!-- Note: station_id stays StringType throughout — the schema explicitly protects against the exact ID-truncation issue we just caught. Checkpointing is included here but only functionally, not yet hardened (no persistent volume for it, no restart-recovery test) — that's deliberately deferred to the hardening pass, consistent with plumbing-first discipline. -->

<!-- All pipeline timestamps (batch and streaming) are stored in UTC internally; local-timezone conversion (IST) happens only at the presentation/dashboard layer. This avoids ambiguity if the project is viewed by someone outside IST, and prevents silent misalignment if batch and streaming timestamps are ever joined or compared directly. -->

for the batch leg also, the timing is it utc

Note: we traded Spark's built-in JDBC writer (fast, but append-only) for .collect() + psycopg2, which pulls the micro-batch to the driver. That's fine at this data volume (a few thousand stations per 5-minute window) but wouldn't scale to a much larger fleet without partitioning the write — worth a one-line note if you want to preempt that question in an interview.
to avoid duplicate formation.

**Trade-off: at-least-once semantics, bounded by a 10-minute watermark**

The streaming leg uses at-least-once delivery with idempotent upserts, not true exactly-once. 
Concretely: Spark may emit multiple intermediate updates for the same (station_id, window_start) 
as a window fills in — this was directly observed (station 2123940513700131194 appeared twice for 
the same window before hardening). The upsert on (station_id, window_start) makes repeated writes 
safe, and the 10-minute watermark bounds how long a window stays open to late data before Spark 
considers it final and stops updating it.

This does NOT guarantee exactly-once processing in the strict sense (e.g. a Spark failure between 
computing and committing a batch could still cause a reprocessed batch to overwrite a window with 
a still-accurate but recomputed value) — a true exactly-once guarantee would need a transactional 
outbox pattern coordinating Kafka offset commits with the Postgres write in a single transaction, 
which wasn't implemented here. For this use case (rolling bike availability), staleness/duplication 
within a 10-minute window is an acceptable trade-off, since the data is inherently approximate and 
re-polled every 30 seconds regardless.


# DECISIONS.md

## Data Cleaning
- Station IDs (start/end) cast to VARCHAR, never numeric — they're identifiers, not 
  quantities, and GBFS/trip data include both long numeric strings (up to 19 digits) 
  and UUID-format IDs. Numeric typing would silently truncate/corrupt precision 
  (confirmed: Redpanda Console's JS-based UI demonstrated this exact rounding failure 
  on 19-digit IDs before the VARCHAR fix).
- Region (jc/nyc) inferred from filename substring rather than passed as an explicit 
  parameter — simpler call site, but couples logic to Citi Bike's naming convention; 
  an unrecognized filename silently defaults to 'nyc' rather than erroring. Known 
  limitation, not fixed.
- Trips with ended_at <= started_at are dropped as malformed (non-positive duration).
- Dedup on ride_id keeps the earliest started_at per duplicate.

## Partitioning (Batch)
- Silver writes originally partitioned by `year_month` derived from `started_at`. 
  Citi Bike's monthly files contain boundary-crossing trips (e.g. June's file 
  includes ~few hundred May-dated rows). Because DuckDB's PARTITION_BY + 
  OVERWRITE_OR_IGNORE replaces the entire partition file rather than merging, 
  processing June overwrote May's full partition with just the leaked rows — 
  silent data loss, caught by manually inspecting row counts.
- Fix: split into `trip_month` (derived from started_at, used for calendar-accurate 
  analytics/joins) and `source_month` (explicitly passed per ingestion run, used only 
  for partitioning). Each run's write is now scoped to its own partition regardless 
  of boundary-leak rows.
- April's boundary-leak rows (18 jc / 512 nyc) are retained as a known, harmless 
  artifact — real trips misfiled by Citi Bike's own export process.

## Orchestration
- Airflow uses a separate Postgres instance for its own metadata, distinct from the 
  warehouse Postgres used for streaming output.
- DAG uses explicit `year_month` params rather than execution-date templating — 
  simpler to trigger manually, less error-prone.
- Validated idempotency: re-ran the full DAG for an already-processed month (June); 
  row counts remained identical, confirming CREATE OR REPLACE + OVERWRITE_OR_IGNORE 
  prevent duplication on rerun.
- Validated full-chain automation: ran July (first-time month) end-to-end, unattended, 
  through all four tasks after the above fixes.
- Incident: hit RAM pressure during automated runs; one task's Airflow retry was 
  mistakenly marked-success manually rather than actually re-executed. Resolved by 
  manually verifying actual output correctness and clearing the task instance to keep 
  DAG history accurate. Root cause: DuckDB had no memory ceiling and competed uncapped 
  with other containers. Fix: explicit `SET memory_limit='2GB'` with disk spill enabled.

## Modeling
- dim_date exists to decouple date-derived attributes (weekday, is_weekend, month) 
  from fact rows — not to store trip data itself. This is what powered the Memorial 
  Day ridership investigation (see below).
- Station lat/lng in dim_station: [fill in your first-seen vs. most-frequent choice here]

## Analytical Finding
- Investigated a ridership dip May 23-25, 2026. Initially hypothesized a Citi Bike 
  price change (May 29 NJ rate hike) — ruled out: the price change postdates the dip 
  by several days and only applies to JC riders, while NYC (the majority of volume) 
  showed the same drop independently. Actual cause: day-of-week/holiday effect — 
  May 23-24 are Sat/Sun, May 25 is Memorial Day, and this is a commuter-dominated 
  system (81.5% member trips). Hourly distribution on the 25th showed an 
  afternoon/evening-skewed leisure-riding pattern rather than the AM/PM commute 
  double-peak, further supporting the holiday explanation.

## Timestamps
- All pipeline timestamps (batch and streaming) stored in UTC internally; local 
  (IST) conversion happens only at the presentation/dashboard layer. Avoids ambiguity 
  for non-IST viewers and prevents silent misalignment if batch and streaming 
  timestamps are ever joined directly.
- [Verify and note here: are batch started_at/ended_at UTC or naive-local in the 
  source CSVs? Confirm before any batch/streaming timestamp join.]

## Streaming
- GBFS topic retention set to 48h — real-time availability data has no long-term 
  analytical value beyond feeding rolling aggregates; unbounded retention would 
  accumulate disk cost for no benefit.
- Messages keyed by station_id so all updates for one station land on the same 
  partition, in order — required for correct per-station aggregation.
- Incident (self-diagnosed): station 2123940513700131194 appeared twice for the same 
  5-minute window. Cause: foreachBatch + JDBC append mode wrote every micro-batch's 
  intermediate state for still-open windows.
- Fix: (1) idempotent upsert via ON CONFLICT (station_id, window_start) — makes 
  repeated writes to an in-progress window safe; (2) 10-minute watermark on event_time 
  — bounds when a window is considered closed. Both were required together: 
  watermarking alone doesn't prevent duplicate writes before the cutoff; upserting 
  alone doesn't decide when a window's data is final.
- Verified checkpoint-restart recovery by killing the Spark container mid-run and 
  confirming it resumed from its last offset rather than reprocessing or losing data.
- Added retry-with-backoff to the GBFS producer poll loop; exhausted retries log and 
  skip to the next 30s cycle rather than crashing the process.
- Trade-off: at-least-once semantics, not exactly-once. A Spark failure between 
  computing and committing a batch could still cause a reprocessed batch to overwrite 
  a window with a recomputed (still accurate) value. True exactly-once would need a 
  transactional outbox pattern coordinating Kafka offset commits with the Postgres 
  write — not implemented, since staleness/duplication within a 10-minute window is 
  acceptable for inherently approximate, 30s-repolled availability data.


## ID Scheme Mismatch — Batch vs. Streaming (RESOLVED)
- Initial join attempt on station_id directly: 0 of 2,520 live IDs matched dim_station.
- Root cause: live GBFS station_status.json uses an internal UUID/long-numeric 
  station_id that differs from the historical short-code scheme (e.g. HB407, JC094, 
  6459.07) used in trip CSVs.
- Bridge found: GBFS's station_information.json exposes a `short_name` field matching 
  the historical short-code format. This feed wasn't originally part of the streaming 
  pipeline (only station_status.json was consumed) — added as a low-frequency 
  reference table (refreshed daily, not every 30s, since station metadata changes 
  rarely) joining gbfs_station_id -> short_name -> dim_station.station_id.
- [Fill in: exact overlap % once measured — e.g. "1,847 of 2,520 live stations matched; 
  remaining gap likely reflects stations added after historical data's date range, or 
  historical stations since decommissioned."]

  2408 of 2523 batch station_ids found in live short_name (95.4% match)

## ID Scheme Bridge — Resolution

- Bridge confirmed: GBFS station_information.json's short_name field matches historical batch station_id format (e.g. HB407, JC094, 6459.07).
- Match rate: 2,408 of 2,523 batch station_ids (95.4%) found in current live short_name values. Remaining ~4.6% gap attributed to station churn over time (stations opened after historical data's range, or since decommissioned) — not investigated further as within expected/acceptable bounds for a bridge across two live systems observed at different points in time.
- Implementation: station_reference table in warehouse Postgres, refreshed daily via a dedicated Airflow DAG (station metadata changes rarely, so daily cadence is deliberately loose, not tied to the 30s streaming cadence). Dashboard joins live availability -> station_reference -> dim_station at query time (not in the Spark stream itself), since the mapping is near-static and doesn't need streaming-level freshness.

Note: We pull the station reference info, and store it in the warehouse_posgresql container itself.

in live display of data on dashboard
live = con.execute("""
    SELECT COALESCE(d.station_name, sr.station_name) AS display_name,
           sr.short_name AS station_id,
           l.avg_bikes_available,
           l.window_start
    FROM livedb.station_rolling_availability l
    JOIN livedb.station_reference sr ON l.station_id = sr.gbfs_station_id
    LEFT JOIN dim_station d ON sr.short_name = d.station_id
    WHERE l.window_start = (SELECT MAX(window_start) FROM livedb.station_rolling_availability)
    ORDER BY l.avg_bikes_available ASC
    LIMIT 10""")
# Note the JOIN on station_reference (inner — you need the bridge to exist) but LEFT JOIN on 
# dim_station (outer — a station can be live-active and bridgeable even if it's not in your
# historical batch data, e.g. newly opened stations).


## ID Scheme Bridge — Resolution
- Bridge confirmed: GBFS station_information.json's short_name field matches 
  historical batch station_id format (e.g. HB407, JC094, 6459.07).
- Match rate: 2,408 of 2,523 batch station_ids (95.4%) found in current live 
  short_name values. Remaining ~4.6% gap attributed to station churn over time 
  (stations opened after historical data's range, or since decommissioned) — 
  not investigated further as within expected/acceptable bounds for a bridge 
  across two live systems observed at different points in time.
- Implementation: station_reference table in warehouse Postgres, refreshed daily 
  via a dedicated Airflow DAG (station metadata changes rarely, so daily cadence 
  is deliberately loose, not tied to the 30s streaming cadence). Dashboard joins 
  live availability -> station_reference -> dim_station at query time (not in 
  the Spark stream itself), since the mapping is near-static and doesn't need 
  streaming-level freshness.


  ## Analytical Finding: Live Empty-Station Count
- Live dashboard surfaced N stations with avg_bikes_available = 0 in the most recent 
  5-minute window (captured [timestamp], NYC local time [X]).
- [If off-peak: consistent with expected overnight bike clustering ahead of morning 
  rebalancing. / If peak: cross-referenced against historical busiest-stations panel — 
  overlap found: <yes/no, which stations>.]
- This directly answers the original project goal stated in the initial project scope 
  ("stations that run empty most often") — but as a live, real-time signal rather 
  than a historical aggregate, demonstrating the value of the streaming leg beyond 
  just architectural completeness.


  ## Analytical Finding: Live Empty-Station Count
- Live dashboard surfaced 63 of 2,523 stations (2.5%) at avg_bikes_available = 0, 
  captured 11:20 AM NYC local time on a weekday.
- Not explained by overnight clustering (time of day rules this out).
- [Fill in once checked: whether this matches a genuine late-morning demand pattern 
  in historical trip data, and whether the empty stations are geographically 
  concentrated or scattered.]
- Directly answers the original project's stated goal ("stations that run empty most 
  often") as a live signal — this is the concrete payoff of the streaming leg beyond 
  architectural completeness: a real, current operational fact the batch-only version 
  of this project could never surface.

the 