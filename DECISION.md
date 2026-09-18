## for months with more than 1 million trips, trip data is split into multiple CSVs within the same compressed file.

<!-- We decided to Use Jersey City(JC) data along with the NYC dataset. by partitioned by the region while storing the silver bucket for quicker inference in paraquet(column storage form) -->

## Bug Flag/Flaw in bronze to silver (Extraction): <!-- region is inferred from filename substring rather than passed explicitly — trades interface simplicity for coupling to naming convention; a truly unknown region silently falls through to 'nyc' rather than erroring, which is a known limitation." That kind of self-aware trade-off note is exactly the habit we talked about earlier. -->

citibike_dbt: Building the dim_station:
<!-- Station coordinates can drift slightly due to GPS jitter. We need to pick one definitive coordinate per station. DuckDB's arg_max function is perfect here: it lets us select the name and coordinates from the most recent time a station was used. -->
<!-- arg_max() is not "adding the latest station name." It's choosing the station name (and coordinates) from the most recent observation for that station ID. -->


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