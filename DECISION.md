# DECISIONS.md

## Data Cleaning (Bronze → Silver)
- Station IDs (start/end) cast to VARCHAR, never numeric — identifiers, not 
  quantities. GBFS/trip data include long numeric strings (up to 19 digits) and 
  UUID-format IDs; numeric typing would silently truncate/corrupt precision 
  (confirmed: Redpanda Console's JS-based UI demonstrated this exact rounding 
  failure on 19-digit IDs before the VARCHAR fix was applied).
- Region (jc/nyc) inferred from filename substring (`CASE WHEN filename LIKE '%JC%'`) 
  rather than passed as an explicit parameter — simpler call site, but couples logic 
  to Citi Bike's naming convention. An unrecognized filename silently defaults to 
  'nyc' rather than erroring. Known limitation, not fixed.
- Trips with `ended_at <= started_at` dropped as malformed (non-positive duration).
- Dedup on `ride_id` keeps the earliest `started_at` per duplicate.

## Partitioning Bug (Silver writes)
- Original scheme partitioned Silver by `year_month` derived from `started_at`. 
  Citi Bike's monthly files contain boundary-crossing trips (e.g. June's file 
  includes ~few hundred May-dated rows). Because DuckDB's `PARTITION_BY` + 
  `OVERWRITE_OR_IGNORE` replaces the entire partition file rather than merging, 
  processing June overwrote May's full partition with just the leaked rows — 
  silent data loss, caught by manually inspecting row counts after the fact.
- Fix: split into `trip_month` (derived from `started_at`, real calendar month, 
  used for analytics/joins/dim_date) and `source_month` (explicitly passed per 
  ingestion run, used only for partitioning — `PARTITION_BY (region, source_month)`). 
  Each run's write is now scoped to its own partition regardless of boundary-leak rows.
- April's boundary-leak rows (18 jc / 512 nyc) retained under `trip_month=202604` 
  as a known, harmless artifact — real trips misfiled by Citi Bike's own export 
  process, not a pipeline defect.
- Validated fix: reprocessed May→June→July from Bronze after the split; all three 
  months showed correct full-scale counts (nyc: 4.69M/5.38M/4.99M; jc: 95K/110K/109K) 
  with no cross-month overwrite.

## Orchestration (Airflow)
- Airflow's own metadata Postgres is a separate instance from the warehouse Postgres 
  used for streaming output — not to be conflated.
- DAG uses explicit `year_month` params rather than execution-date templating — 
  simpler to trigger manually, less error-prone.
- Validated idempotency: re-ran the full DAG for an already-processed month (June); 
  row counts remained identical, confirming `CREATE OR REPLACE` + 
  `OVERWRITE_OR_IGNORE` prevent duplication on rerun.
- Validated full-chain automation: ran July (first-time month) end-to-end, 
  unattended, through all four tasks after the fixes above.
- **Incident**: hit RAM pressure during automated runs on `clean_to_silver` and 
  `run_dbt_gold`. One task's Airflow retry was mistakenly marked-success manually 
  rather than actually re-executed. Resolved by manually verifying actual output 
  correctness (`dbt run`/`dbt test` run by hand) and clearing the falsely-marked 
  task instance to keep DAG history accurate rather than leaving a false success 
  recorded. Root cause: DuckDB had no memory ceiling and competed uncapped with 
  other containers for host RAM. Fix: explicit `SET memory_limit='2GB'` with disk 
  spill (`temp_directory`) enabled — makes behavior under memory pressure 
  predictable (spill-to-disk) instead of erratic (retry-and-hope).
- **Lesson**: a task marked "success" in Airflow doesn't verify anything actually 
  ran — always distinguish "the operator says it worked" from "I independently 
  confirmed the output is correct," especially after manual UI intervention.

## Modeling (Gold layer)
- `dim_date` exists to decouple date-derived attributes (weekday, is_weekend, 
  month) from fact rows — not to store trip data itself. Standard star-schema 
  dimension pattern. This is what powered the Memorial Day ridership investigation 
  (below). Implemented as a **view**, not a table: generated as a wide static 
  spine (2020–2035) since it has no growing source to check incrementally against — 
  a generated calendar isn't "new data arriving," so dbt's incremental pattern 
  doesn't apply here. Computing a 15-year daily spine is cheap enough that physical 
  storage provides no benefit.
- `dim_station` kept as full-refresh, not incremental: small table (~2,500 rows), 
  full rebuild cost is negligible. More importantly, a naive incremental 
  "append new rows only" pattern would silently miss updates to existing stations' 
  attributes (renames, corrected coordinates) — correctly handling that requires a 
  slowly changing dimension (SCD Type 1/2) pattern, real added design complexity 
  not justified by this table's size. Noted as a "with more time" item, not 
  implemented.
- `dim_station` uses ARG_MAX (latest known state per station_id, keyed on most 
  recent trip timestamp seen) rather than first-seen. Confirmed in practice: no 
  meaningful GPS jitter was observed across the dataset, so this choice has no 
  material effect on coordinate accuracy here — ARG_MAX was still the deliberate 
  choice on the reasoning that the *most current* known station location/name is 
  the more defensible default in principle, even though it didn't end up mattering 
  empirically.
- `fact_trips` made **incremental** (`materialized='incremental'`, `unique_key='ride_id'`, 
  filtered on `trip_month > MAX(trip_month) in {{ this }}`). Trade-off: reprocessing 
  an already-loaded month (e.g. for a future idempotency-style re-test) won't 
  naturally overwrite via the incremental filter — would require `--full-refresh`, 
  which defeats the incremental performance benefit for that run. Verified locally: 
  `dbt run --select fact_trips` processes only new `trip_month` values after initial 
  full build, not a full rebuild.

## Architecture: Engine Choices (explicitly considered and decided against switching)
- **Batch cleaning stayed on DuckDB, not PySpark**: for this data volume 
  (~1.2GB/month), DuckDB was empirically sufficient (proven across May/June/July 
  runs) and simpler. Switching would trade real simplicity for demonstrating 
  distributed processing patterns consistently across both legs — not for an 
  actual performance need. Explicitly declined late in the project to avoid 
  reopening a stable, already-hardened part of the pipeline for marginal 
  incremental signal, given Spark competency is already demonstrated by the 
  streaming leg.
- **Gold layer stayed on DuckDB, not Postgres**: DuckDB's `ATTACH ... TYPE postgres` 
  extension already enables cross-engine querying (used directly in the combined 
  batch+streaming dashboard), which is arguably a more interesting demonstrated 
  capability than "everything lives in one database." Declined for the same 
  reason as above — real migration cost (new staging-load step, full dbt profile 
  and model retest) for a story already told adequately by the existing 
  architecture.
- **Why Spark for streaming but not batch (not an inconsistency)**: DuckDB has no 
  streaming engine — no `readStream`/`writeStream`, no windowed aggregation over 
  continuous data, no watermarking, no checkpoint-based fault recovery. For 
  streaming, DuckDB was never a viable candidate; Spark Structured Streaming was 
  a structural necessity, not a preference. For batch, both engines are fully 
  capable of the same task, so the "simplest tool that works" principle applied 
  and DuckDB was kept.

## Analytical Finding: Memorial Day Dip (Batch)
- Investigated a ridership dip May 23–25, 2026. Initially hypothesized a Citi 
  Bike price change (May 29 NJ rate hike) — **ruled out**: the price change 
  postdates the dip by several days and only applies to JC riders, while NYC 
  (majority of volume) showed the same drop independently.
- Actual cause: day-of-week/holiday effect. May 23–24 are Sat/Sun, May 25 is 
  Memorial Day; this is a commuter-dominated system (81.5% member trips). Hourly 
  distribution on the 25th showed an afternoon/evening-skewed leisure-riding 
  pattern rather than the AM/PM commute double-peak, further supporting the 
  holiday explanation over a data or pipeline defect.

## Timestamps
- All pipeline timestamps (batch and streaming) stored in UTC internally; local 
  (IST) conversion happens only at the presentation/dashboard layer. Avoids 
  ambiguity for non-IST viewers and prevents silent misalignment if batch and 
  streaming timestamps are ever joined directly.
- **Known inconsistency, not resolved**: batch timestamps (`started_at`/`ended_at` 
  from Citi Bike CSVs) are naive local NYC time (Eastern), not UTC — confirmed. 
  Streaming timestamps (`event_time` in the Spark job, from GBFS `last_reported`) 
  are UTC, per Spark's default session timezone. This means the two legs are on 
  different clocks, off by 4-5 hours depending on daylight saving time (EDT vs EST).
  This does not affect the batch/streaming ID bridge (station_reference), since 
  that join is on station identity, not time. It would affect any future analysis 
  correlating batch and streaming data by time-of-day (e.g. "does live availability 
  match this hour's historical demand") — such a comparison would currently be 
  silently misaligned by the EDT/EST offset unless explicitly converted. Not fixed; 
  documented as a known limitation given no such time-correlated analysis was 
  ultimately built in this project.

## Streaming (GBFS → Redpanda → Spark → Postgres)
- Non-destructive reads: Redpanda doesn't delete messages once Spark consumes 
  them — consumers track their own offset via checkpointing; data persists until 
  retention expires, independent of read activity.
- Topic retention set to 48h — real-time availability data has no long-term 
  analytical value beyond feeding rolling aggregates; unbounded retention would 
  accumulate disk cost for no benefit. Requires a named Docker volume 
  (`redpanda-data`) to survive `docker-compose down`, not just `stop`.
- Messages keyed by `station_id` so all updates for one station land on the same 
  partition, in order — required for correct per-station windowed aggregation.
- Schema (Spark) explicitly typed `station_id` as StringType throughout — 
  validated after the fact: 19-digit IDs (e.g. `1903998756008763590`) survived 
  intact in Postgres output with no truncation.
- **Incident (self-diagnosed)**: station `2123940513700131194` appeared twice for 
  the same 5-minute window in Postgres output. Cause: `foreachBatch` + JDBC 
  `.mode("append")` wrote every micro-batch's intermediate state for still-open 
  windows.
- Fix required **both**: (1) idempotent upsert via `ON CONFLICT (station_id, 
  window_start) DO UPDATE` (via psycopg2 + `execute_values`, since JDBC's default 
  writer can't upsert) — makes repeated writes to an in-progress window safe; 
  (2) `.withWatermark("event_time", "10 minutes")` — bounds when a window is 
  considered closed. Watermarking alone doesn't prevent duplicate writes before 
  the cutoff; upserting alone doesn't decide when a window's data is final — both 
  were necessary together.
- Verified checkpoint-restart recovery: killed the Spark container mid-run, 
  confirmed on restart it resumed from its last offset (log evidence) rather than 
  reprocessing or losing data, and confirmed no gap/duplication in Postgres 
  station counts across the restart boundary.
- Added retry-with-backoff to the GBFS producer poll loop (same pattern as 
  `download.py`'s batch retry logic); exhausted retries log and skip to the next 
  30s cycle rather than crashing the process.
- **Trade-off (documented)**: at-least-once semantics, not exactly-once. A Spark 
  failure between computing and committing a batch could still cause a 
  reprocessed batch to overwrite a window with a recomputed (still accurate) 
  value. True exactly-once would need a transactional outbox pattern coordinating 
  Kafka offset commits with the Postgres write — not implemented, since 
  staleness/duplication within a 10-minute window is acceptable for inherently 
  approximate, 30s-repolled availability data.
## Limitation: Rebalancing Trucks Not Distinguishable from Organic Usage
- GBFS station_status.json provides only a net bike-count snapshot per station 
  per poll; it has no field distinguishing a truck redistribution event from 
  organic rides. A large single-interval delta (e.g. +12 bikes in one 30s poll) 
  is structurally indistinguishable from twelve independent returns.
- Consequence: the current rolling 5-minute average blends both signals — a 
  truck restock during a window will silently affect "avg_bikes_available" 
  without being separable from real usage.
- Not implemented, but a viable future approach: flag single-poll deltas 
  exceeding a threshold (e.g. ±5) as likely non-organic events and exclude them 
  from demand-oriented aggregates, since ride-driven changes are almost always 
  ±1 per event. This is a heuristic, not a certainty, since the feed provides no 
  ground truth for redistribution events.

## ID Scheme Bridge — Batch vs. Streaming
- Initial join attempt on `station_id` directly (live GBFS vs. batch 
  `dim_station`): **0 of 2,520 matched.**
- Root cause: live GBFS `station_status.json` uses an internal UUID/long-numeric 
  `station_id`, structurally different from the historical short-code scheme 
  (e.g. `HB407`, `JC094`, `6459.07`) used in trip CSVs.
- Bridge found: GBFS's `station_information.json` exposes a `short_name` field 
  matching the historical short-code format.
- Match rate: **2,408 of 2,523 batch station_ids (95.4%)** found in current live 
  `short_name` values. Remaining ~4.6% gap attributed to station churn (opened 
  after historical data's range, or since decommissioned) — not investigated 
  further, treated as within expected/acceptable bounds for a bridge across two 
  systems observed at different points in time.
- Implementation: `station_reference` table in warehouse Postgres 
  (`gbfs_station_id`, `short_name`, `station_name`), refreshed **daily** via a 
  dedicated Airflow DAG (`station_reference_refresh`) — station metadata changes 
  rarely, so daily cadence is deliberately loose, not tied to the 30s streaming 
  cadence. Dashboard joins live availability → `station_reference` → 
  `dim_station` at query time (Option B: enrich at the dashboard layer, not in 
  the Spark stream itself), since the mapping is near-static and doesn't need 
  streaming-level freshness.

## Analytical Finding: Live Empty-Station Count
- Live dashboard surfaced 63–95 of 2,523 stations at `avg_bikes_available = 0`, 
  captured ~11:20 AM NYC local time on a weekday.
- Checked against historical demand by hour: hour 11 is **not** a peak (220K 
  trips historically vs. 291K at hour 8, 442K at hour 17) — it's a relative lull. 
  The "high demand outpacing rebalancing" hypothesis does **not** hold; dropped 
  rather than forced to fit.
- Revised interpretation: depletion during a comparatively low-demand hour more 
  plausibly reflects rebalancing lag (trucks not yet caught up from the morning 
  peak) than real-time demand pressure. **Not conclusively proven** — would need 
  actual rebalancing/truck data (not available) to confirm directly. Stated as an 
  open hypothesis, not a settled finding.
- **Methodology note**: an initial count query (63) and a follow-up detail query 
  (95 rows) run moments apart against the live, 30s-refreshing stream resolved 
  `MAX(window_start)` independently and likely hit two different windows — not a 
  bug, but a real property of querying a moving stream. Fixed by pinning 
  `window_start` once via a CTE and reusing it across all queries in the same 
  analysis.
- **Deferred**: geographic clustering of empty stations (concentrated vs. 
  scattered) was proposed as a follow-up but explicitly not pursued — noted as 
  out of scope for this project, not forgotten.
- Directly answers the original project's stated goal ("stations that run empty 
  most often") as a **live** signal — the concrete payoff of the streaming leg 
  beyond architectural completeness.

## CI
- dbt CI (`dbt build --target ci`) runs against a committed sample Parquet 
  fixture, not live MinIO/S3 — GitHub Actions runners have no access to local 
  infra.
- Fixture creation caught two real bugs before they reached CI: (1) the export 
  query initially omitted `hive_partitioning=true`, which would have silently 
  produced a fixture missing the `region`/`source_month` columns entirely; 
  (2) an unstratified `LIMIT 1000` would have pulled a skewed sample from a 
  single partition, giving CI no real coverage of region variation or the 
  incremental `fact_trips` month-filtering logic. Fixed via `ROW_NUMBER() 
  PARTITION BY region, source_month ... QUALIFY rn <= 150`, giving a stratified 
  150-row-per-partition sample (900 rows total: 2 regions × 3 months present in 
  Silver at fixture-creation time — May/June/July; April not present, since it 
  only ever existed as rare boundary-leak rows too sparse to appear in a random 
  150-row sample of May).
- Caught a naming inconsistency during fixture verification: an earlier draft 
  referenced a `year_month` column that was never real in the actual pipeline 
  (superseded by the `trip_month`/`source_month` split) — confirmed via grep that 
  production dbt models correctly reference `trip_month`/`source_month`, not 
  the stale name.
- Fixture does not exercise the Bronze→Silver cleaning rules themselves (null 
  filtering, dedup, malformed-duration filtering) since it's sampled from 
  already-cleaned Silver data — CI validates Silver's *output guarantees* 
  (not_null, unique, relationships), not the cleaning logic that produces them. 
  Noted as a scope boundary, not a gap to necessarily close.
- Validated: `dbt build --target ci` passes 10/10 tests locally, in-memory, with 
  zero dependency on Docker/MinIO/Postgres being up.

## Postgres Sink Retention (Intentional Asymmetry)
- Unlike Redpanda's 48h topic retention, the Postgres station_rolling_availability 
  table has no pruning — it retains full history indefinitely. This is deliberate: 
  Redpanda only needs to retain enough for Spark to consume once, while the 
  "chronic shortage" dashboard panel requires cumulative history across the table's 
  entire lifetime to be meaningful. Trade-off: this table will grow unbounded over 
  a long-running deployment; acceptable for a portfolio project's timeframe, would 
  need a retention/rollup strategy (e.g. aggregate older windows into daily 
  summaries) for real production use.