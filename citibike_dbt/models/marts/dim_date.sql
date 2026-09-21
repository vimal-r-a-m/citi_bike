{{ config(materialized='view') }}

with date_spine as (
    select cast(unnest(generate_series(date '2025-01-01', date '2030-12-31', interval 1 day)) as date) as date_day
)

select
    date_day as date_key,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_spine


-- date_key   | year | month | day | day_of_week | is_weekend
-- -----------|------|-------|-----|-------------|-----------
-- 2026-09-14 | 2026 | 9     | 14  | 1           | false
-- 2026-09-19 | 2026 | 9     | 19  | 6           | true
-- 2026-09-20 | 2026 | 9     | 20  | 0           | true
