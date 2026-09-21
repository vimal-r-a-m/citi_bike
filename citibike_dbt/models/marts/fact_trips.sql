{{
    config(
        materialized='incremental',
        unique_key=['ride_id']
    )
}}

select
    ride_id,
    cast(started_at as date) as start_date_key,
    cast(ended_at as date) as end_date_key,
    started_at,
    ended_at,
    start_station_id,
    end_station_id,
    rideable_type,
    member_casual,
    region,
    trip_month,
    date_diff('minute', started_at, ended_at) as duration_minutes
from {{ ref('stg_trips') }}

{% if is_incremental() %}
WHERE trip_month > (SELECT MAX(trip_month) FROM {{ this }})
{% endif %}