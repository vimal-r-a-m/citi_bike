with all_stations as (
    select 
        start_station_id as station_id,
        start_station_name as station_name,
        start_lat as lat,
        start_lng as lng,
        started_at as event_time 
    from {{ ref('stg_trips') }}
    where start_station_id is not null
    
    union all
    
    select end_station_id as station_id, end_station_name as station_name, end_lat as lat, end_lng as lng, ended_at as event_time 
    from {{ ref('stg_trips') }}
    where end_station_id is not null
)

select
    station_id,
    arg_max(station_name, event_time) as station_name,
    arg_max(lat, event_time) as lat,
    arg_max(lng, event_time) as lng
from all_stations
group by station_id