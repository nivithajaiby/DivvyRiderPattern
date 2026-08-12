-- SILVER: clean Divvy trips. Grain: one row per clean trip.
with source as (
    select * from {{ source('raw', 'divvy_trips') }}
),
typed as (
    select
        ride_id,
        rideable_type,
        lower(trim(member_casual))               as member_casual,
        try_to_timestamp_ntz(started_at)         as started_at,
        try_to_timestamp_ntz(ended_at)           as ended_at,
        trim(start_station_name)                 as start_station_name,
        start_station_id,
        trim(end_station_name)                   as end_station_name,
        end_station_id,
        try_to_double(start_lat)                 as start_lat,
        try_to_double(start_lng)                 as start_lng,
        try_to_double(end_lat)                   as end_lat,
        try_to_double(end_lng)                   as end_lng
    from source
),
deduped as (
    select * from typed
    qualify row_number() over (partition by ride_id order by started_at) = 1
),
cleaned as (
    select
        *,
        datediff('second', started_at, ended_at) / 60.0 as trip_duration_min
    from deduped
    where started_at is not null
      and ended_at   is not null
      and ended_at > started_at
      and datediff('hour', started_at, ended_at) <= 24
      and not (
            datediff('second', started_at, ended_at) < 60
            and start_station_id = end_station_id
            and start_station_id is not null
      )
      -- ONE-MONTH FILTER: uncomment the next two lines to build on Jan 2022 only.
      -- and started_at >= '2022-01-01'
      -- and started_at <  '2022-02-01'
)
select
    *,
    (start_station_name is null)                               as station_missing,
    not (start_lat between 41.6 and 42.1
         and start_lng between -88.0 and -87.4)                as geo_bad,
    cast(started_at as date)                                   as trip_date,
    date_trunc('hour', started_at)                            as trip_hour,
    dayname(started_at)                                        as day_of_week,
    (dayofweekiso(started_at) >= 6)                            as is_weekend
from cleaned
