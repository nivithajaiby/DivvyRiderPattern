-- SILVER: clean weather. One row per hour. Chicago local time (matches Divvy).
with source as (
    select * from {{ source('raw', 'weather_hourly') }}
),
typed as (
    select
        try_to_timestamp_ntz(weather_time)   as weather_hour,
        try_to_double(temperature_2m)        as temp_c,
        try_to_double(precipitation)         as precip_mm,
        try_to_double(wind_speed_10m)        as wind_kmh,
        try_to_double(apparent_temperature)  as feels_like_c,
        _ingested_at
    from source
),
deduped as (
    select * from typed
    qualify row_number() over (partition by weather_hour order by _ingested_at desc) = 1
)
select weather_hour, temp_c, precip_mm, wind_kmh, feels_like_c
from deduped
where weather_hour is not null
