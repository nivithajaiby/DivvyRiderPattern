-- SILVER (enriched): one row per clean trip + weather + calendar.
with trips as (select * from {{ ref('stg_divvy_trips') }}),
weather as (select * from {{ ref('stg_weather') }}),
holidays as (select * from {{ ref('stg_holidays') }})
select
    t.ride_id, t.rideable_type, t.member_casual,
    t.started_at, t.ended_at, t.trip_duration_min,
    t.trip_date, t.trip_hour, t.day_of_week, t.is_weekend,
    t.start_station_name, t.start_station_id,
    t.station_missing, t.geo_bad,
    w.temp_c, w.precip_mm, w.wind_kmh, w.feels_like_c,
    (h.holiday_date is not null) as is_holiday,
    h.holiday_name
from trips t
left join weather  w on t.trip_hour = w.weather_hour
left join holidays h on t.trip_date = h.holiday_date
