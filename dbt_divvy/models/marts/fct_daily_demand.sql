-- GOLD: daily rides by rider type + weather + buckets. One row per date x type.
with enriched as (select * from {{ ref('int_trips_enriched') }}),
daily_weather as (
    select trip_date,
        avg(temp_c) as avg_temp_c, sum(precip_mm) as total_precip_mm,
        avg(wind_kmh) as avg_wind_kmh, max(is_holiday) as is_holiday
    from (select distinct trip_date, trip_hour, temp_c, precip_mm, wind_kmh, is_holiday from enriched)
    group by trip_date
),
demand as (
    select trip_date, member_casual, is_weekend, count(*) as rides
    from enriched group by trip_date, member_casual, is_weekend
)
select
    d.trip_date, d.member_casual, d.is_weekend, d.rides,
    w.avg_temp_c, w.total_precip_mm, w.avg_wind_kmh, w.is_holiday,
    case when w.avg_temp_c < 5 then 'cold' when w.avg_temp_c < 18 then 'mild' else 'warm' end as temp_bucket,
    case when w.total_precip_mm = 0 then 'none' when w.total_precip_mm < 2.5 then 'light' else 'heavy' end as precip_bucket
from demand d
left join daily_weather w on d.trip_date = w.trip_date
