-- GOLD: rides + weather by daypart. One row per date x rider type x daypart.
-- Captures within-day weather "impression" (rainy morning vs sunny evening)
-- matched to how many of each rider type rode in that part of the day.
with enriched as (select * from {{ ref('int_trips_enriched') }}),

-- tag each trip with its daypart, from the trip's hour
tagged as (
    select
        *,
        case
            when extract(hour from started_at) between 6  and 11 then 'morning'
            when extract(hour from started_at) between 12 and 17 then 'afternoon'
            when extract(hour from started_at) between 18 and 22 then 'evening'
            else 'overnight'
        end as daypart
    from enriched
),

-- weather per (date x daypart): one reading per hour first, so a busy hour
-- doesn't get its weather counted once per trip
daypart_weather as (
    select
        trip_date,
        daypart,
        avg(temp_c)       as avg_temp_c,
        sum(precip_mm)    as total_precip_mm,
        avg(feels_like_c) as avg_feels_like_c
    from (
        select distinct trip_date, trip_hour, daypart,
               temp_c, precip_mm, feels_like_c
        from tagged
    )
    group by trip_date, daypart
),

-- rides per (date x rider type x daypart)
daypart_demand as (
    select
        trip_date,
        member_casual,
        is_weekend,
        daypart,
        count(*) as rides
    from tagged
    group by trip_date, member_casual, is_weekend, daypart
)

select
    d.trip_date,
    d.member_casual,
    d.is_weekend,
    d.daypart,
    d.rides,
    w.avg_temp_c,
    w.total_precip_mm,
    w.avg_feels_like_c,
    case when w.avg_temp_c < 5  then 'cold'
         when w.avg_temp_c < 18 then 'mild'
         else 'warm' end as temp_bucket,
    case when w.total_precip_mm = 0   then 'none'
         when w.total_precip_mm < 2.5 then 'light'
         else 'heavy' end as precip_bucket
from daypart_demand d
left join daypart_weather w
    on d.trip_date = w.trip_date
   and d.daypart   = w.daypart