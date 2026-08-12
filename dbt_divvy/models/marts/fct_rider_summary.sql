-- GOLD: behaviour summary per rider type. One row per rider type.
with enriched as (select * from {{ ref('int_trips_enriched') }})
select
    member_casual,
    count(*) as total_rides,
    round(avg(trip_duration_min), 1) as avg_duration_min,
    round(median(trip_duration_min), 1) as median_duration_min,
    round(100.0 * sum(iff(is_weekend, 1, 0)) / count(*), 1) as pct_weekend_rides
from enriched
group by member_casual
