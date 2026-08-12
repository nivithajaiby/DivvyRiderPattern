-- GOLD: rides by hour-of-day x rider type x weekend. Commute vs leisure.
with enriched as (select * from {{ ref('int_trips_enriched') }})
select
    extract(hour from started_at) as hour_of_day,
    member_casual, is_weekend, count(*) as rides
from enriched
group by 1, 2, 3
order by 1, 2, 3
