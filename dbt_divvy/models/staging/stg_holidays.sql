-- SILVER: clean holidays. One row per US public holiday date.
with source as (
    select * from {{ source('raw', 'holidays') }}
),
typed as (
    select
        try_to_date(holiday_date)  as holiday_date,
        trim(holiday_name)         as holiday_name
    from source
)
select holiday_date, holiday_name
from typed
where holiday_date is not null
qualify row_number() over (partition by holiday_date order by holiday_name) = 1
