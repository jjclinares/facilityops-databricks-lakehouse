with days as (
    select explode(
        sequence(
            make_date(year(current_date()) - 2, 1, 1),
            make_date(year(current_date()) + 1, 12, 31),
            interval 1 day
        )
    ) as date_day
)

select
    date_day,
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    date_format(date_day, 'MMM') as month_short_name,
    cast(date_trunc('MONTH', date_day) as date) as month_start_date,
    day(date_day) as day_of_month,
    weekday(date_day) + 1 as iso_day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    weekofyear(date_day) as iso_week,
    weekday(date_day) >= 5 as is_weekend
from days
