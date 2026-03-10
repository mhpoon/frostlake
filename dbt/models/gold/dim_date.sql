-- Generates a date dimension from 2015-01-01 to 2035-12-31
with date_spine as (
    select
        range::date as date_day
    from range(
        date '2015-01-01',
        date '2035-12-31',
        interval '1 day'
    )
)

select
    -- surrogate key
    cast(strftime(date_day, '%Y%m%d') as integer)   as date_key,

    date_day,
    year(date_day)                                   as year,
    month(date_day)                                  as month,
    day(date_day)                                    as day,
    quarter(date_day)                                as quarter,
    weekofyear(date_day)                             as week_of_year,
    dayofweek(date_day)                              as day_of_week,
    strftime(date_day, '%B')                         as month_name,
    strftime(date_day, '%A')                         as day_name,
    strftime(date_day, '%Y-%m')                      as year_month,
    case when dayofweek(date_day) in (0, 6)
        then false else true
    end                                              as is_weekday

from date_spine