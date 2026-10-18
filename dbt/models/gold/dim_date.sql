with days as (
    select explode(sequence(date '2026-01-01', date '2027-12-31', interval 1 day)) as date_day
)
select
    d.date_day,
    year(d.date_day) as year,
    month(d.date_day) as month,
    date_format(d.date_day, 'E') as day_name,
    dayofweek(d.date_day) in (1, 7) as is_weekend,
    h.holiday_date is not null as is_th_bank_holiday,
    h.holiday_name,
    not (dayofweek(d.date_day) in (1, 7) or h.holiday_date is not null) as is_th_business_day
from days d
left join {{ ref('stg_holidays') }} h on h.holiday_date = d.date_day