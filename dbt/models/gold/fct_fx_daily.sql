-- One USD/THB rate for every calendar day. Weekends and Thai bank holidays
-- have no published rate, so they carry the last published one forward.
select
    d.date_day,
    fx.usd_thb_mid as published_rate,
    last_value(fx.usd_thb_mid, true) over (
        order by d.date_day rows between unbounded preceding and current row
    ) as usd_thb,
    fx.usd_thb_mid is not null as is_published
from {{ ref('dim_date') }} d
left join {{ ref('stg_fx') }} fx on fx.rate_date = d.date_day
where d.date_day <= current_date()