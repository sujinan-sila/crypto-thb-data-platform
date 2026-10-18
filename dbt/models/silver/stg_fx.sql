-- BOT weighted-average interbank rate, THB per 1 USD. Real response shape:
-- {"result": {"data": {"data_detail": [{"period": "2026-10-09", "rate": "33.5580000"}, ...]}}}
-- Business days only; weekends and Thai bank holidays are simply absent.
with details as (
    select
        explode(from_json(payload:result.data.data_detail, 'array<string>')) as d,
        ingested_at
    from {{ source('bronze', 'fx_usd_thb') }}
)
select
    to_date(d:period::string) as rate_date,
    cast(d:rate::string as decimal(12, 7)) as usd_thb_mid
from details
-- each daily file asks for the last 7 days, so a date appears in several files
qualify row_number() over (partition by d:period::string order by ingested_at desc) = 1