-- BOT financial institutions' holidays. Real response shape (checked 14 Oct 2026):
-- {"result": {"data": [{"Date": "2026-10-13", "HolidayDescription": "...",
--                       "HolidayDescriptionThai": "...", ...}, ...]}}
-- Note: data is a list directly, not data.data_detail like the FX API.
with days as (
    select
        explode(from_json(payload:result.data, 'array<string>')) as h,
        ingested_at
    from {{ source('bronze', 'th_bank_holidays') }}
)
select
    to_date(h:Date::string) as holiday_date,
    h:HolidayDescription::string as holiday_name,
    h:HolidayDescriptionThai::string as holiday_name_th
from days
-- the same year can be fetched again after BOT adds a special holiday
qualify row_number() over (partition by h:Date::string order by ingested_at desc) = 1