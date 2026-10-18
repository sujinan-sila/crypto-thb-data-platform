{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'minute_start'],
    liquid_clustered_by=['symbol', 'minute_start'],
    on_schema_change='append_new_columns'
) }}

with candles as (
    select * from {{ source('silver', 'candles_1m') }}
    {% if is_incremental() %}
    -- New rows by when Spark wrote them, not by minute_start: the last minutes
    -- of each stream run only come out in the next run, with an older minute_start
    where processed_at > (select max(processed_at) from {{ this }})
    {% endif %}
)

select
    s.symbol_key,
    c.symbol,
    c.minute_start,
    to_date(from_utc_timestamp(c.minute_start, 'Asia/Bangkok')) as date_th,
    c.open, c.high, c.low, c.close,
    c.volume, c.quote_volume, c.trade_count,
    c.processed_at
from candles c
left join {{ ref('dim_symbol') }} s
    on s.symbol = c.symbol
   and c.minute_start >= s.valid_from
   and c.minute_start < s.valid_to