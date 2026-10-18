-- Binance kline: [open_time, open, high, low, close, volume, close_time,
--                 quote_volume, trades, ...]  positional, no field names
with files as (
    select
        source_file,
        ingested_at,
        regexp_extract(source_file, '/([A-Z]+)_p[12][.]json$', 1) as symbol,
        from_json(payload, 'array<array<string>>') as klines
    from {{ source('bronze', 'klines_1m') }}
    -- ADF may re-run a day and rewrite the same file; keep the newest copy
    qualify row_number() over (partition by source_file order by ingested_at desc) = 1
)
select
    symbol,
    timestamp_millis(cast(k[0] as bigint)) as minute_start,
    cast(k[1] as double) as open,
    cast(k[2] as double) as high,
    cast(k[3] as double) as low,
    cast(k[4] as double) as close,
    cast(k[5] as double) as volume,
    cast(k[7] as double) as quote_volume,
    cast(k[8] as int) as trade_count
from files
lateral view explode(klines) t as k