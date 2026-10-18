-- Current rules for each symbol, from the latest exchangeInfo file.
-- The snapshot on top of this keeps the history.
with latest_file as (
    select payload
    from {{ source('bronze', 'exchange_info') }}
    qualify row_number() over (order by ds desc, ingested_at desc) = 1
),
symbols as (
    select explode(from_json(payload:symbols, 'array<string>')) as s
    from latest_file
),
parsed as (
    select
        s:symbol::string as symbol,
        s:status::string as status,
        s:baseAsset::string as base_asset,
        s:quoteAsset::string as quote_asset,
        from_json(s:filters, 'array<string>') as filters
    from symbols
)
select
    symbol,
    status,
    base_asset,
    quote_asset,
    filter(filters, f -> f:filterType::string = 'PRICE_FILTER')[0]:tickSize::string as tick_size,
    filter(filters, f -> f:filterType::string = 'LOT_SIZE')[0]:stepSize::string as step_size
from parsed