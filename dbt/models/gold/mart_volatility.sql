-- Rolling 60-minute volatility of 1-minute log returns, annualised
with returns as (
    select
        symbol,
        minute_start,
        close,
        ln(close / lag(close) over (partition by symbol order by minute_start)) as log_return
    from {{ ref('fct_candles_1m') }}
)
select
    *,
    stddev_samp(log_return) over (
        partition by symbol order by minute_start
        rows between 59 preceding and current row
    ) * sqrt(60 * 24 * 365) as volatility_annualised
from returns