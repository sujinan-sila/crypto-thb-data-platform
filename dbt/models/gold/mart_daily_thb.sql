with daily as (
    select
        symbol,
        date_th,
        min_by(open, minute_start) as open_usdt,
        max(high) as high_usdt,
        min(low) as low_usdt,
        max_by(close, minute_start) as close_usdt,
        sum(volume) as volume,
        sum(quote_volume) as turnover_usdt,
        count(*) as minutes_with_trades
    from {{ ref('fct_candles_1m') }}
    group by symbol, date_th
)
select
    d.*,
    try_divide(d.turnover_usdt, d.volume) as vwap_usdt,
    fx.usd_thb,
    fx.is_published as fx_published_that_day,
    d.close_usdt * fx.usd_thb as close_thb,
    d.turnover_usdt * fx.usd_thb as turnover_thb
from daily d
left join {{ ref('fct_fx_daily') }} fx on fx.date_day = d.date_th