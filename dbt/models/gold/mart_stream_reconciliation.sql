-- Every stream candle vs Binance's official kline for the same minute,
-- only inside the period the stream was running.
with stream_candles as  (
    select * from {{ source('silver', 'candles_1m') }}
),
coverage as (
    select symbol, min(minute_start) as first_minute, max(minute_start) as last_minute
    from stream_candles group by symbol
),
exchange as (
    select k.*
    from {{ ref('stg_klines') }} k
    join coverage c
      on c.symbol = k.symbol
     and k.minute_start between c.first_minute and c.last_minute
    where k.trade_count > 0
),
joined as (
    select
        coalesce(s.symbol, e.symbol) as symbol,
        coalesce(s.minute_start, e.minute_start) as minute_start,
        s.trade_count as stream_trades,
        e.trade_count as exchange_trades,
        s.open = e.open and s.high = e.high and s.low = e.low and s.close = e.close as ohlc_equal,
        abs(s.volume - e.volume) <= 1e-8 * greatest(e.volume, 1) as volume_equal
    from stream_candles s
    full outer join exchange e
      on e.symbol = s.symbol and e.minute_start = s.minute_start
)
select
    *,
    case
        when stream_trades is null then 'missing_in_stream'
        when exchange_trades is null then 'missing_in_exchange'
        when stream_trades < exchange_trades then 'stream_has_fewer_trades'
        when stream_trades > exchange_trades then 'stream_has_more_trades'
        when not ohlc_equal or not volume_equal then 'values_differ'
        else 'match'
    end as status
from joined