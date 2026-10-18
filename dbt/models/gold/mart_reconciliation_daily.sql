select
    symbol,
    to_date(minute_start) as date_utc,
    count(*) as minutes_compared,
    count_if(status = 'match') as minutes_matched,
    round(100 * count_if(status = 'match') / count(*), 3) as match_pct,
    count_if(status = 'missing_in_stream') as missing_in_stream,
    count_if(status = 'stream_has_fewer_trades') as fewer_trades,
    count_if(status = 'stream_has_more_trades') as more_trades,
    count_if(status = 'values_differ') as values_differ
from {{ ref('mart_stream_reconciliation') }}
group by all