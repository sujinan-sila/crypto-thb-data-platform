select
    {{ dbt_utils.generate_surrogate_key(['symbol', 'dbt_valid_from']) }} as symbol_key,
    symbol,
    base_asset,
    quote_asset,
    status,
    tick_size,
    step_size,
    -- The first snapshot ran days after the stream started. Let the first
    -- version cover all earlier history, or older candles find no symbol row.
    case
        when row_number() over (partition by symbol order by dbt_valid_from) = 1
            then timestamp '1970-01-01'
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_symbols') }}