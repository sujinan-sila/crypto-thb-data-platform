{{ config(severity='warn') }}
-- Days where the stream matched the exchange on fewer than 99% of minutes
select * from {{ ref('mart_reconciliation_daily') }} where match_pct < 99