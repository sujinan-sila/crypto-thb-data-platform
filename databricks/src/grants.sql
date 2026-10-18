-- Analysts read the gold layer and nothing else.
GRANT USE CATALOG ON CATALOG crypto TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA crypto.gold TO `analysts`;
-- (no grants on bronze or silver: raw and intermediate data stay with engineers)

ALTER SCHEMA crypto.gold SET TAGS ('layer' = 'gold', 'domain' = 'market_data');
ALTER TABLE crypto.gold.mart_daily_thb SET TAGS ('consumer' = 'power_bi');
COMMENT ON TABLE crypto.gold.fct_candles_1m IS
  'One row per symbol per minute from the live Binance trade stream, UTC.';

SHOW GRANTS ON SCHEMA crypto.gold;
SHOW GRANTS ON SCHEMA crypto.bronze;