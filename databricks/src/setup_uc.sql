CREATE CATALOG IF NOT EXISTS crypto
  MANAGED LOCATION 'abfss://lakehouse@stcryptothb7.dfs.core.windows.net/crypto'
  COMMENT 'Crypto market data priced in THB';

CREATE SCHEMA IF NOT EXISTS crypto.bronze COMMENT 'Data exactly as received';
CREATE SCHEMA IF NOT EXISTS crypto.silver COMMENT 'Cleaned and typed: 1-minute candles, parsed reference data';
CREATE SCHEMA IF NOT EXISTS crypto.gold   COMMENT 'Star schema and marts for analysts and Power BI';

CREATE VOLUME IF NOT EXISTS crypto.bronze.checkpoints;