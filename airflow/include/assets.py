from airflow.sdk import Asset

BRONZE_BATCH = Asset("databricks://crypto/bronze/batch_sources")
SILVER_CANDLES = Asset("databricks://crypto/silver/candles_1m")
