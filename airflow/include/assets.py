from airflow.sdk import Asset

# The Databricks provider checks these URIs: databricks://<workspace host>/<catalog>/<schema>/<table>
WORKSPACE = "adb-7405611334478618.18.azuredatabricks.net"

# Not one table: "the four batch bronze tables were refreshed" (fx, holidays, exchange_info, klines)
BRONZE_BATCH = Asset(f"databricks://{WORKSPACE}/crypto/bronze/batch_sources")
SILVER_CANDLES = Asset(f"databricks://{WORKSPACE}/crypto/silver/candles_1m")
