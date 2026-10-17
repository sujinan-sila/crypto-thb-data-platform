# Databricks notebook source
conn = dbutils.secrets.get("crypto", "eh-listen-connection")
namespace = "ehns-cryptothb7"

jaas = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{conn}";'
)

kafka_options = {
    "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
    "subscribe": "trades",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": jaas,
}

print(conn)  # shows [REDACTED]: Databricks never prints secret values

# COMMAND ----------

df = (
    spark.read.format("kafka")
    .options(**kafka_options)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
display(df.groupBy("partition").count().orderBy("partition"))

# COMMAND ----------

from pyspark.sql import functions as F

display(
    df.select(
        F.col("key").cast("string").alias("symbol"),
        F.col("value").cast("string").alias("payload"),
        "partition", "offset", "timestamp",
    ).limit(10)
)

# COMMAND ----------

bronze = (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 500000)
    .load()
    .select(
        F.col("key").cast("string").alias("symbol"),
        F.col("value").cast("string").alias("payload"),
        "partition",
        "offset",
        F.col("timestamp").alias("enqueued_at"),
        F.current_timestamp().alias("ingested_at"),
    )
)

query = (
    bronze.writeStream
    .option("checkpointLocation", "/Volumes/crypto/bronze/checkpoints/trades")
    .trigger(availableNow=True)
    .toTable("crypto.bronze.trades")
)
query.awaitTermination()
print(query.lastProgress["numInputRows"] if query.lastProgress else "no new rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT symbol, COUNT(*) AS trades, MIN(enqueued_at) AS first_seen, MAX(enqueued_at) AS last_seen
# MAGIC FROM crypto.bronze.trades
# MAGIC GROUP BY symbol
# MAGIC ORDER BY trades DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY crypto.bronze.trades

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM crypto.bronze.trades VERSION AS OF 1