# Databricks notebook source
# Event Hubs (Kafka protocol) -> crypto.bronze.trades: raw messages plus Kafka metadata
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "crypto")
dbutils.widgets.text("eh_namespace", "ehns-cryptothb7")
catalog = dbutils.widgets.get("catalog")
namespace = dbutils.widgets.get("eh_namespace")

# COMMAND ----------

conn = dbutils.secrets.get("crypto", "eh-listen-connection")
# Databricks ships a shaded Kafka client, hence "kafkashaded." in front
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

table = f"{catalog}.bronze.trades"
before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

query = (
    bronze.writeStream
    .option("checkpointLocation", f"/Volumes/{catalog}/bronze/checkpoints/trades")
    .trigger(availableNow=True)
    .toTable(table)
)
query.awaitTermination()

after = spark.table(table).count()
print(f"trades added this run: {after - before:,}  (table now has {after:,})")
