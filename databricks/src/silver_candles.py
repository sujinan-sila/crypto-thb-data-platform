# Databricks notebook source
# crypto.bronze.trades -> crypto.silver.candles_1m (event-time windows, watermark, dedup)
from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "crypto")
catalog = dbutils.widgets.get("catalog")

# Binance sends "t" (trade id) and "T" (trade time); without this Spark treats
# them as the same column and fails as ambiguous
spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

trade_schema = T.StructType([
    T.StructField("s", T.StringType()),            # symbol
    T.StructField("t", T.LongType()),              # trade id
    T.StructField("p", T.StringType()),            # price (text on the wire)
    T.StructField("q", T.StringType()),            # quantity
    T.StructField("T", T.LongType()),              # trade time, ms, UTC
    T.StructField("received_at", T.LongType()),    # added by our producer
])


def parse(df):
    return (
        df.select(F.from_json("payload", trade_schema).alias("d"), "enqueued_at")
        .select(
            F.col("d.s").alias("symbol"),
            F.col("d.t").alias("trade_id"),
            F.col("d.p").cast("double").alias("price"),
            F.col("d.q").cast("double").alias("qty"),
            (F.col("d.T") / 1000).cast("timestamp").alias("trade_time"),
            "d.T", "d.received_at", "enqueued_at",
        )
    )

# COMMAND ----------

trades = parse(spark.readStream.table(f"{catalog}.bronze.trades"))

# Event Hubs may deliver a retried message twice; trade ids are unique per symbol.
# Binance -> Event Hubs latency measured at p99 364 ms, max 1.5 s, so a 30 s
# watermark drops almost nothing that arrives late.
deduped = (
    trades.withWatermark("trade_time", "30 seconds")
    .dropDuplicatesWithinWatermark(["symbol", "trade_id"])
)

# first()/last() have no guaranteed order in Spark; min/max of a struct that
# starts with the time gives the real first and last trade of the minute
first_price = F.min(F.struct("trade_time", "trade_id", "price")).getField("price")
last_price = F.max(F.struct("trade_time", "trade_id", "price")).getField("price")

candles = (
    deduped.groupBy(F.window("trade_time", "1 minute"), "symbol")
    .agg(
        first_price.alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        last_price.alias("close"),
        F.sum("qty").alias("volume"),
        F.sum(F.col("price") * F.col("qty")).alias("quote_volume"),
        F.count(F.lit(1)).alias("trade_count"),
    )
    .select(F.col("window.start").alias("minute_start"), "*")
    .drop("window")
    .withColumn("processed_at", F.current_timestamp())
)

table = f"{catalog}.silver.candles_1m"
before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

query = (
    candles.writeStream.outputMode("append")
    .option("checkpointLocation", f"/Volumes/{catalog}/bronze/checkpoints/candles_1m")
    .trigger(availableNow=True)
    .toTable(table)
)
query.awaitTermination()

after = spark.table(table).count()
print(f"candles written this run: {after - before:,}  (table now has {after:,})")
