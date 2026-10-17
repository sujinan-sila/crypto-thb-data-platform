# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark.conf.set("spark.sql.caseSensitive", "true")
spark.conf.set("spark.sql.session.timeZone", "UTC")

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

display(parse(spark.read.table("crypto.bronze.trades")).limit(10))

# COMMAND ----------

trades_all = parse(spark.read.table("crypto.bronze.trades"))
display(
    trades_all.select(
        (F.col("received_at") - F.col("T")).alias("binance_to_producer_ms"),
        ((F.col("enqueued_at").cast("double") * 1000) - F.col("T")).alias("binance_to_eventhubs_ms"),
    ).summary("min", "50%", "99%", "max")
)

# COMMAND ----------

trades = parse(spark.readStream.table("crypto.bronze.trades"))

deduped = (
    trades.withWatermark("trade_time", "30 seconds")
    .dropDuplicatesWithinWatermark(["symbol", "trade_id"])
)

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

table = "crypto.silver.candles_1m"
before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

query = (
    candles.writeStream.outputMode("append")
    .option("checkpointLocation", "/Volumes/crypto/bronze/checkpoints/candles_1m")
    .trigger(availableNow=True)
    .toTable(table)
)
query.awaitTermination()

after = spark.table(table).count()
print(f"candles written this run: {after - before:,}  (table now has {after:,})")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT symbol, minute_start, open, high, low, close, trade_count
# MAGIC FROM crypto.silver.candles_1m
# MAGIC ORDER BY minute_start DESC, symbol
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) duplicate candles: should return nothing
# MAGIC SELECT symbol, minute_start, COUNT(*) AS n
# MAGIC FROM crypto.silver.candles_1m
# MAGIC GROUP BY ALL
# MAGIC HAVING n > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) a candle that breaks the rules of OHLC: should return nothing
# MAGIC SELECT * FROM crypto.silver.candles_1m
# MAGIC WHERE high < greatest(open, close) OR low > least(open, close)