# Databricks notebook source
# ADLS landing (raw API files from Data Factory) -> crypto.bronze.<source>, new files only
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "crypto")
dbutils.widgets.text("storage_account", "stcryptothb7")
catalog = dbutils.widgets.get("catalog")
landing = f"abfss://landing@{dbutils.widgets.get('storage_account')}.dfs.core.windows.net"

SOURCES = ["fx_usd_thb", "th_bank_holidays", "exchange_info", "klines_1m"]

# COMMAND ----------

for source in SOURCES:
    table = f"{catalog}.bronze.{source}"
    before = spark.table(table).count() if spark.catalog.tableExists(table) else 0

    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")  # one row per file: the API response as received
        # ADF re-running a day rewrites the same file names; load the new copy too
        # (dbt keeps the newest row per file)
        .option("cloudFiles.allowOverwrites", "true")
        .load(f"{landing}/{source}/")
        .select(
            F.col("value").alias("payload"),
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
        # the ds=YYYY-MM-DD folder Data Factory wrote into, as a real date column
        .withColumn("ds", F.regexp_extract("source_file", r"ds=(\d{4}-\d{2}-\d{2})", 1).cast("date"))
        .writeStream
        .option("checkpointLocation", f"/Volumes/{catalog}/bronze/checkpoints/{source}")
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()

    after = spark.table(table).count()
    print(f"{source:18} files added: {after - before}  (table now has {after})")
