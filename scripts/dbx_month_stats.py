# Databricks notebook source
# Content statistics for one bronze month in the Volume, for comparison with the laptop copy
# (scripts/12_month_stats.py computes the same figures locally with DuckDB).
#
# Parameter: path = folder in the Volume holding the month's part-NN.parquet files

# COMMAND ----------

import json

from pyspark.sql import functions as F

dbutils.widgets.text("path", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
path = dbutils.widgets.get("path")  # noqa: F821

df = spark.read.parquet(path)  # noqa: F821
s = df.agg(
    F.count("*").alias("n_rows"),
    F.countDistinct("transaction_hash", "log_index").alias("distinct_keys"),
    F.countDistinct("transaction_hash").alias("distinct_transactions"),
    F.sum("usd_amount").alias("sum_usd_amount"),
    F.sum("token_amount").alias("sum_token_amount"),
    F.min("block_number").alias("min_block"),
    F.max("block_number").alias("max_block"),
    # exact integer checksums, independent of row order and of floating-point summation order
    F.sum(F.col("block_number").cast("decimal(38,0)")).alias("sum_block_number"),
    F.sum(F.col("log_index").cast("decimal(38,0)")).alias("sum_log_index"),
    F.sum(F.round(F.col("usd_amount") * 1e6).cast("decimal(38,0)")).alias("sum_usd_micro"),
    F.sum(F.round(F.col("token_amount") * 1e6).cast("decimal(38,0)")).alias("sum_token_micro"),
).first()

result = {k: (float(v) if k in ("sum_usd_amount", "sum_token_amount") else int(v))
          for k, v in s.asDict().items()}
result["sum_usd_amount"] = repr(result["sum_usd_amount"])
result["sum_token_amount"] = repr(result["sum_token_amount"])
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))  # noqa: F821
