# Databricks notebook source
# Test E, steps 2 and 3: read a bronze month back from the Unity Catalog Volume with Spark.
#
# Parameters (job base_parameters / notebook widgets):
#   path           folder in the Volume holding the month's part-NN.parquet files
#   expected_rows  row count recorded in the laptop's bronze manifest

# COMMAND ----------

import json
import time

from pyspark.sql import functions as F

dbutils.widgets.text("path", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
dbutils.widgets.text("expected_rows", "0")  # noqa: F821
path = dbutils.widgets.get("path")  # noqa: F821
expected_rows = int(dbutils.widgets.get("expected_rows"))  # noqa: F821

started = time.time()
files = [{"name": f.name, "bytes": f.size} for f in dbutils.fs.ls(path)]  # noqa: F821
df = spark.read.parquet(path)  # noqa: F821
stats = df.agg(
    F.count("*").alias("n_rows"),
    F.min("block_number").alias("block_min"),
    F.max("block_number").alias("block_max"),
    F.countDistinct("transaction_hash", "log_index").alias("distinct_keys"),
    F.sum("token_amount").alias("shares"),
).first()

result = {
    "path": path,
    "files": files,
    "columns": [f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields],
    "rows": stats["n_rows"],
    "expected_rows": expected_rows,
    "rows_match": stats["n_rows"] == expected_rows,
    "distinct_keys": stats["distinct_keys"],
    "block_min": stats["block_min"],
    "block_max": stats["block_max"],
    "shares": float(stats["shares"]),
    "spark_version": spark.version,  # noqa: F821
    "seconds": round(time.time() - started, 1),
}
# default=str: Spark reads the source's unsigned 64-bit columns as decimal(20,0)
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
