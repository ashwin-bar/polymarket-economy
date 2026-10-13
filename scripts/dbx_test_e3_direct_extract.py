# Databricks notebook source
# Test E, step 3: extract one true UTC month straight from Hugging Face into the Volume.
#
# DuckDB (pip-installed in the session) reads hf://...@main/<table>.parquet filtered by block
# range, writes one Parquet file to local disk, the file is copied into the Unity Catalog
# Volume, and Spark reads it back to confirm the row count.
#
# Parameters (job base_parameters / notebook widgets):
#   table, year, month     which bronze month
#   first_block, end_block block range of the true UTC month (end exclusive), from the
#                          laptop's lookup\month_bounds.csv
#   expected_rows          row count from the laptop's bronze manifest

# COMMAND ----------

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

from pyspark.sql import functions as F

for name, default in [("table", "trades"), ("year", "2024"), ("month", "12"),
                      ("first_block", ""), ("end_block", ""), ("expected_rows", "0")]:
    dbutils.widgets.text(name, default)  # noqa: F821 (dbutils and spark are provided by Databricks)
p = {k: dbutils.widgets.get(k) for k in  # noqa: F821
     ("table", "year", "month", "first_block", "end_block", "expected_rows")}
table, year, month = p["table"], int(p["year"]), int(p["month"])
first_block, end_block, expected_rows = int(p["first_block"]), int(p["end_block"]), int(p["expected_rows"])

REPO = "SII-WANGZJ/Polymarket_data"
source = f"hf://datasets/{REPO}@main/{table}.parquet"
volume_dir = f"/Volumes/workspace/polymarket/bronze/{table}/year={year:04d}/month={month:02d}"
timings = {}

t = time.time()
subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "duckdb"], check=True)
import duckdb  # noqa: E402

timings["pip_install_s"] = round(time.time() - t, 1)

# COMMAND ----------

con = duckdb.connect()
con.execute("SET preserve_insertion_order = false")
con.execute("CALL enable_logging('HTTP')")
where = f"block_number >= {first_block} AND block_number < {end_block}"

t = time.time()
remote_count = con.execute(f"SELECT count(*) FROM read_parquet('{source}') WHERE {where}").fetchone()[0]
timings["remote_count_s"] = round(time.time() - t, 1)

local_dir = tempfile.mkdtemp()
local_file = os.path.join(local_dir, "part-00.parquet")
t = time.time()
con.execute(f"""
    COPY (SELECT * FROM read_parquet('{source}') WHERE {where})
    TO '{local_file}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1048576)""")
timings["extract_s"] = round(time.time() - t, 1)

http_requests, http_bytes = con.execute("""
    SELECT count(*),
           coalesce(sum(split_part(replace(request.headers['Range'], 'bytes=', ''), '-', 2)::BIGINT
                      - split_part(replace(request.headers['Range'], 'bytes=', ''), '-', 1)::BIGINT + 1), 0)
    FROM duckdb_logs_parsed('HTTP') WHERE request.type = 'GET'""").fetchone()
file_bytes = os.path.getsize(local_file)

# COMMAND ----------

t = time.time()
# Plain file copy through the /Volumes mount. dbutils.fs.cp cannot read the session's local
# disk on serverless (LocalFilesystemAccessDeniedException for file:/tmp/...).
os.makedirs(volume_dir, exist_ok=True)
shutil.copyfile(local_file, f"{volume_dir}/part-00.parquet")
timings["copy_to_volume_s"] = round(time.time() - t, 1)
shutil.rmtree(local_dir)

t = time.time()
df = spark.read.parquet(volume_dir)  # noqa: F821
stats = df.agg(F.count("*").alias("n"), F.min("block_number").alias("lo"),
               F.max("block_number").alias("hi")).first()
timings["spark_readback_s"] = round(time.time() - t, 1)

result = {
    "volume_dir": volume_dir,
    "files": [{"name": f.name, "bytes": f.size} for f in dbutils.fs.ls(volume_dir)],  # noqa: F821
    "remote_count": remote_count,
    "spark_rows": stats["n"],
    "expected_rows": expected_rows,
    "rows_match": stats["n"] == expected_rows == remote_count,
    "block_min": stats["lo"], "block_max": stats["hi"],
    "file_bytes": file_bytes,
    "http_get_requests": http_requests, "http_bytes_received": int(http_bytes),
    "timings": timings,
    "duckdb_version": duckdb.__version__, "spark_version": spark.version,  # noqa: F821
    "cpu_count": os.cpu_count(),
}

# Quota impact: the Free Edition quota is undisclosed. Report usage if the billing system
# table is readable; it normally lags by some hours.
try:
    rows = spark.sql("""
        SELECT usage_date, sku_name, usage_unit, round(sum(usage_quantity), 4) AS quantity
        FROM system.billing.usage WHERE usage_date >= current_date() - 2
        GROUP BY ALL ORDER BY usage_date, sku_name""").collect()  # noqa: F821
    result["billing_usage"] = [r.asDict() for r in rows]
except Exception as exc:
    result["billing_usage_error"] = f"{type(exc).__name__}: {exc}"[:600]

print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
