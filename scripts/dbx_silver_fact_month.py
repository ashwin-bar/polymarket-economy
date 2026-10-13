# Databricks notebook source
# Silver fact tables, one UTC month per run (docs/SILVER_PLAN.md):
#
#   polymarket_silver.trades        one row per matched fill (from bronze trades): volume
#   polymarket_silver.wallet_fills  one row per wallet side of a fill (from bronze users): people
#
# What silver does to a bronze month:
#   - chain time: ts_utc interpolated between block anchors (equi-join on the 1,000-block anchor);
#     the month is authoritative from the bronze partition, so a time that interpolation puts
#     just outside the month is clamped to the month's first or last second
#   - unsigned 64-bit columns cast to bigint; transaction_hash and re-derivable ids dropped;
#     the key is (block_number, log_index)
#   - wallet addresses replaced by wallet_id from polymarket_silver.dim_wallet
#
# The month is written with replaceWhere, so a re-run replaces exactly that month. Data quality
# checks compare the silver month with its bronze month; any hard failure raises, and the month
# is then not recorded in polymarket_silver.load_log. A month already in load_log is skipped.
#
# Parameters: table (trades | wallet_fills), year, month, force ('true' to rebuild a logged month)

# COMMAND ----------

import json
import time
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

for name, default in {"table": "trades", "year": "", "month": "", "force": "false"}.items():
    dbutils.widgets.text(name, default)  # noqa: F821 (dbutils and spark are provided by Databricks)
table, year, month = dbutils.widgets.get("table"), int(dbutils.widgets.get("year")), int(dbutils.widgets.get("month"))  # noqa: F821
force = dbutils.widgets.get("force") == "true"  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
source = "users" if table == "wallet_fills" else "trades"
bronze_path = f"/Volumes/workspace/polymarket/bronze/{source}/year={year:04d}/month={month:02d}"
month_start = date(year, month, 1)
next_month = date(year + month // 12, month % 12 + 1, 1)
target = f"{SILVER}.{table}"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {SILVER}.load_log (
    table_name STRING, month_start DATE, rows BIGINT, checks STRING, timings STRING, loaded_at TIMESTAMP)""")  # noqa: F821
spark.sql(f"""CREATE TABLE IF NOT EXISTS {SILVER}.dq_results (
    table_name STRING, month_start DATE, test STRING, expected STRING, actual STRING,
    passed BOOLEAN, hard BOOLEAN, run_at TIMESTAMP)""")  # noqa: F821
done = spark.sql(f"SELECT 1 FROM {SILVER}.load_log WHERE table_name = '{table}' AND month_start = DATE '{month_start}'").count()  # noqa: F821
if done and not force:
    dbutils.notebook.exit(json.dumps({"skipped": True, "reason": "month is in load_log"}))  # noqa: F821

# COMMAND ----------

COMMON_DDL = "month_start DATE, date_utc DATE, ts_utc TIMESTAMP, block_number BIGINT, log_index INT"
if table == "trades":
    ddl = (f"{COMMON_DDL}, market_id STRING, maker_wallet_id BIGINT, taker_wallet_id BIGINT, price DOUBLE, "
           "usd_amount DOUBLE, token_amount DOUBLE, maker_direction STRING, taker_direction STRING, "
           "exchange STRING, exchange_version STRING, source_timestamp BIGINT")
else:
    ddl = (f"{COMMON_DDL}, wallet_id BIGINT, role STRING, direction STRING, market_id STRING, price DOUBLE, "
           "usd_amount DOUBLE, token_amount DOUBLE")
spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({ddl}) USING DELTA PARTITIONED BY (month_start)")  # noqa: F821

timings = {}
bronze = spark.read.parquet(bronze_path)  # noqa: F821
anchors = F.broadcast(spark.table(f"{SILVER}.block_anchor"))  # noqa: F821
wallets = F.broadcast(spark.table(f"{SILVER}.dim_wallet")  # noqa: F821
                      .select(F.col("wallet_id").cast("long").alias("wallet_id"), "address"))

month_first_ts = F.lit(f"{month_start} 00:00:00").cast("timestamp")
month_last_ts = F.lit(f"{next_month} 00:00:00").cast("timestamp") - F.expr("INTERVAL 1 SECOND")
block = F.col("block_number").cast("long")
timed = (bronze.withColumn("block_number", block)
         .withColumn("anchor_block", (F.floor(block / 1000) * 1000).cast("long"))
         .join(anchors, "anchor_block", "left")
         .withColumn("raw_ts", F.timestamp_seconds(
             F.col("ts_unix") + (F.col("block_number") - F.col("anchor_block"))
             * (F.col("next_ts_unix") - F.col("ts_unix")) / 1000.0))
         .withColumn("ts_utc", F.greatest(F.least(F.col("raw_ts"), month_last_ts), month_first_ts))
         .withColumn("month_start", F.lit(str(month_start)).cast("date"))
         .withColumn("date_utc", F.to_date("ts_utc"))
         .withColumn("log_index", F.col("log_index").cast("int")))

if table == "trades":
    silver = (timed
              .join(wallets.select(F.col("address").alias("maker"), F.col("wallet_id").alias("maker_wallet_id")), "maker", "left")
              .join(wallets.select(F.col("address").alias("taker"), F.col("wallet_id").alias("taker_wallet_id")), "taker", "left")
              .select("month_start", "date_utc", "ts_utc", "block_number", "log_index",
                      F.col("market_id").cast("string").alias("market_id"), "maker_wallet_id", "taker_wallet_id",
                      "price", "usd_amount", "token_amount", "maker_direction", "taker_direction",
                      F.col("contract").alias("exchange"),
                      F.when(F.col("contract").endswith("_V2"), "V2").otherwise("V1").alias("exchange_version"),
                      F.col("timestamp").cast("long").alias("source_timestamp")))
else:
    silver = (timed.join(wallets, "address", "left")
              .select("month_start", "date_utc", "ts_utc", "block_number", "log_index", "wallet_id", "role",
                      "direction", F.col("market_id").cast("string").alias("market_id"), "price", "usd_amount",
                      "token_amount"))

t = time.time()
(silver.write.format("delta").mode("overwrite")
 .option("replaceWhere", f"month_start = '{month_start}'").saveAsTable(target))
timings["write_s"] = round(time.time() - t, 1)

# COMMAND ----------

# Bronze side and silver side, one pass each. The integer checksums are exact and independent
# of row order (same method as dbx_month_stats.py).


def dec(col):
    return F.sum(F.col(col).cast("decimal(38,0)"))


def micro(col):
    return F.sum(F.round(F.col(col) * 1e6).cast("decimal(38,0)"))


key_extra = ["role"] if table == "wallet_fills" else []
t = time.time()
b = bronze.agg(
    F.count("*").alias("rows"), dec("block_number").alias("sum_block"), dec("log_index").alias("sum_log_index"),
    micro("usd_amount").alias("usd_micro"), micro("token_amount").alias("token_micro"),
    F.countDistinct("transaction_hash", "log_index", *key_extra).alias("distinct_hash_keys"),
    F.countDistinct(F.col("block_number").cast("long"), "log_index", *key_extra).alias("distinct_block_keys"),
).first().asDict()
timings["bronze_checks_s"] = round(time.time() - t, 1)

t = time.time()
mine = spark.table(target).where(F.col("month_start") == F.lit(str(month_start)).cast("date"))  # noqa: F821
aggs = [
    F.count("*").alias("rows"), dec("block_number").alias("sum_block"), dec("log_index").alias("sum_log_index"),
    micro("usd_amount").alias("usd_micro"), micro("token_amount").alias("token_micro"),
    F.countDistinct("block_number", "log_index", *key_extra).alias("distinct_block_keys"),
    F.count_if(F.col("ts_utc").isNull()).alias("null_ts"),
    F.count_if(F.col("market_id").isNull()).alias("null_market_id"),
    F.count_if((F.col("ts_utc") < month_first_ts) | (F.col("ts_utc") > month_last_ts)).alias("ts_outside_month"),
    F.count_if(F.col("ts_utc") == month_first_ts).alias("at_first_second"),
    F.count_if(F.col("ts_utc") == month_last_ts).alias("at_last_second"),
    F.min("ts_utc").alias("min_ts"), F.max("ts_utc").alias("max_ts"), F.countDistinct("date_utc").alias("days"),
]
if table == "trades":
    aggs += [F.count_if(F.col("maker_wallet_id").isNull()).alias("null_maker_wallet_id"),
             F.count_if(F.col("taker_wallet_id").isNull()).alias("null_taker_wallet_id"),
             F.count_if(F.col("exchange_version") == "V2").alias("v2_rows"),
             F.max(F.abs(F.col("source_timestamp") - F.unix_timestamp("ts_utc"))).alias("max_abs_source_clock_error_s")]
else:
    aggs += [F.count_if(F.col("wallet_id").isNull()).alias("null_wallet_id"),
             F.count_if(~F.col("role").isin("maker", "taker")).alias("bad_role"),
             F.count_if(~F.col("direction").isin("BUY", "SELL")).alias("bad_direction"),
             F.count_if(F.col("role") == "maker").alias("maker_rows"),
             F.countDistinct("wallet_id").alias("wallets")]
s = mine.agg(*aggs).first().asDict()
timings["silver_checks_s"] = round(time.time() - t, 1)

# COMMAND ----------

tests = []  # (name, expected, actual, hard)
for k in ("rows", "sum_block", "sum_log_index", "usd_micro", "token_micro"):
    tests.append((f"{k} = bronze", b[k], s[k], True))
tests.append(("bronze (transaction_hash, log_index) unique", b["rows"], b["distinct_hash_keys"], True))
tests.append(("bronze (block_number, log_index) keys = (transaction_hash, log_index) keys",
              b["distinct_hash_keys"], b["distinct_block_keys"], True))
tests.append(("silver key unique", s["rows"], s["distinct_block_keys"], True))
for k in ("null_ts", "null_market_id", "ts_outside_month"):
    tests.append((k, 0, s[k], True))
if table == "trades":
    tests.append(("null_maker_wallet_id", 0, s["null_maker_wallet_id"], True))
else:
    for k in ("null_wallet_id", "bad_role", "bad_direction"):
        tests.append((k, 0, s[k], True))
    other = spark.sql(f"SELECT rows FROM {SILVER}.load_log WHERE table_name = 'trades' AND month_start = DATE '{month_start}'").collect()  # noqa: F821
    if other:
        tests.append(("maker rows = silver trades rows", other[0]["rows"], s["maker_rows"], True))

now = datetime.now(timezone.utc)
failed = [f"{name}: expected {exp}, got {act}" for name, exp, act, hard in tests if hard and exp != act]
spark.createDataFrame(  # noqa: F821
    [(table, month_start, name, str(exp), str(act), exp == act, hard, now) for name, exp, act, hard in tests],
    "table_name string, month_start date, test string, expected string, actual string, passed boolean, hard boolean, run_at timestamp",
).write.mode("append").saveAsTable(f"{SILVER}.dq_results")

size = spark.sql(f"DESCRIBE DETAIL {target}").first()  # noqa: F821
result = {"table": table, "month_start": str(month_start), "rows": s["rows"], "bronze": b, "silver": s,
          "timings": timings, "table_bytes_all_months": size["sizeInBytes"], "table_files_all_months": size["numFiles"],
          "failed": failed}
print(json.dumps(result, indent=2, default=str))
if failed:
    raise RuntimeError("FAILED, month not recorded in load_log: " + json.dumps(failed, default=str))

spark.sql(f"DELETE FROM {SILVER}.load_log WHERE table_name = '{table}' AND month_start = DATE '{month_start}'")  # noqa: F821
spark.createDataFrame(  # noqa: F821
    [(table, month_start, s["rows"], json.dumps(s, default=str), json.dumps(timings), now)],
    "table_name string, month_start date, rows bigint, checks string, timings string, loaded_at timestamp",
).write.mode("append").saveAsTable(f"{SILVER}.load_log")
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
