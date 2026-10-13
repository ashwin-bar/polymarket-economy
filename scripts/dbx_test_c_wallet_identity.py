# Databricks notebook source
# Test C (brief §9 concern 1): wallet identity around the Deposit Wallet launch on 4 May 2026.
# One Spark pass over the whole bronze users table:
#   - first-seen wallets per UTC day (first fill across the full history since Nov 2022)
#   - size of new wallets' first day (cash traded and fills)
#   - activity and last-seen day of wallets that existed before the launch
#
# Days are true UTC days from the chain-time lookup, passed in as block bounds (the dataset
# timestamp is not used). Output is one row per day: aggregates only, no wallet-level rows.
#
# Parameters:
#   start_date        first UTC day of the window, YYYY-MM-DD
#   day_first_blocks  comma-separated first block of each UTC day from start_date onwards;
#                     the last entry closes the last day (rows from it onwards are ignored)
#   vet_from, vet_to  veterans = first seen before vet_from and active on at least one day in
#                     [vet_from, vet_to]; regulars = veterans active on 8 or more of those days
#   launch            Deposit Wallet launch date

# COMMAND ----------

import json
from datetime import date, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULTS = {"start_date": "2026-01-01", "day_first_blocks": "", "vet_from": "2026-03-01",
            "vet_to": "2026-05-03", "launch": "2026-05-04"}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)  # noqa: F821 (dbutils and spark are provided by Databricks)
p = {name: dbutils.widgets.get(name) for name in DEFAULTS}  # noqa: F821

start = date.fromisoformat(p["start_date"])
blocks = [int(x) for x in p["day_first_blocks"].split(",")]
n_days = len(blocks) - 1


def day_index(iso: str) -> int:
    return (date.fromisoformat(iso) - start).days


vet_from, vet_to, launch = day_index(p["vet_from"]), day_index(p["vet_to"]), day_index(p["launch"])

# Day -1 is everything before the window; only the wallet's existence matters there.
bounds = [(-1, 0, blocks[0])] + [(d, blocks[d], blocks[d + 1]) for d in range(n_days)]
spark.createDataFrame(bounds, "day int, first_block long, end_block long").createOrReplaceTempView("day_bounds")  # noqa: F821

users = (spark.read.parquet("/Volumes/workspace/polymarket/bronze/users")  # noqa: F821
         .select("address", F.col("block_number").cast("long").alias("block_number"), "usd_amount"))
users.createOrReplaceTempView("users_bronze")

# COMMAND ----------

wallet_days = spark.sql("""
    SELECT /*+ BROADCAST(b), RANGE_JOIN(b, 50000) */
           u.address, b.day, count(*) AS fills, sum(u.usd_amount) AS cash
    FROM users_bronze u JOIN day_bounds b
      ON u.block_number >= b.first_block AND u.block_number < b.end_block
    GROUP BY u.address, b.day""")  # noqa: F821

w = Window.partitionBy("address")
in_vet_window = (F.col("day") >= vet_from) & (F.col("day") <= vet_to)
tagged = (wallet_days
          .withColumn("first_day", F.min("day").over(w))
          .withColumn("last_day", F.max("day").over(w))
          .withColumn("vet_days", F.sum(F.when(in_vet_window, 1).otherwise(0)).over(w)))

is_new = F.col("day") == F.col("first_day")
is_vet = (F.col("first_day") < vet_from) & (F.col("vet_days") >= 1)
is_reg = (F.col("first_day") < vet_from) & (F.col("vet_days") >= 8)
is_pre = F.col("first_day") < launch
is_last = F.col("day") == F.col("last_day")


def n(cond):
    return F.sum(F.when(cond, 1).otherwise(0))


def cash(cond):
    return F.round(F.sum(F.when(cond, F.col("cash")).otherwise(0)), 0)


def pct(col, q):
    return F.percentile_approx(F.when(is_new, F.col(col)), q, 10000)


daily = (tagged.groupBy("day").agg(
    F.count("*").alias("active_wallets"),
    F.sum("fills").alias("fills"),
    F.round(F.sum("cash"), 0).alias("cash_traded"),
    n(is_new).alias("new_wallets"),
    cash(is_new).alias("new_cash_first_day"),
    F.round(pct("cash", 0.5), 2).alias("new_cash_p50"),
    F.round(pct("cash", 0.9), 2).alias("new_cash_p90"),
    F.round(pct("cash", 0.99), 2).alias("new_cash_p99"),
    pct("fills", 0.5).alias("new_fills_p50"),
    pct("fills", 0.9).alias("new_fills_p90"),
    n(is_new & (F.col("cash") >= 1000)).alias("new_cash_ge_1k"),
    n(is_new & (F.col("cash") >= 10000)).alias("new_cash_ge_10k"),
    n(is_new & (F.col("fills") >= 50)).alias("new_fills_ge_50"),
    n(is_pre).alias("pre_launch_active"),
    cash(is_pre).alias("pre_launch_cash"),
    n(is_vet).alias("vet_active"),
    cash(is_vet).alias("vet_cash"),
    n(is_vet & is_last).alias("vet_last_seen"),
    n(is_reg).alias("reg_active"),
    cash(is_reg).alias("reg_cash"),
    n(is_reg & is_last).alias("reg_last_seen"),
).orderBy("day").collect())

# COMMAND ----------

rows = []
for r in daily:
    d = r.asDict()
    d["date"] = "before " + start.isoformat() if d["day"] < 0 else (start + timedelta(days=d["day"])).isoformat()
    rows.append(d)
totals = {
    "wallets_total": sum(d["new_wallets"] for d in rows),
    "wallets_first_seen_before_window": next((d["new_wallets"] for d in rows if d["day"] < 0), 0),
    "veterans": sum(d["vet_last_seen"] for d in rows),
    "regulars": sum(d["reg_last_seen"] for d in rows),
}
result = {"params": {k: v for k, v in p.items() if k != "day_first_blocks"}, "days": n_days,
          "first_block": blocks[0], "end_block": blocks[-1], "totals": totals, "daily": rows}
print(json.dumps(totals, indent=2))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
