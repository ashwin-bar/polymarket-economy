# Databricks notebook source
# Follow-ups to test C (brief §9), over the whole bronze users table:
#   1. Profile of the veterans whose last-ever fill was on the day before the V2 cutover
#      (27 April 2026), against veterans who carried on and veterans who stopped earlier.
#   2. Wallets first seen on or after the Deposit Wallet launch (4 May 2026): how concentrated
#      their cash traded is, and whether heavy new wallets start when heavy veterans stop.
#   3. New wallets sharing an identical first-day cash amount with many others (scripted).
#
# Spark builds two per-wallet tables; DuckDB on the driver summarises them. Wallet-level rows
# stay inside the job: the output is aggregates only.
#
# Days are true UTC days from block bounds (same parameter as test C). An "hour" is one 24th of
# a UTC day by block count, which is close to a clock hour because block times are steady
# within a day; it is used only for the share of hours a wallet was active.
#
# Parameters:
#   start_date, day_first_blocks   as in dbx_test_c_wallet_identity.py
#   vet_from, vet_to               veteran window (first seen before vet_from, active in it)
#   cliff                          the day whose last-seen wallets are profiled
#   launch                         Deposit Wallet launch date
#   censor_from                    wallets last seen on or after this day count as still active

# COMMAND ----------

import json
import subprocess
import sys
from datetime import date

from pyspark.sql import functions as F

DEFAULTS = {"start_date": "2026-01-01", "day_first_blocks": "", "vet_from": "2026-03-01",
            "vet_to": "2026-05-03", "cliff": "2026-04-27", "launch": "2026-05-04",
            "censor_from": "2026-09-05"}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)  # noqa: F821 (dbutils and spark are provided by Databricks)
p = {name: dbutils.widgets.get(name) for name in DEFAULTS}  # noqa: F821

start = date.fromisoformat(p["start_date"])
blocks = [int(x) for x in p["day_first_blocks"].split(",")]
n_days = len(blocks) - 1


def day_index(iso: str) -> int:
    return (date.fromisoformat(iso) - start).days


vet_from, vet_to = day_index(p["vet_from"]), day_index(p["vet_to"])
cliff, launch, censor_from = day_index(p["cliff"]), day_index(p["launch"]), day_index(p["censor_from"])

bounds = [(-1, 0, blocks[0])] + [(d, blocks[d], blocks[d + 1]) for d in range(n_days)]
spark.createDataFrame(bounds, "day int, first_block long, end_block long").createOrReplaceTempView("day_bounds")  # noqa: F821
(spark.read.parquet("/Volumes/workspace/polymarket/bronze/users")  # noqa: F821
 .select("address", F.col("block_number").cast("long").alias("block_number"), "usd_amount", "role",
         F.col("market_id").cast("string").alias("market_id"))
 .createOrReplaceTempView("users_bronze"))

# COMMAND ----------

# One row per wallet over the whole history.
wallets = spark.sql(f"""
    WITH wallet_days AS (
        SELECT /*+ BROADCAST(b), RANGE_JOIN(b, 50000) */
               u.address, b.day, count(*) AS fills, sum(u.usd_amount) AS cash,
               count_if(u.role = 'maker') AS maker_fills
        FROM users_bronze u JOIN day_bounds b
          ON u.block_number >= b.first_block AND u.block_number < b.end_block
        GROUP BY u.address, b.day)
    SELECT address,
           min(day) AS first_day, max(day) AS last_day,
           min_by(cash, day) AS first_day_cash, min_by(fills, day) AS first_day_fills,
           count_if(day BETWEEN {vet_from} AND {vet_to}) AS vet_days,
           coalesce(sum(cash) FILTER (WHERE day BETWEEN {vet_from} AND {vet_to}), 0) AS vet_cash,
           count_if(day >= {launch}) AS post_days,
           coalesce(sum(cash) FILTER (WHERE day >= {launch}), 0) AS post_cash,
           coalesce(sum(fills) FILTER (WHERE day >= {launch}), 0) AS post_fills,
           coalesce(sum(maker_fills) FILTER (WHERE day >= {launch}), 0) AS post_maker_fills
    FROM wallet_days GROUP BY address""").toPandas()  # noqa: F821

# One row per wallet active between vet_from and the cliff day: the common period in which the
# wallets that stopped and the wallets that carried on can be compared.
profile = spark.sql(f"""
    SELECT /*+ BROADCAST(b), RANGE_JOIN(b, 50000) */
           u.address, count(*) AS fills, count_if(u.role = 'maker') AS maker_fills,
           sum(u.usd_amount) AS cash, count(DISTINCT u.market_id) AS markets,
           count(DISTINCT b.day) AS days_active,
           count(DISTINCT b.day * 24 + least(23, floor(24 * (u.block_number - b.first_block)
                                                        / (b.end_block - b.first_block)))) AS hours_active,
           count_if(b.day = {cliff}) AS fills_cliff_day,
           count_if(b.day = {cliff} AND u.role = 'maker') AS maker_fills_cliff_day,
           count_if(b.day BETWEEN {cliff} - 6 AND {cliff}) AS fills_last_week
    FROM users_bronze u JOIN day_bounds b
      ON u.block_number >= b.first_block AND u.block_number < b.end_block
    WHERE b.day BETWEEN {vet_from} AND {cliff}
    GROUP BY u.address""").toPandas()  # noqa: F821

# COMMAND ----------

subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "duckdb"], check=True)
import duckdb  # noqa: E402

con = duckdb.connect()
profile_hours = (cliff - vet_from + 1) * 24


def rows(sql: str) -> list[dict]:
    return con.execute(sql).fetchdf().to_dict("records")


# ---- 1. Veterans by what happened after the cliff day ------------------------------------
con.execute(f"""
    CREATE TABLE vets AS
    SELECT CASE WHEN w.last_day = {cliff} THEN 'A last fill on cliff day'
                WHEN w.last_day > {cliff} THEN 'B carried on'
                ELSE 'C stopped earlier' END AS grp,
           w.first_day < 0 AS seen_before_window,
           pr.*, pr.maker_fills / pr.fills AS maker_share, pr.hours_active / {profile_hours} AS hours_share
    FROM wallets w JOIN profile pr USING (address)
    WHERE w.first_day < {vet_from} AND w.vet_days >= 1""")


def dist(col: str, digits: int = 2) -> str:
    return (f"round(median({col}), {digits}) AS {col}_p50, round(quantile_cont({col}, 0.9), {digits}) AS {col}_p90, "
            f"round(quantile_cont({col}, 0.99), {digits}) AS {col}_p99, round(avg({col}), {digits}) AS {col}_mean")


veteran_profile = rows(f"""
    SELECT grp, count(*) AS wallets,
           round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_of_veterans,
           round(100.0 * sum(fills) / sum(sum(fills)) OVER (), 2) AS pct_of_veteran_fills,
           round(100.0 * sum(cash) / sum(sum(cash)) OVER (), 2) AS pct_of_veteran_cash,
           round(sum(cash) / 1e6, 2) AS cash_m,
           {dist('fills', 0)}, {dist('cash', 0)}, {dist('markets', 0)}, {dist('days_active', 0)},
           {dist('maker_share', 3)}, {dist('hours_share', 4)},
           round(100.0 * sum(maker_fills) / sum(fills), 2) AS pct_fills_as_maker,
           round(100.0 * count(*) FILTER (maker_share >= 0.9) / count(*), 2) AS pct_wallets_maker_90,
           round(100.0 * count(*) FILTER (maker_share = 0) / count(*), 2) AS pct_wallets_never_maker,
           round(100.0 * count(*) FILTER (hours_share >= 0.5) / count(*), 2) AS pct_wallets_hours_50,
           round(100.0 * count(*) FILTER (seen_before_window) / count(*), 2) AS pct_first_seen_before_2026,
           round(100.0 * count(*) FILTER (fills_last_week > 0) / count(*), 2) AS pct_active_in_last_week,
           round(100.0 * count(*) FILTER (fills_cliff_day > 0) / count(*), 2) AS pct_active_on_cliff_day,
           round(100.0 * sum(maker_fills_cliff_day) / nullif(sum(fills_cliff_day), 0), 2) AS pct_cliff_day_fills_as_maker,
           round(100.0 * count(*) FILTER (fills_cliff_day > 0 AND maker_fills_cliff_day = fills_cliff_day)
                 / nullif(count(*) FILTER (fills_cliff_day > 0), 0), 2) AS pct_only_maker_on_cliff_day,
           round(median(fills_cliff_day) FILTER (fills_cliff_day > 0), 0) AS cliff_day_fills_p50
    FROM vets GROUP BY grp ORDER BY grp""")
# Group A by size, to see whether it is one kind of wallet or several.
cliff_group_by_fills = rows("""
    SELECT CASE WHEN fills < 10 THEN '1: under 10 fills' WHEN fills < 100 THEN '2: 10-99'
                WHEN fills < 1000 THEN '3: 100-999' ELSE '4: 1,000+' END AS fills_in_window,
           count(*) AS wallets, round(sum(cash) / 1e6, 2) AS cash_m,
           round(100.0 * sum(maker_fills) / sum(fills), 2) AS pct_fills_as_maker,
           round(median(markets), 0) AS markets_p50, round(median(hours_share), 4) AS hours_share_p50
    FROM vets WHERE grp LIKE 'A%' GROUP BY ALL ORDER BY 1""")

# ---- 2. Wallets first seen on or after the launch ---------------------------------------
con.execute(f"""
    CREATE TABLE period AS
    SELECT CASE WHEN first_day >= {launch} THEN 'first seen after launch' ELSE 'first seen before launch' END AS cohort,
           *, post_maker_fills / nullif(post_fills, 0) AS maker_share
    FROM wallets WHERE post_cash > 0""")
concentration = rows("""
    WITH ranked AS (
        SELECT *, row_number() OVER (PARTITION BY cohort ORDER BY post_cash DESC) AS rk,
               count(*) OVER (PARTITION BY cohort) AS n, sum(post_cash) OVER (PARTITION BY cohort) AS total
        FROM period)
    SELECT cohort, any_value(n) AS wallets, round(any_value(total) / 1e6, 1) AS cash_m,
           round(median(post_cash), 2) AS cash_p50, round(quantile_cont(post_cash, 0.99), 0) AS cash_p99,
           round(100.0 * sum(post_cash) FILTER (rk <= 100) / any_value(total), 2) AS pct_cash_top_100,
           round(100.0 * sum(post_cash) FILTER (rk <= 1000) / any_value(total), 2) AS pct_cash_top_1000,
           round(100.0 * sum(post_cash) FILTER (rk <= n * 0.001) / any_value(total), 2) AS pct_cash_top_0_1pct,
           round(100.0 * sum(post_cash) FILTER (rk <= n * 0.01) / any_value(total), 2) AS pct_cash_top_1pct,
           round(100.0 * sum(post_cash) FILTER (rk <= n * 0.10) / any_value(total), 2) AS pct_cash_top_10pct,
           count(*) FILTER (post_cash >= 1e5) AS wallets_100k, count(*) FILTER (post_cash >= 1e6) AS wallets_1m,
           count(*) FILTER (post_cash >= 1e7) AS wallets_10m,
           round(100.0 * sum(post_cash) FILTER (post_cash >= 1e6) / any_value(total), 2) AS pct_cash_from_1m_wallets,
           round(100.0 * sum(post_maker_fills) FILTER (post_cash >= 1e6)
                 / sum(post_fills) FILTER (post_cash >= 1e6), 2) AS pct_fills_as_maker_1m_wallets,
           round(median(post_fills) FILTER (post_cash >= 1e6), 0) AS fills_p50_1m_wallets,
           round(median(post_days) FILTER (post_cash >= 1e6), 0) AS days_active_p50_1m_wallets
    FROM ranked GROUP BY cohort ORDER BY cohort""")

# Timing by week from the launch: heavy new wallets starting, heavy veterans last seen.
# Heavy = $1M+ (and $100K+) of cash traded: after the launch for new wallets, in the veteran
# window for veterans. Veterans last seen from censor_from onwards count as still active.
timing = rows(f"""
    WITH new_w AS (
        SELECT (first_day - {launch}) // 7 AS week,
               count(*) FILTER (post_cash >= 1e6) AS new_1m_starting,
               round(sum(post_cash) FILTER (post_cash >= 1e6) / 1e6, 1) AS new_1m_cash_m,
               count(*) FILTER (post_cash >= 1e5) AS new_100k_starting,
               count(*) FILTER (post_cash >= 1e6 AND first_day_cash >= 1e4) AS new_1m_big_first_day
        FROM wallets WHERE first_day >= {launch} GROUP BY 1),
    vet_w AS (
        SELECT (last_day - {launch}) // 7 AS week,
               count(*) FILTER (vet_cash >= 1e6) AS vet_1m_last_seen,
               round(sum(vet_cash) FILTER (vet_cash >= 1e6) / 1e6, 1) AS vet_1m_window_cash_m,
               count(*) FILTER (vet_cash >= 1e5) AS vet_100k_last_seen
        FROM wallets WHERE first_day < {vet_from} AND vet_days >= 1 GROUP BY 1)
    SELECT week, CASE WHEN week * 7 + {launch} >= {censor_from} THEN 'still active (censored)' ELSE '' END AS note,
           n.* EXCLUDE (week), v.* EXCLUDE (week)
    FROM new_w n FULL JOIN vet_w v USING (week) ORDER BY week""")
heavy_veterans = rows(f"""
    SELECT count(*) FILTER (vet_cash >= 1e6) AS vet_1m, count(*) FILTER (vet_cash >= 1e6 AND last_day < {launch}) AS vet_1m_stopped_before_launch,
           count(*) FILTER (vet_cash >= 1e6 AND last_day >= {launch} AND last_day < {censor_from}) AS vet_1m_stopped_after_launch,
           count(*) FILTER (vet_cash >= 1e6 AND last_day >= {censor_from}) AS vet_1m_still_active,
           count(*) FILTER (vet_cash >= 1e5) AS vet_100k,
           count(*) FILTER (vet_cash >= 1e5 AND last_day >= {launch} AND last_day < {censor_from}) AS vet_100k_stopped_after_launch,
           count(*) FILTER (vet_cash >= 1e5 AND last_day >= {censor_from}) AS vet_100k_still_active
    FROM wallets WHERE first_day < {vet_from} AND vet_days >= 1""")

# ---- 3. Identical first-day amounts ------------------------------------------------------
# A new wallet is flagged when 20 or more wallets first seen on the same day traded exactly the
# same first-day cash (to the cent), with that amount at least $100.
con.execute("""
    CREATE TABLE flagged AS
    SELECT *, count(*) OVER (PARTITION BY first_day, round(first_day_cash, 2)) AS twins
    FROM wallets WHERE first_day >= 0
    QUALIFY twins >= 20 AND first_day_cash >= 100""")
identical_amounts = {
    "by_month": rows(f"""
        SELECT strftime(DATE '{start.isoformat()}' + first_day::INTEGER, '%Y-%m') AS first_seen_month,
               count(*) AS flagged_wallets, count(DISTINCT (first_day, round(first_day_cash, 2))) AS amount_groups,
               round(sum(first_day_cash) / 1e6, 2) AS first_day_cash_m,
               round(median(first_day_fills), 0) AS first_day_fills_p50,
               round(100.0 * count(*) FILTER (first_day = last_day) / count(*), 1) AS pct_never_seen_again
        FROM flagged GROUP BY 1 ORDER BY 1"""),
    "top_amounts": rows("""
        SELECT round(first_day_cash, 2) AS first_day_cash, count(*) AS wallets, count(DISTINCT first_day) AS days,
               round(median(first_day_fills), 0) AS first_day_fills_p50
        FROM flagged GROUP BY 1 ORDER BY wallets DESC LIMIT 15"""),
    "share_of_post_launch": rows(f"""
        SELECT count(*) AS new_wallets_after_launch,
               count(*) FILTER (f.address IS NOT NULL) AS flagged,
               round(100.0 * sum(w.post_cash) FILTER (f.address IS NOT NULL) / sum(w.post_cash), 2) AS pct_of_their_cash,
               count(*) FILTER (w.post_cash >= 1e6) AS new_1m_wallets,
               count(*) FILTER (w.post_cash >= 1e6 AND f.address IS NOT NULL) AS new_1m_wallets_flagged
        FROM wallets w LEFT JOIN flagged f USING (address) WHERE w.first_day >= {launch}"""),
}

result = {"params": {k: v for k, v in p.items() if k != "day_first_blocks"},
          "wallets_total": len(wallets), "profile_window_hours": profile_hours,
          "veteran_profile": veteran_profile, "cliff_group_by_fills": cliff_group_by_fills,
          "concentration": concentration, "timing_by_week": timing, "heavy_veterans": heavy_veterans,
          "identical_first_day_amounts": identical_amounts}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
