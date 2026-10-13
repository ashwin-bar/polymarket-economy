# Databricks notebook source
# V2 completeness check (brief §9): does the fill history after the 28 April 2026 V2 cutover
# add up to markets.volume as well as it did before?
#
# Spark sums trades per market over the whole bronze layer (shares matched = token_amount, the
# notional measure of brief §7). DuckDB on the driver joins that to markets.parquet read straight
# from Hugging Face main and compares per market. A market's period comes from its own fills
# (markets.created_at and end_date are not reliable, concern 14):
#   pre    last fill before 28 April 2026 (UTC)
#   post   first fill on or after 29 April 2026 (UTC)
#   spans  anything else
# Only closed markets with markets.volume > 0 are compared. Output is aggregates per group.
#
# Parameters:
#   month_first_blocks   "YYYY-MM:block,..." first block of each UTC month, oldest first
#   pre_end_block        first block of 28 April 2026
#   post_start_block     first block of 29 April 2026

# COMMAND ----------

import json
import subprocess
import sys

from pyspark.sql import functions as F

for name in ("month_first_blocks", "pre_end_block", "post_start_block"):
    dbutils.widgets.text(name, "")  # noqa: F821 (dbutils and spark are provided by Databricks)
months = [(m, int(b)) for m, b in (x.split(":") for x in dbutils.widgets.get("month_first_blocks").split(","))]  # noqa: F821
pre_end = int(dbutils.widgets.get("pre_end_block"))  # noqa: F821
post_start = int(dbutils.widgets.get("post_start_block"))  # noqa: F821

per_market = (spark.read.parquet("/Volumes/workspace/polymarket/bronze/trades")  # noqa: F821
              .groupBy(F.col("market_id").cast("string").alias("market_id"))
              .agg(F.count("*").alias("fills"),
                   F.sum("token_amount").alias("shares"),
                   F.min(F.col("block_number").cast("long")).alias("first_block"),
                   F.max(F.col("block_number").cast("long")).alias("last_block"))
              .toPandas())

# COMMAND ----------

subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "duckdb"], check=True)
import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

month_bounds = pd.DataFrame(months, columns=["month", "first_block"])
con = duckdb.connect()
con.execute("""
    CREATE TABLE markets AS
    SELECT id::VARCHAR AS market_id, volume, closed, created_at,
           regexp_matches(slug, 'updown|up-or-down') AS recurring
    FROM read_parquet('hf://datasets/SII-WANGZJ/Polymarket_data@main/markets.parquet')""")
con.execute(f"""
    CREATE TABLE joined AS
    SELECT m.market_id, m.volume, m.closed, m.recurring, m.created_at, t.fills, t.shares,
           (SELECT max(b.month) FROM month_bounds b WHERE b.first_block <= t.first_block) AS first_fill_month,
           CASE WHEN t.last_block < {pre_end} THEN 'pre'
                WHEN t.first_block >= {post_start} THEN 'post' ELSE 'spans' END AS period,
           t.shares / nullif(m.volume, 0) AS ratio
    FROM markets m LEFT JOIN per_market t USING (market_id)""")


def rows(sql: str) -> list[dict]:
    return con.execute(sql).fetchdf().to_dict("records")


STATS = """
    count(*) AS markets,
    sum(fills) AS fills,
    round(sum(shares) / 1e6, 3) AS shares_m,
    round(sum(volume) / 1e6, 3) AS markets_volume_m,
    round(sum(shares) / sum(volume), 4) AS ratio_of_totals,
    round(100.0 * count(*) FILTER (abs(ratio - 1) <= 0.01) / count(*), 2) AS pct_within_1pct,
    round(100.0 * count(*) FILTER (ratio < 0.99) / count(*), 2) AS pct_below,
    round(100.0 * count(*) FILTER (ratio > 1.01) / count(*), 2) AS pct_above,
    round(100.0 * sum(volume) FILTER (abs(ratio - 1) <= 0.01) / sum(volume), 2) AS pct_volume_within_1pct,
    round(100.0 * sum(volume) FILTER (ratio < 0.99) / sum(volume), 2) AS pct_volume_below,
    round(quantile_cont(ratio, 0.05), 4) AS ratio_p05,
    round(median(ratio), 4) AS ratio_p50,
    round(quantile_cont(ratio, 0.95), 4) AS ratio_p95"""
COMPARED = "closed = 1 AND volume > 0 AND fills IS NOT NULL"

result = {
    "params": {"pre_end_block": pre_end, "post_start_block": post_start},
    "markets_rows": con.execute("SELECT count(*) FROM markets").fetchone()[0],
    "markets_with_fills_in_trades": len(per_market),
    "trades_markets_missing_from_markets": rows("""
        SELECT count(*) AS markets, sum(fills) AS fills, round(sum(shares) / 1e6, 3) AS shares_m
        FROM per_market WHERE market_id NOT IN (SELECT market_id FROM markets)"""),
    "by_period": rows(f"""
        SELECT period, {STATS} FROM joined WHERE {COMPARED} GROUP BY period ORDER BY period"""),
    "by_period_recurring": rows(f"""
        SELECT period, recurring, {STATS} FROM joined WHERE {COMPARED}
        GROUP BY period, recurring ORDER BY period, recurring"""),
    "by_first_fill_month": rows(f"""
        SELECT CASE WHEN first_fill_month < '2026-01' THEN left(first_fill_month, 4) ELSE first_fill_month END AS first_fill,
               period, {STATS}
        FROM joined WHERE {COMPARED} GROUP BY ALL ORDER BY first_fill, period"""),
    # Size of the shortfall among markets below markets.volume, by period.
    "below_detail": rows(f"""
        SELECT period,
               count(*) FILTER (ratio < 0.5) AS ratio_under_0_5,
               count(*) FILTER (ratio >= 0.5 AND ratio < 0.9) AS ratio_0_5_to_0_9,
               count(*) FILTER (ratio >= 0.9 AND ratio < 0.99) AS ratio_0_9_to_0_99,
               round(sum(volume - shares) FILTER (ratio < 0.99) / 1e6, 3) AS shortfall_shares_m
        FROM joined WHERE {COMPARED} GROUP BY period ORDER BY period"""),
    # Closed markets with volume but no fills at all. created_at is the only date available for
    # these and is not reliable (concern 14), so read the months as indicative.
    "volume_but_no_fills_by_created_month": rows("""
        SELECT CASE WHEN created_at < TIMESTAMPTZ '2026-01-01' THEN strftime(created_at, '%Y')
                    ELSE strftime(created_at, '%Y-%m') END AS created,
               count(*) AS closed_markets_with_volume,
               count(*) FILTER (fills IS NULL) AS no_fills,
               round(100.0 * count(*) FILTER (fills IS NULL) / count(*), 2) AS pct_no_fills,
               round(sum(volume) FILTER (fills IS NULL) / 1e6, 3) AS volume_no_fills_m,
               round(100.0 * sum(volume) FILTER (fills IS NULL) / sum(volume), 3) AS pct_volume_no_fills
        FROM joined WHERE closed = 1 AND volume > 0 GROUP BY ALL ORDER BY created"""),
}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
