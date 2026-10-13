# Databricks notebook source
# Silver wallet-month table (docs/SILVER_PLAN.md §1 and §5): one row per wallet and UTC month
# in which it had at least one fill, from polymarket_silver.wallet_fills.
#
#   fills, maker_fills, maker_share
#   days_active, hours_active      distinct UTC days and clock hours with a fill
#   hours_available                clock hours from the wallet's first fill in the month to the
#                                  end of the month's data (4 Oct 2026 for the last month), so a
#                                  wallet that starts mid-month is not under-classified
#   hours_share                    hours_active / hours_available
#   markets_traded, cash_traded, cash_staked (BUY fills only), cash_per_fill (brief §7)
#   segment, segment_rule_version  rule v1 (decision 11): a wallet-month is automated if it has
#                                  1,000+ fills and hours_share >= 0.25, or 10,000+ fills whatever
#                                  its hours; automated splits into market_maker (maker_share >=
#                                  0.50) and high_frequency_taker; everything else is retail.
#                                  Known limit: wallets with 1,000-9,999 fills active in under a
#                                  quarter of hours stay retail (5-10% of fills in the months
#                                  calibrated). Thresholds are not scaled for the two partial
#                                  months (Nov 2022, Oct 2026), which are under-classified.
#
# Two plain group-bys over the fills (wallet x hour, wallet x market) rather than several
# count-distincts in one pass.
#
# Parameter: months = "2024-12" or "2024-12,2026-03" builds only those months (pilot; each is
# written with replaceWhere). Empty = the whole history, table replaced.

# COMMAND ----------

import json
import time

dbutils.widgets.text("months", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
months = [m for m in dbutils.widgets.get("months").split(",") if m]  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
TARGET = f"{SILVER}.wallet_month"
DATA_END = "2026-10-05 00:00:00"  # first second after the last day of data
# Rule v1 (decision 11, calibrated by dbx_silver_segment_calibration.py).
RULE_VERSION, MIN_FILLS, MIN_HOURS_SHARE, MAKER_SPLIT, ALWAYS_AUTOMATED_FILLS = "v1", 1000, 0.25, 0.50, 10000
month_filter = ("WHERE month_start IN (" + ", ".join(f"DATE '{m}-01'" for m in months) + ")") if months else ""
result, problems, timings = {"months": months or "all"}, [], {}

# COMMAND ----------

BUILD_SQL = f"""
    WITH hourly AS (
        SELECT wallet_id, month_start, date_trunc('hour', ts_utc) AS hour_ts, count(*) AS fills,
               count_if(role = 'maker') AS maker_fills, sum(usd_amount) AS cash_traded,
               sum(CASE WHEN direction = 'BUY' THEN usd_amount ELSE 0 END) AS cash_staked
        FROM {SILVER}.wallet_fills {month_filter} GROUP BY wallet_id, month_start, date_trunc('hour', ts_utc)),
    a AS (
        SELECT wallet_id, month_start, sum(fills) AS fills, sum(maker_fills) AS maker_fills,
               count(DISTINCT CAST(hour_ts AS DATE)) AS days_active, count(*) AS hours_active,
               min(hour_ts) AS first_hour, sum(cash_traded) AS cash_traded, sum(cash_staked) AS cash_staked
        FROM hourly GROUP BY wallet_id, month_start),
    m AS (
        SELECT wallet_id, month_start, count(DISTINCT market_id) AS markets_traded
        FROM {SILVER}.wallet_fills {month_filter} GROUP BY wallet_id, month_start),
    j AS (
        SELECT a.wallet_id, a.month_start, a.fills, a.maker_fills, a.maker_fills / a.fills AS maker_share,
               a.days_active, a.hours_active,
               CAST((unix_timestamp(least(CAST(add_months(a.month_start, 1) AS TIMESTAMP), TIMESTAMP '{DATA_END}'))
                     - unix_timestamp(a.first_hour)) / 3600 AS BIGINT) AS hours_available,
               m.markets_traded, a.cash_traded, a.cash_staked, a.cash_traded / a.fills AS cash_per_fill
        FROM a JOIN m USING (wallet_id, month_start))
    SELECT wallet_id, month_start, fills, maker_fills, maker_share, days_active, hours_active, hours_available,
           hours_active / hours_available AS hours_share, markets_traded, cash_traded, cash_staked, cash_per_fill,
           CASE WHEN (fills >= {MIN_FILLS} AND hours_active / hours_available >= {MIN_HOURS_SHARE})
                     OR fills >= {ALWAYS_AUTOMATED_FILLS}
                THEN CASE WHEN maker_share >= {MAKER_SPLIT} THEN 'market_maker' ELSE 'high_frequency_taker' END
                ELSE 'retail' END AS segment,
           '{RULE_VERSION}' AS segment_rule_version
    FROM j"""

t = time.time()
writer = spark.sql(BUILD_SQL).write.format("delta").mode("overwrite").partitionBy("month_start")  # noqa: F821
if months:
    writer = writer.option("replaceWhere", "month_start IN (" + ", ".join(f"'{m}-01'" for m in months) + ")")
else:
    writer = writer.option("overwriteSchema", "true")
writer.saveAsTable(TARGET)
timings["build_s"] = round(time.time() - t, 1)

# COMMAND ----------

# Checks against wallet_fills, month by month: fills from the load log (which matched bronze),
# wallets from the load log's distinct count, cash from one more pass over the fills.
t = time.time()
checks = [r.asDict() for r in spark.sql(f"""
    WITH wm AS (
        SELECT month_start, count(*) AS wallets, sum(fills) AS fills, sum(maker_fills) AS maker_fills,
               sum(cash_traded) AS cash_traded, sum(cash_staked) AS cash_staked,
               count_if(hours_active > hours_available OR hours_available < 1) AS bad_hours,
               count_if(days_active > day(last_day(month_start)) OR hours_active < days_active
                        OR markets_traded < 1 OR maker_fills > fills) AS bad_counts,
               count_if(segment IS NULL) AS null_segment,
               count_if(segment = 'market_maker') AS market_makers,
               count_if(segment = 'high_frequency_taker') AS hf_takers,
               sum(CASE WHEN segment != 'retail' THEN fills ELSE 0 END) AS automated_fills
        FROM {TARGET} {month_filter} GROUP BY month_start),
    f AS (
        SELECT month_start, sum(usd_amount) AS cash_traded, count_if(role = 'maker') AS maker_fills,
               sum(CASE WHEN direction = 'BUY' THEN usd_amount ELSE 0 END) AS cash_staked
        FROM {SILVER}.wallet_fills {month_filter} GROUP BY month_start),
    l AS (
        SELECT month_start, rows AS fills, CAST(get_json_object(checks, '$.wallets') AS BIGINT) AS wallets
        FROM {SILVER}.load_log WHERE table_name = 'wallet_fills')
    SELECT CAST(wm.month_start AS STRING) AS month_start, wm.wallets, l.wallets AS wallets_expected, wm.fills,
           l.fills AS fills_expected, wm.maker_fills, f.maker_fills AS maker_fills_expected,
           abs(wm.cash_traded / f.cash_traded - 1) AS cash_traded_rel_diff,
           abs(wm.cash_staked / f.cash_staked - 1) AS cash_staked_rel_diff,
           wm.bad_hours, wm.bad_counts, wm.null_segment, wm.market_makers, wm.hf_takers,
           round(100.0 * wm.automated_fills / wm.fills, 2) AS pct_fills_automated
    FROM wm JOIN f USING (month_start) JOIN l USING (month_start) ORDER BY 1""").collect()]  # noqa: F821
timings["checks_s"] = round(time.time() - t, 1)
for c in checks:
    bad = []
    if c["wallets"] != c["wallets_expected"] or c["fills"] != c["fills_expected"] or c["maker_fills"] != c["maker_fills_expected"]:
        bad.append("wallets, fills or maker fills differ from wallet_fills")
    if c["cash_traded_rel_diff"] > 1e-9 or c["cash_staked_rel_diff"] > 1e-9:
        bad.append("cash differs from wallet_fills")
    if c["bad_hours"] or c["bad_counts"] or c["null_segment"]:
        bad.append(f"bad hours {c['bad_hours']}, bad counts {c['bad_counts']}, null segment {c['null_segment']}")
    if bad:
        problems.append(f"{c['month_start']}: " + "; ".join(bad))
s = spark.sql(f"""
    SELECT count(*) AS n, count(DISTINCT wallet_id, month_start) AS distinct_keys, count(DISTINCT month_start) AS months,
           count(DISTINCT wallet_id) AS wallets, sum(fills) AS fills
    FROM {TARGET} {month_filter}""").first().asDict()  # noqa: F821
if s["n"] != s["distinct_keys"]:
    problems.append(f"key not unique: {s['n']} rows, {s['distinct_keys']} keys")
if not months and (s["months"] != 48 or s["fills"] != 2_026_673_731 or s["wallets"] != 3_192_135):
    problems.append(f"totals: {s}, expected 48 months, 2,026,673,731 fills, 3,192,135 wallets")
size = spark.sql(f"DESCRIBE DETAIL {TARGET}").first()  # noqa: F821
result.update(summary=s, by_month=checks, timings=timings, table_bytes=size["sizeInBytes"], problems=problems)
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
