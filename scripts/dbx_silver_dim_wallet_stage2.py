# Databricks notebook source
# Silver wallet dimension, stage 2 (docs/SILVER_PLAN.md §1 and §5). Adds to each wallet:
#
#   entry_category, entry_subcategory        category of the market of its first fill (dim_market)
#   entered_via_world_cup_market, entered_via_us_election_2024_market
#   cohort                                   world_cup_2026 / us_election_2024 / benchmark_q1_2026,
#                                            by first-fill date alone (decision 9)
#   entry profile over the 30 days from its first fill: entry_fills, entry_maker_share,
#   entry_hours_active, entry_hours_available, entry_hours_share, entry_cash_traded,
#   entry_window_complete (false when the data ends inside the 30 days)
#   segment_at_entry                         segment rule v1 (decision 11) on that profile
#   is_scripted_cohort                       recomputed with the rule of decision 36
#
# wallet_id and every other stage 1 column are carried over unchanged, except that first_fill_ts
# is clamped to first_month, as the fact tables' ts_utc is. The table is replaced
# from itself, reading only the stage 1 columns, so the notebook can be re-run.
#
# Parameter: pilot_first_month = "2024-12" works only on wallets first seen that month and
# writes nothing. Empty = every wallet, table replaced.

# COMMAND ----------

import json
import time

dbutils.widgets.text("pilot_first_month", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
pilot = dbutils.widgets.get("pilot_first_month")  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
DATA_END = "2026-10-05 00:00:00"  # first second after the last day of data
RULE_VERSION, MIN_FILLS, MIN_HOURS_SHARE, MAKER_SPLIT, ALWAYS_AUTOMATED_FILLS = "v1", 1000, 0.25, 0.50, 10000
STAGE1 = ["wallet_id", "address", "first_fill_ts", "first_block", "first_log_index", "first_date", "first_month",
          "first_market_id", "last_date", "lifetime_fills", "lifetime_cash_traded", "first_day_cash", "first_day_fills",
          "is_left_censored", "last_seen_v2_eve"]
EXPECTED = {"wallets": 3_192_135, "lifetime_fills": 2_026_673_731, "world_cup_2026": 226_923,
            "us_election_2024": 218_361, "benchmark_q1_2026": 713_066}
timings = {}

wallet_filter = f"WHERE first_month = DATE '{pilot}-01'" if pilot else ""
# A pilot month's wallets need that month's fills and the next month's for their 30 days.
fills_filter = f"AND f.month_start IN (DATE '{pilot}-01', add_months(DATE '{pilot}-01', 1))" if pilot else ""

# COMMAND ----------

# The scripted-cohort count is over all wallets, pilot or not, so the flag is the same either way.
SELECT_SQL = f"""
    WITH s AS (
        SELECT {", ".join(c for c in STAGE1 if c != "first_fill_ts")},
               -- Stage 1 interpolated first_fill_ts without the month clamp the fact tables use, so a
               -- first fill within seconds of a month boundary could fall outside first_month. Clamp it
               -- here so it agrees with first_date and with wallet_fills.ts_utc (no-op on a re-run).
               greatest(least(first_fill_ts, CAST(add_months(first_month, 1) AS TIMESTAMP) - INTERVAL 1 SECOND),
                        CAST(first_month AS TIMESTAMP)) AS first_fill_ts,
               count(*) OVER (PARTITION BY first_date, CAST(round(first_day_cash * 100) AS BIGINT)) AS same_day_same_amount
        FROM {SILVER}.dim_wallet),
    w AS (SELECT * FROM s {wallet_filter}),
    hourly AS (
        SELECT /*+ BROADCAST(w) */ f.wallet_id, date_trunc('hour', f.ts_utc) AS hour_ts, count(*) AS fills,
               count_if(f.role = 'maker') AS maker_fills, sum(f.usd_amount) AS cash_traded
        FROM {SILVER}.wallet_fills f JOIN (SELECT wallet_id, first_fill_ts FROM w) w USING (wallet_id)
        WHERE f.ts_utc < w.first_fill_ts + INTERVAL 30 DAYS {fills_filter}
        GROUP BY f.wallet_id, date_trunc('hour', f.ts_utc)),
    e AS (
        SELECT wallet_id, sum(fills) AS entry_fills, sum(maker_fills) AS entry_maker_fills, count(*) AS entry_hours_active,
               min(hour_ts) AS first_hour, sum(cash_traded) AS entry_cash_traded
        FROM hourly GROUP BY wallet_id),
    j AS (
        SELECT w.*, e.entry_fills, e.entry_maker_fills / e.entry_fills AS entry_maker_share, e.entry_hours_active,
               -- clock hours from the wallet's first hour to the last hour the 30-day window (or the data) reaches
               CAST((unix_timestamp(date_trunc('hour', least(w.first_fill_ts + INTERVAL 30 DAYS, TIMESTAMP '{DATA_END}')
                                               - INTERVAL 1 SECOND))
                     - unix_timestamp(e.first_hour)) / 3600 AS BIGINT) + 1 AS entry_hours_available,
               e.entry_cash_traded,
               w.first_fill_ts + INTERVAL 30 DAYS <= TIMESTAMP '{DATA_END}' AS entry_window_complete,
               m.category AS entry_category, m.subcategory AS entry_subcategory,
               m.is_world_cup AS entered_via_world_cup_market,
               m.is_us_election_2024 AS entered_via_us_election_2024_market, m.taxonomy_version
        FROM w LEFT JOIN e USING (wallet_id)
        LEFT JOIN {SILVER}.dim_market m ON m.market_id = w.first_market_id)
    SELECT {", ".join(STAGE1)},
           (same_day_same_amount >= 20 AND first_day_cash >= 100
            AND (CAST(round(first_day_cash * 100) AS BIGINT) % 5000 != 0 OR same_day_same_amount >= 200)) AS is_scripted_cohort,
           CASE WHEN first_date BETWEEN DATE '2026-06-11' AND DATE '2026-07-19' THEN 'world_cup_2026'
                WHEN first_date BETWEEN DATE '2024-09-28' AND DATE '2024-11-05' THEN 'us_election_2024'
                WHEN first_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31' THEN 'benchmark_q1_2026' END AS cohort,
           entry_category, entry_subcategory, entered_via_world_cup_market, entered_via_us_election_2024_market,
           entry_fills, entry_maker_share, entry_hours_active, entry_hours_available,
           entry_hours_active / entry_hours_available AS entry_hours_share, entry_cash_traded, entry_window_complete,
           CASE WHEN (entry_fills >= {MIN_FILLS} AND entry_hours_active / entry_hours_available >= {MIN_HOURS_SHARE})
                     OR entry_fills >= {ALWAYS_AUTOMATED_FILLS}
                THEN CASE WHEN entry_maker_share >= {MAKER_SPLIT} THEN 'market_maker' ELSE 'high_frequency_taker' END
                ELSE 'retail' END AS segment_at_entry,
           '{RULE_VERSION}' AS segment_rule_version, taxonomy_version
    FROM j"""

t = time.time()
if pilot:
    spark.sql(SELECT_SQL).createOrReplaceTempView("dim_wallet_stage2")  # noqa: F821
    dim = "dim_wallet_stage2"
else:
    spark.sql(f"CREATE OR REPLACE TABLE {SILVER}.dim_wallet AS {SELECT_SQL}")  # noqa: F821
    dim = f"{SILVER}.dim_wallet"
    timings["build_s"] = round(time.time() - t, 1)

# COMMAND ----------

t = time.time()
s = spark.sql(f"""
    SELECT count(*) AS wallets, count(DISTINCT wallet_id) AS distinct_ids, count(DISTINCT address) AS distinct_addresses,
           min(wallet_id) AS min_id, max(wallet_id) AS max_id, sum(lifetime_fills) AS lifetime_fills,
           count_if(entry_fills IS NULL) AS null_entry_fills, count_if(entry_category IS NULL) AS null_entry_category,
           count_if(segment_at_entry IS NULL) AS null_segment_at_entry,
           count_if(entry_fills < first_day_fills) AS entry_fills_below_first_day,
           count_if(entry_fills > lifetime_fills) AS entry_fills_above_lifetime,
           count_if(datediff(last_date, first_date) <= 28 AND entry_fills != lifetime_fills) AS short_lived_entry_not_lifetime,
           count_if(entry_hours_active > entry_hours_available OR entry_hours_available < 1
                    OR entry_hours_available > 721) AS bad_entry_hours,
           count_if(NOT entry_window_complete) AS entry_window_incomplete,
           count_if(CAST(first_fill_ts AS DATE) != first_date) AS first_fill_ts_not_on_first_date,
           count_if(is_scripted_cohort) AS scripted_cohort,
           count_if(entry_category = 'Uncategorised') AS entry_uncategorised,
           sum(entry_fills) AS entry_fills
    FROM {dim}""").first().asDict()  # noqa: F821
by_cohort = [r.asDict() for r in spark.sql(f"""
    SELECT coalesce(cohort, 'none') AS cohort_or_none, count(*) AS wallets,
           count_if(entered_via_world_cup_market) AS via_world_cup_market,
           count_if(entered_via_us_election_2024_market) AS via_us_election_2024_market,
           count_if(segment_at_entry = 'market_maker') AS market_maker_at_entry,
           count_if(segment_at_entry = 'high_frequency_taker') AS hf_taker_at_entry,
           count_if(is_scripted_cohort) AS scripted, count_if(NOT entry_window_complete) AS entry_window_incomplete,
           round(100.0 * sum(CASE WHEN segment_at_entry != 'retail' THEN entry_fills END) / sum(entry_fills), 2)
               AS pct_entry_fills_automated,
           percentile_approx(entry_fills, 0.5) AS median_entry_fills
    FROM {dim} GROUP BY 1 ORDER BY 1""").collect()]  # noqa: F821
entry_category = [r.asDict() for r in spark.sql(f"""
    SELECT cohort, entry_category, count(*) AS wallets FROM {dim} WHERE cohort IS NOT NULL
    GROUP BY 1, 2 ORDER BY 1, 3 DESC""").collect()]  # noqa: F821
timings["checks_s"] = round(time.time() - t, 1)

problems = []
if s["distinct_ids"] != s["wallets"] or s["distinct_addresses"] != s["wallets"]:
    problems.append(f"wallets {s['wallets']}, ids {s['distinct_ids']}, addresses {s['distinct_addresses']}")
for check in ("null_entry_fills", "null_entry_category", "null_segment_at_entry", "entry_fills_below_first_day",
              "entry_fills_above_lifetime", "short_lived_entry_not_lifetime", "bad_entry_hours",
              "first_fill_ts_not_on_first_date"):
    if s[check]:
        problems.append(f"{check} = {s[check]}")
if not pilot:
    if (s["wallets"], s["min_id"], s["max_id"]) != (EXPECTED["wallets"], 1, EXPECTED["wallets"]):
        problems.append(f"wallets {s['wallets']}, ids {s['min_id']}..{s['max_id']}")
    if s["lifetime_fills"] != EXPECTED["lifetime_fills"]:
        problems.append(f"lifetime fills {s['lifetime_fills']}")
    for c in by_cohort:
        if c["cohort_or_none"] in EXPECTED and c["wallets"] != EXPECTED[c["cohort_or_none"]]:
            problems.append(f"cohort {c['cohort_or_none']}: {c['wallets']} wallets, expected {EXPECTED[c['cohort_or_none']]}")

result = {"pilot_first_month": pilot, "timings": timings, "summary": s, "by_cohort": by_cohort,
          "entry_category_by_cohort": entry_category, "problems": problems}
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
if not pilot:
    spark.sql(f"DROP TABLE IF EXISTS {SILVER}.wallet_base")  # noqa: F821 (stage 1 staging, not needed here)
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
