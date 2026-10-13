# Databricks notebook source
# Silver wallet dimension, stage 1 (docs/SILVER_PLAN.md): one row per wallet over the whole
# bronze users history, with a stable integer wallet_id that the fact tables carry instead of
# the address. Addresses live only in this table.
#
# Stage 2 (entry category, 30-day entry profile, segment at entry) is added later, once the
# market dimension and the segment thresholds exist; wallet_id does not change because it is
# assigned in a fixed order (first fill's block, log index, then address).
#
# A fill's UTC day comes from the block-anchor equi-join (time interpolated inside the
# 1,000-block bucket) and is clamped to the bronze month, exactly as in
# dbx_silver_fact_month.py, so days agree between this table and the fact tables. (A range join
# to dim_date on an expression stalled in a single task on 13 Oct 2026 and is not used.)
#
# Built in two statements so each cost is visible:
#   1. polymarket_silver.wallet_base: one row per wallet (the only pass over users)
#   2. polymarket_silver.dim_wallet: wallet_id by row_number over those rows, plus flags
#
# Parameter: pilot_years = e.g. "2022,2023" runs both statements on those bronze years only and
# writes nothing (timing and plan check). Empty = full history, tables written.

# COMMAND ----------

import json
import time

from pyspark.sql import functions as F

dbutils.widgets.text("pilot_years", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
pilot_years = [int(y) for y in dbutils.widgets.get("pilot_years").split(",") if y]  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
EXCHANGE_CONTRACTS = ["0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", "0xc5d563a36ae78145c45a50134d48a1215220f80a",
                      "0xe111180000d2663c0091e4f400237545b87b996b", "0xe2222d279d744050d28e00520010520000310f59"]
EXPECTED_WALLETS, EXPECTED_FILLS = 3_192_135, 2_026_673_731
timings = {}

users = (spark.read.parquet("/Volumes/workspace/polymarket/bronze/users")  # noqa: F821
         .select("address", F.col("block_number").cast("long").alias("block_number"),
                 F.col("log_index").cast("long").alias("log_index"), "usd_amount",
                 F.col("market_id").cast("string").alias("market_id"), "year", "month"))
if pilot_years:
    users = users.where(F.col("year").isin(pilot_years))
users.createOrReplaceTempView("users_bronze")

# COMMAND ----------

BASE_SQL = f"""
    WITH timed AS (
        SELECT /*+ BROADCAST(a) */ u.address, u.block_number, u.log_index, u.market_id, u.usd_amount,
               make_date(u.year, u.month, 1) AS month_start,
               timestamp_seconds(a.ts_unix + (u.block_number - a.anchor_block)
                                 * (a.next_ts_unix - a.ts_unix) / 1000.0) AS raw_ts
        FROM users_bronze u JOIN {SILVER}.block_anchor a
          ON a.anchor_block = CAST(floor(u.block_number / 1000) * 1000 AS BIGINT)),
    wallet_days AS (
        SELECT address,
               CAST(greatest(least(raw_ts, CAST(add_months(month_start, 1) AS TIMESTAMP) - INTERVAL 1 SECOND),
                             CAST(month_start AS TIMESTAMP)) AS DATE) AS date_utc,
               count(*) AS fills, sum(usd_amount) AS cash,
               min(struct(block_number, log_index, market_id)) AS first_fill
        FROM timed GROUP BY 1, 2)
    SELECT address, min(first_fill) AS first_fill, min(date_utc) AS first_date, max(date_utc) AS last_date,
           sum(fills) AS lifetime_fills, sum(cash) AS lifetime_cash_traded,
           min_by(cash, date_utc) AS first_day_cash, min_by(fills, date_utc) AS first_day_fills
    FROM wallet_days GROUP BY address"""


def dim_sql(base: str) -> str:
    return f"""
    WITH flagged AS (
        SELECT *,
               -- Scripted cohort (brief §7, decision 26): 20+ wallets first seen the same day with an
               -- identical first-day cash amount of $100 or more that is not a whole multiple of $50.
               count(*) OVER (PARTITION BY first_date, CAST(round(first_day_cash * 100) AS BIGINT)) AS same_day_same_amount
        FROM {base})
    SELECT CAST(row_number() OVER (ORDER BY f.first_fill.block_number, f.first_fill.log_index, f.address)
                AS BIGINT) AS wallet_id,
           f.address,
           timestamp_seconds(a.ts_unix + (f.first_fill.block_number - a.anchor_block)
                             * (a.next_ts_unix - a.ts_unix) / 1000.0) AS first_fill_ts,
           f.first_fill.block_number AS first_block, f.first_fill.log_index AS first_log_index,
           f.first_date, CAST(date_trunc('month', f.first_date) AS DATE) AS first_month,
           f.first_fill.market_id AS first_market_id,
           f.last_date, f.lifetime_fills, f.lifetime_cash_traded, f.first_day_cash, f.first_day_fills,
           (f.same_day_same_amount >= 20 AND f.first_day_cash >= 100
            AND CAST(round(f.first_day_cash * 100) AS BIGINT) % 5000 != 0) AS is_scripted_cohort,
           f.first_date < DATE '2023-03-01' AS is_left_censored,
           f.last_date = DATE '2026-04-27' AS last_seen_v2_eve
    FROM flagged f LEFT JOIN {SILVER}.block_anchor a
      ON a.anchor_block = CAST(floor(f.first_fill.block_number / 1000) * 1000 AS BIGINT)"""


if pilot_years:
    # Nothing is written: statement 1 is timed by counting it, then both statements run together.
    t = time.time()
    base_rows = spark.sql(BASE_SQL).count()  # noqa: F821
    timings["statement_1_s"] = round(time.time() - t, 1)
    spark.sql(BASE_SQL).createOrReplaceTempView("wallet_base_pilot")  # noqa: F821
    spark.sql(dim_sql("wallet_base_pilot")).createOrReplaceTempView("dim_wallet_view")  # noqa: F821
    dim = "dim_wallet_view"
else:
    t = time.time()
    spark.sql(f"CREATE OR REPLACE TABLE {SILVER}.wallet_base AS {BASE_SQL}")  # noqa: F821
    timings["statement_1_s"] = round(time.time() - t, 1)
    base_rows = spark.table(f"{SILVER}.wallet_base").count()  # noqa: F821
    t = time.time()
    spark.sql(f"CREATE OR REPLACE TABLE {SILVER}.dim_wallet AS {dim_sql(f'{SILVER}.wallet_base')}")  # noqa: F821
    timings["statement_2_s"] = round(time.time() - t, 1)
    dim = f"{SILVER}.dim_wallet"

# COMMAND ----------

t = time.time()
s = spark.sql(f"""
    SELECT count(*) AS wallets, count(DISTINCT address) AS distinct_addresses,
           min(wallet_id) AS min_id, max(wallet_id) AS max_id,
           sum(lifetime_fills) AS lifetime_fills, round(sum(lifetime_cash_traded), 0) AS lifetime_cash_traded,
           count_if(first_fill_ts IS NULL) AS null_first_fill_ts,
           count_if(CAST(first_fill_ts AS DATE) != first_date) AS first_ts_not_on_first_date,
           count_if(first_date > last_date) AS first_after_last,
           count_if(address != lower(address)) AS not_lower_case,
           count_if(address IN ({", ".join(repr(c) for c in EXCHANGE_CONTRACTS)})) AS exchange_contracts,
           count_if(is_scripted_cohort) AS scripted_cohort, count_if(is_left_censored) AS left_censored,
           count_if(last_seen_v2_eve) AS last_seen_v2_eve,
           count_if(first_date >= DATE '2026-05-04') AS first_seen_after_launch,
           count_if(first_date BETWEEN DATE '2026-06-11' AND DATE '2026-07-19') AS world_cup_cohort,
           count_if(first_date BETWEEN DATE '2024-09-28' AND DATE '2024-11-05') AS election_cohort,
           count_if(first_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31') AS q1_2026_cohort
    FROM {dim}""").first().asDict()  # noqa: F821
timings["statement_2_and_checks_s" if pilot_years else "checks_s"] = round(time.time() - t, 1)
scripted_by_month = [r.asDict() for r in spark.sql(f"""
    SELECT date_format(first_date, 'yyyy-MM') AS first_month, count(*) AS wallets,
           round(sum(first_day_cash) / 1e6, 2) AS first_day_cash_m
    FROM {dim} WHERE is_scripted_cohort GROUP BY 1 ORDER BY 1""").collect()]  # noqa: F821

problems = []
if s["distinct_addresses"] != s["wallets"] or base_rows != s["wallets"]:
    problems.append(f"wallets {s['wallets']}, distinct {s['distinct_addresses']}, base rows {base_rows}")
if (s["min_id"], s["max_id"]) != (1, s["wallets"]):
    problems.append(f"wallet_id range {s['min_id']}..{s['max_id']}")
if not pilot_years:
    if s["wallets"] != EXPECTED_WALLETS:
        problems.append(f"wallets {s['wallets']}, expected {EXPECTED_WALLETS}")
    if s["lifetime_fills"] != EXPECTED_FILLS:
        problems.append(f"lifetime fills {s['lifetime_fills']} != {EXPECTED_FILLS}")
for check in ("null_first_fill_ts", "first_after_last", "exchange_contracts"):
    if s[check]:
        problems.append(f"{check} = {s[check]}")

result = {"pilot_years": pilot_years, "timings": timings, "summary": s,
          "scripted_by_first_month": scripted_by_month, "problems": problems}
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
