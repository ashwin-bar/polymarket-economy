# Databricks notebook source
# Data quality and reconciliation pass over the whole silver layer (docs/SILVER_PLAN.md §6).
# Per-month checks against bronze were made when each fact month was loaded (dq_results,
# load_log); this pass checks the finished tables against each other and against the known
# totals. Every test is appended to polymarket_silver.dq_results (month_start null) and a hard
# failure raises. Outputs are counts only.
#
# Also returns new wallets per UTC day, which the laptop driver compares with test C's saved
# daily table (eda\C_wallet_identity_daily.csv).

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
S = "workspace.polymarket_silver"
TRADES_ROWS, FILLS_ROWS, WALLETS, MARKETS = 1_266_885_846, 2_026_673_731, 3_192_135, 4_049_900
EXCHANGE_CONTRACTS = ["0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", "0xc5d563a36ae78145c45a50134d48a1215220f80a",
                      "0xe111180000d2663c0091e4f400237545b87b996b", "0xe2222d279d744050d28e00520010520000310f59"]
tests, timings = [], {}  # tests: (table, test, expected, actual, hard)


def one(sql: str) -> dict:
    return spark.sql(sql).first().asDict()  # noqa: F821


def check(table: str, name: str, expected, actual, hard: bool = True) -> None:
    tests.append((table, name, expected, actual, hard))


# COMMAND ----------

t = time.time()
a = one(f"""SELECT count(*) AS n, count_if(anchor_block % 1000 != 0) AS off_grid,
                   count_if(next_ts_unix <= ts_unix) AS not_increasing,
                   (max(anchor_block) - min(anchor_block)) / 1000 + 1 AS span FROM {S}.block_anchor""")
check("block_anchor", "rows", 59_074, a["n"])
check("block_anchor", "anchors exactly 1,000 blocks apart", 59_074, int(a["span"]) if not a["off_grid"] else -1)
check("block_anchor", "time strictly increasing", 0, a["not_increasing"])

d = one(f"""SELECT count(*) AS n, count(DISTINCT date_utc) AS days, count_if(end_block <= first_block) AS bad
            FROM {S}.dim_date""")
check("dim_date", "one row per UTC day", 1414, d["days"] if d["n"] == d["days"] else -1)
check("dim_date", "block ranges increasing", 0, d["bad"])

# Fact tables: totals, months, earlier per-month results, and referential integrity.
log = one(f"""SELECT count_if(table_name = 'trades') AS trades_months, count_if(table_name = 'wallet_fills') AS fills_months,
                     sum(CASE WHEN table_name = 'trades' THEN rows END) AS trades_rows,
                     sum(CASE WHEN table_name = 'wallet_fills' THEN rows END) AS fills_rows FROM {S}.load_log""")
tr = one(f"""SELECT count(*) AS n, count(DISTINCT month_start) AS months, count_if(market_id IS NULL OR ts_utc IS NULL) AS nulls,
                    count_if(maker_wallet_id IS NULL OR taker_wallet_id IS NULL) AS null_wallets,
                    count_if(date_trunc('month', ts_utc) != month_start) AS ts_outside_month FROM {S}.trades""")
check("trades", "rows = bronze total", TRADES_ROWS, tr["n"])
check("trades", "rows = load_log total", log["trades_rows"], tr["n"])
check("trades", "months loaded", 48, tr["months"] if log["trades_months"] == 48 else -1)
check("trades", "null market_id or ts_utc", 0, tr["nulls"])
check("trades", "null maker or taker wallet_id", 0, tr["null_wallets"])
check("trades", "ts_utc outside its month", 0, tr["ts_outside_month"])
check("trades", "market_id not in dim_market",
      0, one(f"SELECT count(*) AS n FROM {S}.trades t LEFT ANTI JOIN {S}.dim_market m USING (market_id)")["n"])

wf = one(f"""SELECT count(*) AS n, count(DISTINCT month_start) AS months, count_if(wallet_id IS NULL) AS null_wallet,
                    count_if(role NOT IN ('maker', 'taker') OR direction NOT IN ('BUY', 'SELL')) AS bad_values,
                    count_if(role = 'maker') AS maker_rows, count_if(date_trunc('month', ts_utc) != month_start) AS ts_outside_month
             FROM {S}.wallet_fills""")
check("wallet_fills", "rows = bronze total", FILLS_ROWS, wf["n"])
check("wallet_fills", "rows = load_log total", log["fills_rows"], wf["n"])
check("wallet_fills", "months loaded", 48, wf["months"] if log["fills_months"] == 48 else -1)
check("wallet_fills", "null wallet_id", 0, wf["null_wallet"])
check("wallet_fills", "role and direction values", 0, wf["bad_values"])
check("wallet_fills", "ts_utc outside its month", 0, wf["ts_outside_month"])
check("wallet_fills", "maker rows = trades rows (concern 7)", tr["n"], wf["maker_rows"])
check("wallet_fills", "months where maker rows != trades rows", 0, one(f"""
    SELECT count(*) AS n FROM (SELECT month_start, count_if(role = 'maker') AS m FROM {S}.wallet_fills GROUP BY 1) f
    FULL JOIN (SELECT month_start, count(*) AS t FROM {S}.trades GROUP BY 1) x USING (month_start)
    WHERE f.m IS DISTINCT FROM x.t""")["n"])
check("wallet_fills", "market_id not in dim_market",
      0, one(f"SELECT count(*) AS n FROM {S}.wallet_fills f LEFT ANTI JOIN {S}.dim_market m USING (market_id)")["n"])
check("dq_results", "failed per-month checks from the fact load", 0,
      one(f"SELECT count(*) AS n FROM {S}.dq_results WHERE NOT passed AND month_start IS NOT NULL")["n"])
timings["facts_s"] = round(time.time() - t, 1)

# COMMAND ----------

t = time.time()
w = one(f"""
    SELECT count(*) AS n, count(DISTINCT address) AS addresses, count(DISTINCT wallet_id) AS ids, max(wallet_id) AS max_id,
           count_if(address IN ({", ".join(repr(c) for c in EXCHANGE_CONTRACTS)})) AS exchange_contracts,
           count_if(first_date > last_date) AS first_after_last, sum(lifetime_fills) AS lifetime_fills,
           count_if(entry_category IS NULL OR segment_at_entry IS NULL OR entry_fills IS NULL) AS null_stage2,
           count_if(cohort = 'world_cup_2026') AS world_cup, count_if(cohort = 'us_election_2024') AS election,
           count_if(cohort = 'benchmark_q1_2026') AS q1_2026, count_if(is_scripted_cohort) AS scripted
    FROM {S}.dim_wallet""")
check("dim_wallet", "rows", WALLETS, w["n"])
check("dim_wallet", "address unique", w["n"], w["addresses"])
check("dim_wallet", "wallet_id unique and 1..n", w["n"], w["ids"] if w["max_id"] == w["n"] else -1)
check("dim_wallet", "exchange contract addresses", 0, w["exchange_contracts"])
check("dim_wallet", "first_date after last_date", 0, w["first_after_last"])
check("dim_wallet", "lifetime fills sum = wallet_fills rows", wf["n"], w["lifetime_fills"])
check("dim_wallet", "null entry category, entry fills or segment at entry", 0, w["null_stage2"])
check("dim_wallet", "World Cup cohort (11 Jun-19 Jul 2026)", 226_923, w["world_cup"])
check("dim_wallet", "election cohort (28 Sep-5 Nov 2024)", 218_361, w["election"])
check("dim_wallet", "Q1 2026 cohort", 713_066, w["q1_2026"])
check("trades", "maker or taker wallet_id not in dim_wallet", 0, one(f"""
    SELECT count(*) AS n FROM {S}.trades t WHERE t.maker_wallet_id > {WALLETS} OR t.taker_wallet_id > {WALLETS}
                                               OR t.maker_wallet_id < 1 OR t.taker_wallet_id < 1""")["n"])
check("wallet_fills", "wallet_id not in dim_wallet", 0,
      one(f"SELECT count(*) AS n FROM {S}.wallet_fills WHERE wallet_id > {WALLETS} OR wallet_id < 1")["n"])

# wallet_month against wallet_fills (through the load log) and against dim_wallet, wallet by wallet.
wm = one(f"""SELECT count(*) AS n, count(DISTINCT wallet_id, month_start) AS keys, sum(fills) AS fills,
                    count_if(segment IS NULL OR segment_rule_version != 'v1') AS bad_segment,
                    count_if(hours_active > hours_available) AS bad_hours,
                    count_if(segment != CASE WHEN (fills >= 1000 AND hours_share >= 0.25) OR fills >= 10000
                                             THEN CASE WHEN maker_share >= 0.5 THEN 'market_maker' ELSE 'high_frequency_taker' END
                                             ELSE 'retail' END) AS segment_not_rule_v1
             FROM {S}.wallet_month""")
check("wallet_month", "key (wallet_id, month_start) unique", wm["n"], wm["keys"])
check("wallet_month", "fills sum = wallet_fills rows", wf["n"], wm["fills"])
check("wallet_month", "null segment or not rule v1", 0, wm["bad_segment"] + wm["segment_not_rule_v1"])
check("wallet_month", "hours_active above hours_available", 0, wm["bad_hours"])
check("wallet_month", "months where fills != wallet_fills load_log", 0, one(f"""
    SELECT count(*) AS n FROM (SELECT month_start, sum(fills) AS f FROM {S}.wallet_month GROUP BY 1) m
    FULL JOIN (SELECT month_start, rows FROM {S}.load_log WHERE table_name = 'wallet_fills') l USING (month_start)
    WHERE m.f IS DISTINCT FROM l.rows""")["n"])
x = one(f"""
    SELECT count_if(m.fills IS DISTINCT FROM w.lifetime_fills) AS fills_differ,
           count_if(m.first_month IS DISTINCT FROM w.first_month) AS first_month_differs,
           count_if(abs(m.cash / nullif(w.lifetime_cash_traded, 0) - 1) > 1e-6) AS cash_differs
    FROM {S}.dim_wallet w FULL JOIN (
        SELECT wallet_id, sum(fills) AS fills, min(month_start) AS first_month, sum(cash_traded) AS cash
        FROM {S}.wallet_month GROUP BY wallet_id) m USING (wallet_id)""")
check("wallet_month", "wallets whose fills != dim_wallet lifetime_fills", 0, x["fills_differ"])
check("wallet_month", "wallets whose first month != dim_wallet first_month", 0, x["first_month_differs"])
check("wallet_month", "wallets whose cash traded != dim_wallet lifetime cash", 0, x["cash_differs"])
c7 = one(f"""SELECT count_if(month_start = DATE '2024-12-01') AS dec_2024, count_if(month_start = DATE '2026-10-01') AS oct_2026
             FROM {S}.wallet_month""")
check("wallet_month", "active wallets December 2024 (concern 7)", 358_846, c7["dec_2024"])
check("wallet_month", "active wallets October 2026 (concern 7)", 83_592, c7["oct_2026"])
timings["wallets_s"] = round(time.time() - t, 1)

# COMMAND ----------

t = time.time()
m = one(f"""
    SELECT count(*) AS n, count(DISTINCT market_id) AS ids, count_if(category IS NULL OR subcategory IS NULL) AS null_category,
           sum(fills) AS fills, count(DISTINCT event_id) AS events,
           round(100.0 * count_if(volume_recon_status = 'within_1pct' AND last_fill_ts < TIMESTAMP '2026-04-28')
                 / count_if(volume_recon_status IN ('within_1pct', 'short', 'over') AND last_fill_ts < TIMESTAMP '2026-04-28'), 1) AS pre,
           round(100.0 * count_if(volume_recon_status = 'within_1pct' AND first_fill_ts >= TIMESTAMP '2026-04-29')
                 / count_if(volume_recon_status IN ('within_1pct', 'short', 'over') AND first_fill_ts >= TIMESTAMP '2026-04-29'), 1) AS post
    FROM {S}.dim_market""")
check("dim_market", "rows", MARKETS, m["n"])
check("dim_market", "market_id unique", m["n"], m["ids"])
check("dim_market", "null category or subcategory", 0, m["null_category"])
check("dim_market", "fills sum = trades rows", tr["n"], m["fills"])
check("dim_market", "pre-cutover markets within 1% of markets.volume, % (V2 follow-up: 92.6)", 92.6, float(m["pre"]))
check("dim_market", "post-cutover markets within 1% of markets.volume, % (V2 follow-up: 96.6)", 96.6, float(m["post"]))
check("dim_event", "rows = events in dim_market", m["events"], one(f"SELECT count(*) AS n FROM {S}.dim_event")["n"])
mm = one(f"SELECT sum(fills) AS fills, count(DISTINCT market_id, month_start) AS keys, count(*) AS n FROM {S}.market_month")
check("market_month", "fills sum = trades rows", tr["n"], mm["fills"])
check("market_month", "key (market_id, month_start) unique", mm["n"], mm["keys"])
timings["markets_s"] = round(time.time() - t, 1)

# COMMAND ----------

now = datetime.now(timezone.utc)
failed = [f"{table}: {name}: expected {exp}, got {act}" for table, name, exp, act, hard in tests if hard and exp != act]
spark.createDataFrame(  # noqa: F821
    [(table, None, name, str(exp), str(act), exp == act, hard, now) for table, name, exp, act, hard in tests],
    "table_name string, month_start date, test string, expected string, actual string, passed boolean, hard boolean, run_at timestamp",
).write.mode("append").saveAsTable(f"{S}.dq_results")

sizes = {}
for name in ("trades", "wallet_fills", "wallet_month", "dim_wallet", "dim_market", "dim_event", "market_month", "dim_date",
             "block_anchor"):
    detail = spark.sql(f"DESCRIBE DETAIL {S}.{name}").first()  # noqa: F821
    sizes[name] = {"bytes": detail["sizeInBytes"], "files": detail["numFiles"]}
result = {
    "tests": [{"table": table, "test": name, "expected": exp, "actual": act, "passed": exp == act}
              for table, name, exp, act, hard in tests],
    "tests_run": len(tests), "tests_failed": len(failed), "failed": failed, "timings": timings, "table_sizes": sizes,
    "tables_in_schema": sorted(r["tableName"] for r in spark.sql(f"SHOW TABLES IN {S}").collect()),  # noqa: F821
    "scripted_cohort_wallets": w["scripted"],
    "new_wallets_by_day": {str(r["first_date"]): r["n"] for r in spark.sql(
        f"SELECT first_date, count(*) AS n FROM {S}.dim_wallet GROUP BY 1").collect()},  # noqa: F821
}
print(json.dumps({k: v for k, v in result.items() if k != "new_wallets_by_day"}, indent=2, default=str))
if failed:
    raise RuntimeError("FAILED: " + json.dumps(failed, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
