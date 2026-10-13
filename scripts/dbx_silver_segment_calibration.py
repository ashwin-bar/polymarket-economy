# Databricks notebook source
# Calibration of the wallet-segment thresholds (docs/SILVER_PLAN.md §5, decision 11), read from
# polymarket_silver.wallet_month. Nothing is written; every output is an aggregate.
#
# Rule shape: automated = fills >= F in the month AND hours_share >= H; an automated wallet is a
# market maker if maker_share >= S, otherwise a high-frequency taker; everyone else is retail.
#
# For five months (Dec 2024, Mar, Jun, Jul and Sep 2026) and the grid
#   H in {0.25, 0.50, 0.75, 0.90} x F in {300, 1,000, 3,000} x S in {0.5, 0.7}
# it reports wallets, share of fills and share of cash traded per segment, and how many wallets
# keep their segment in the following month (not for Sep 2026: October has 4 days of data).
# Also the joint distribution the thresholds cut through, and the scripted-cohort flag's
# largest flagged and exempted first-day amounts (decision 26).

# COMMAND ----------

import json

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
MONTHS = ["2024-12-01", "2026-03-01", "2026-06-01", "2026-07-01", "2026-09-01"]
NO_NEXT_MONTH = "2026-09-01"
HOURS, FILLS, SPLITS = [0.25, 0.50, 0.75, 0.90], [300, 1000, 3000], [0.5, 0.7]
result = {"months": MONTHS, "grid": {"hours_share": HOURS, "fills": FILLS, "maker_split": SPLITS}}


def rows(sql: str) -> list[dict]:
    return [r.asDict() for r in spark.sql(sql).collect()]  # noqa: F821


month_list = ", ".join(f"DATE '{m}'" for m in MONTHS)
spark.createDataFrame([(h, f, s) for h in HOURS for f in FILLS for s in SPLITS],  # noqa: F821
                      "h double, f int, s double").createOrReplaceTempView("grid")
# Each wallet-month with the same wallet's next month alongside (null if it did not trade then).
spark.sql(f"""
    SELECT w.month_start, w.wallet_id, w.fills, w.maker_share, w.hours_share, w.cash_traded, w.days_active,
           n.wallet_id IS NOT NULL AS active_next, n.fills AS next_fills, n.maker_share AS next_maker_share,
           n.hours_share AS next_hours_share
    FROM {SILVER}.wallet_month w
    LEFT JOIN {SILVER}.wallet_month n ON n.wallet_id = w.wallet_id AND n.month_start = add_months(w.month_start, 1)
    WHERE w.month_start IN ({month_list})""").createOrReplaceTempView("wm")  # noqa: F821

result["month_totals"] = rows("""
    SELECT CAST(month_start AS STRING) AS month, count(*) AS wallets, sum(fills) AS fills,
           round(sum(cash_traded) / 1e6, 1) AS cash_traded_m,
           round(100.0 * count_if(active_next) / count(*), 2) AS pct_active_next_month
    FROM wm GROUP BY 1 ORDER BY 1""")

# COMMAND ----------

SEG = """CASE WHEN {p}fills >= g.f AND {p}hours_share >= g.h
              THEN CASE WHEN {p}maker_share >= g.s THEN 'mm' ELSE 'hft' END ELSE 'retail' END"""
spark.sql(f"""
    SELECT wm.*, g.h, g.f, g.s, {SEG.format(p='')} AS seg,
           CASE WHEN active_next THEN {SEG.format(p='next_')} END AS next_seg
    FROM wm CROSS JOIN grid g""").createOrReplaceTempView("cells")  # noqa: F821
result["grid_results"] = rows(f"""
    SELECT CAST(month_start AS STRING) AS month, h, f, s,
           count_if(seg = 'mm') AS mm_wallets, count_if(seg = 'hft') AS hft_wallets,
           round(100.0 * count_if(seg != 'retail') / count(*), 3) AS pct_wallets_automated,
           round(100.0 * sum(CASE WHEN seg = 'mm' THEN fills END) / sum(fills), 2) AS pct_fills_mm,
           round(100.0 * sum(CASE WHEN seg = 'hft' THEN fills END) / sum(fills), 2) AS pct_fills_hft,
           round(100.0 * sum(CASE WHEN seg = 'mm' THEN cash_traded END) / sum(cash_traded), 2) AS pct_cash_mm,
           round(100.0 * sum(CASE WHEN seg = 'hft' THEN cash_traded END) / sum(cash_traded), 2) AS pct_cash_hft,
           CASE WHEN month_start != DATE '{NO_NEXT_MONTH}' THEN
               round(100.0 * count_if(seg = 'mm' AND next_seg = 'mm') / nullif(count_if(seg = 'mm'), 0), 1) END AS pct_mm_same_next,
           CASE WHEN month_start != DATE '{NO_NEXT_MONTH}' THEN
               round(100.0 * count_if(seg = 'hft' AND next_seg = 'hft') / nullif(count_if(seg = 'hft'), 0), 1) END AS pct_hft_same_next,
           CASE WHEN month_start != DATE '{NO_NEXT_MONTH}' THEN
               round(100.0 * count_if(seg != 'retail' AND next_seg IN ('mm', 'hft')) / nullif(count_if(seg != 'retail'), 0), 1)
               END AS pct_automated_still_automated,
           CASE WHEN month_start != DATE '{NO_NEXT_MONTH}' THEN
               round(100.0 * count_if(seg != 'retail' AND active_next) / nullif(count_if(seg != 'retail'), 0), 1)
               END AS pct_automated_active_next,
           CASE WHEN month_start != DATE '{NO_NEXT_MONTH}' THEN
               round(100.0 * count_if(seg = 'retail' AND next_seg IN ('mm', 'hft')) / nullif(count_if(seg = 'retail' AND active_next), 0), 3)
               END AS pct_retail_active_next_turning_automated
    FROM cells GROUP BY month_start, h, f, s ORDER BY month_start, h, f, s""")

# COMMAND ----------

# The joint distribution of fills and hours_share, and maker share among busy wallets.
result["fills_by_hours_share"] = rows("""
    SELECT CAST(month_start AS STRING) AS month,
           CASE WHEN fills < 10 THEN 'a 1-9' WHEN fills < 100 THEN 'b 10-99' WHEN fills < 300 THEN 'c 100-299'
                WHEN fills < 1000 THEN 'd 300-999' WHEN fills < 3000 THEN 'e 1,000-2,999'
                WHEN fills < 10000 THEN 'f 3,000-9,999' ELSE 'g 10,000+' END AS fills_band,
           CASE WHEN hours_share < 0.10 THEN 'a <0.10' WHEN hours_share < 0.25 THEN 'b 0.10-0.25'
                WHEN hours_share < 0.50 THEN 'c 0.25-0.50' WHEN hours_share < 0.75 THEN 'd 0.50-0.75'
                WHEN hours_share < 0.90 THEN 'e 0.75-0.90' ELSE 'f 0.90+' END AS hours_share_band,
           count(*) AS wallets, round(100.0 * sum(fills) / sum(sum(fills)) OVER (PARTITION BY month_start), 2) AS pct_fills,
           round(100.0 * sum(cash_traded) / sum(sum(cash_traded)) OVER (PARTITION BY month_start), 2) AS pct_cash,
           round(avg(maker_share), 3) AS mean_maker_share
    FROM wm GROUP BY month_start, 2, 3 ORDER BY 1, 2, 3""")
result["maker_share_of_busy_wallets"] = rows("""
    SELECT CAST(month_start AS STRING) AS month, least(floor(maker_share * 10), 9) / 10 AS maker_share_from,
           count(*) AS wallets, round(100.0 * sum(fills) / sum(sum(fills)) OVER (PARTITION BY month_start), 2) AS pct_of_their_fills
    FROM wm WHERE fills >= 1000 AND hours_share >= 0.5 GROUP BY month_start, 2 ORDER BY 1, 2""")
# Short-lived wallets with a full hours_share: how much of the "automated" group they are.
result["automated_by_days_active"] = rows("""
    SELECT CAST(month_start AS STRING) AS month,
           CASE WHEN days_active <= 1 THEN 'a 1 day' WHEN days_active <= 3 THEN 'b 2-3 days'
                WHEN days_active <= 7 THEN 'c 4-7 days' ELSE 'd 8+ days' END AS days_band,
           count(*) AS wallets, round(100.0 * sum(fills) / sum(sum(fills)) OVER (PARTITION BY month_start), 2) AS pct_of_their_fills
    FROM wm WHERE fills >= 1000 AND hours_share >= 0.5 GROUP BY month_start, 2 ORDER BY 1, 2""")

# COMMAND ----------

# Scripted-cohort flag (decision 26): what it catches and what the $50-multiple exemption lets go.
spark.sql(f"""
    SELECT first_date, CAST(round(first_day_cash * 100) AS BIGINT) AS cents, count(*) AS wallets,
           count_if(is_scripted_cohort) AS flagged, round(sum(lifetime_cash_traded) / 1e6, 2) AS lifetime_cash_m
    FROM {SILVER}.dim_wallet WHERE first_day_cash >= 100 GROUP BY 1, 2 HAVING count(*) >= 20""").createOrReplaceTempView("same_amount")  # noqa: F821
result["scripted_flag"] = {
    "totals": rows("""
        SELECT cents % 5000 = 0 AS multiple_of_50, count(*) AS groups, sum(wallets) AS wallets, sum(flagged) AS flagged_wallets
        FROM same_amount GROUP BY 1 ORDER BY 1"""),
    "top_flagged": rows("""
        SELECT CAST(first_date AS STRING) AS first_date, cents / 100 AS first_day_cash, wallets, lifetime_cash_m
        FROM same_amount WHERE cents % 5000 != 0 ORDER BY wallets DESC LIMIT 15"""),
    "top_exempted": rows("""
        SELECT CAST(first_date AS STRING) AS first_date, cents / 100 AS first_day_cash, wallets, lifetime_cash_m
        FROM same_amount WHERE cents % 5000 = 0 ORDER BY wallets DESC LIMIT 15"""),
    "exempted_by_amount": rows("""
        SELECT cents / 100 AS first_day_cash, count(*) AS groups, sum(wallets) AS wallets
        FROM same_amount WHERE cents % 5000 = 0 GROUP BY 1 ORDER BY 3 DESC LIMIT 10"""),
}

print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
