-- Methodology tables (docs/GOLD_PLAN.md §3): the data-quality summary and the suppression log.

-- Every test in one place: the per-month checks made when each fact month was loaded (rolled up
-- to one row per test), the whole-layer silver pass (latest run), and the gold tests.
CREATE OR REPLACE TABLE workspace.polymarket_gold.dq_summary AS
SELECT 'silver, per month' AS layer, table_name, test,
       concat(count(*), ' months') AS expected,
       concat(count_if(passed), ' months passed') AS actual,
       count_if(passed) = count(*) AS passed
FROM workspace.polymarket_silver.dq_results
WHERE month_start IS NOT NULL
GROUP BY table_name, test
UNION ALL
SELECT 'silver, whole layer', table_name, test, expected, actual, passed
FROM workspace.polymarket_silver.dq_results
WHERE month_start IS NULL
  AND run_at = (SELECT max(run_at) FROM workspace.polymarket_silver.dq_results WHERE month_start IS NULL)
UNION ALL
SELECT 'gold', table_name, test, expected, actual, passed
FROM workspace.polymarket_gold.dq_gold
;

-- What small-cell suppression removed. The 'ALL' rows of the cube tables are exact; this shows
-- how much of each table's detail is missing at the finest grain, and how many values inside
-- kept rows were nulled.
CREATE OR REPLACE TABLE workspace.polymarket_gold.suppression_log AS
SELECT 'weekly_kpi' AS table_name, count(*) AS rows_kept,
       'cash traded visible at the finest grain (all four dimensions specific), % of total' AS coverage_measure,
       round(100.0 * sum(CASE WHEN activity_status != 'ALL' AND segment != 'ALL' AND scripted != 'ALL' AND category != 'ALL' THEN cash_traded END)
             / sum(CASE WHEN activity_status = 'ALL' AND segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL' THEN cash_traded END), 3) AS coverage_pct,
       count_if(whale_wallets_in_segment IS NULL) + count_if(whale_wallets_global IS NULL) AS values_nulled,
       'whale counts of 1-9 and their cash; 3 weeks of Nov-Dec 2022 with under 10 active wallets are absent' AS note
FROM workspace.polymarket_gold.weekly_kpi
UNION ALL
SELECT 'cohort_retention', count(*),
       'cohort wallets visible at the finest grain, % of all wallets (monthly cohorts, week 0)',
       round(100.0 * sum(CASE WHEN cohort_type = 'month' AND week_n = 0 AND cohort != 'ALL' AND entry_category != 'ALL'
                                   AND segment_at_entry != 'ALL' AND scripted != 'ALL' THEN cohort_wallets END)
             / max(CASE WHEN cohort_type = 'month' AND week_n = 0 AND cohort = 'ALL' AND entry_category = 'ALL'
                             AND segment_at_entry = 'ALL' AND scripted = 'ALL' THEN cohort_wallets END), 3),
       count_if(active_wallets IS NULL), 'active or observable counts of 1-9, with their fills and cash'
FROM workspace.polymarket_gold.cohort_retention
UNION ALL
SELECT 'cohort_retention_monthly', count(*),
       'cohort wallets visible at the finest grain, % of all wallets (monthly cohorts, period 0)',
       round(100.0 * sum(CASE WHEN cohort_type = 'month' AND month_n = 0 AND cohort != 'ALL' AND entry_category != 'ALL'
                                   AND segment_at_entry != 'ALL' AND scripted != 'ALL' THEN cohort_wallets END)
             / max(CASE WHEN cohort_type = 'month' AND month_n = 0 AND cohort = 'ALL' AND entry_category = 'ALL'
                             AND segment_at_entry = 'ALL' AND scripted = 'ALL' THEN cohort_wallets END), 3),
       count_if(active_wallets IS NULL), 'active or observable counts of 1-9, with their fills and cash'
FROM workspace.polymarket_gold.cohort_retention_monthly
UNION ALL
SELECT 'cohort_category_transition', count(*),
       'base wallets visible at the finest grain, % of all observable wallets (monthly cohorts)',
       round(100.0 * sum(CASE WHEN cohort_type = 'month' AND traded_category = 'Any category' AND cohort != 'ALL'
                                   AND entry_category != 'ALL' AND segment_at_entry != 'ALL' THEN base_wallets END)
             / max(CASE WHEN cohort_type = 'month' AND cohort = 'ALL' AND entry_category = 'ALL' AND segment_at_entry = 'ALL'
                        THEN base_wallets END), 3),
       count_if(wallets IS NULL), 'wallet counts of 1-9, with their fills and cash'
FROM workspace.polymarket_gold.cohort_category_transition
UNION ALL
SELECT 'listing_scorecard', count(*),
       'notional of listings whose trader count is shown exactly, % of total',
       round(100.0 * sum(CASE WHEN traders IS NOT NULL THEN notional_shares END) / sum(notional_shares), 3),
       count_if(traders IS NULL) + count_if(wallets_acquired IS NULL),
       'trader and acquired counts of 1-9 (the band column still shows 1-9)'
FROM workspace.polymarket_gold.listing_scorecard
UNION ALL
SELECT 'series_month', count(*),
       'notional of series-months whose trader count is shown, % of total',
       round(100.0 * sum(CASE WHEN traders IS NOT NULL THEN notional_shares END) / sum(notional_shares), 3),
       count_if(traders IS NULL) + count_if(wallets_acquired IS NULL), 'trader and acquired counts of 1-9'
FROM workspace.polymarket_gold.series_month
