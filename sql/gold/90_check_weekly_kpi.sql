-- Checks on gold.weekly_kpi and gold.weekly_market_volume against silver (docs/GOLD_PLAN.md §7).
-- Statement 1 returns one row per test; statement 2 a digest of selected weeks for review.

WITH k AS (SELECT * FROM workspace.polymarket_gold.weekly_kpi),
top AS (SELECT * FROM k WHERE activity_status = 'ALL' AND segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL'),
silver AS (
    SELECT (SELECT count(*) FROM workspace.polymarket_silver.trades) AS fills,
           (SELECT sum(token_amount) FROM workspace.polymarket_silver.trades) AS notional,
           (SELECT sum(fills) FROM workspace.polymarket_silver.wallet_month) AS wallet_fills,
           (SELECT sum(cash_traded) FROM workspace.polymarket_silver.wallet_month) AS cash_traded,
           (SELECT sum(cash_staked) FROM workspace.polymarket_silver.wallet_month) AS cash_staked
),
weekly_wallets AS (
    SELECT CAST(date_trunc('week', date_utc) AS DATE) AS week_start, count(DISTINCT wallet_id) AS wallets
    FROM workspace.polymarket_silver.wallet_fills GROUP BY 1
),
new_by_week AS (
    SELECT CAST(date_trunc('week', first_date) AS DATE) AS week_start, count(*) AS wallets
    FROM workspace.polymarket_silver.dim_wallet GROUP BY 1
),
mv AS (SELECT sum(fills) AS fills, sum(notional_shares) AS notional FROM workspace.polymarket_gold.weekly_market_volume)
SELECT 'rows in weekly_kpi' AS test, '<= 77568' AS expected, CAST(count(*) AS STRING) AS actual FROM k
UNION ALL SELECT 'weeks with an all-ALL row', '202', CAST(count(*) AS STRING) FROM top
UNION ALL SELECT 'rows with fewer than 10 active wallets', '0', CAST(count_if(active_wallets < 10) AS STRING) FROM k
UNION ALL SELECT 'fills, all-ALL rows = trades rows', CAST((SELECT fills FROM silver) AS STRING), CAST(CAST(sum(fills) AS BIGINT) AS STRING) FROM top
UNION ALL SELECT 'wallet_fills, all-ALL rows = wallet_fills rows', CAST((SELECT wallet_fills FROM silver) AS STRING), CAST(sum(wallet_fills) AS STRING) FROM top
UNION ALL SELECT 'notional, all-ALL rows vs trades (relative difference)', '< 1e-9', CAST(abs(sum(notional_shares) / (SELECT notional FROM silver) - 1) AS STRING) FROM top
UNION ALL SELECT 'cash traded, all-ALL rows vs wallet_month (relative difference)', '< 1e-9', CAST(abs(sum(cash_traded) / (SELECT cash_traded FROM silver) - 1) AS STRING) FROM top
UNION ALL SELECT 'cash staked, all-ALL rows vs wallet_month (relative difference)', '< 1e-9', CAST(abs(sum(cash_staked) / (SELECT cash_staked FROM silver) - 1) AS STRING) FROM top
UNION ALL SELECT 'weeks where active wallets != distinct wallets in wallet_fills', '0',
       CAST(count_if(t.active_wallets IS DISTINCT FROM w.wallets) AS STRING) FROM weekly_wallets w FULL JOIN top t USING (week_start)
UNION ALL SELECT 'weeks where new wallets != dim_wallet first fills (weeks with 10+ new)', '0',
       CAST(count_if(n.wallets >= 10 AND k.active_wallets IS DISTINCT FROM n.wallets) AS STRING)
       FROM new_by_week n LEFT JOIN (SELECT * FROM k WHERE activity_status = 'new' AND segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL') k USING (week_start)
UNION ALL SELECT 'new wallets over all weeks (3,192,135 less weeks under 10)', CAST((SELECT sum(wallets) FROM new_by_week WHERE wallets >= 10) AS STRING),
       CAST(sum(active_wallets) AS STRING) FROM k WHERE activity_status = 'new' AND segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL'
UNION ALL SELECT 'weeks where new + returning + resurrected != active (all three present)', '0',
       CAST(count_if(parts = 3 AND split_sum != total) AS STRING)
       FROM (SELECT week_start, count_if(activity_status != 'ALL') AS parts,
                    sum(CASE WHEN activity_status != 'ALL' THEN active_wallets END) AS split_sum,
                    max(CASE WHEN activity_status = 'ALL' THEN active_wallets END) AS total
             FROM k WHERE segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL' GROUP BY week_start)
UNION ALL SELECT 'weeks where global whales != ceil(1% of active) (weeks with 10+ whales)', '0',
       CAST(count_if(whale_wallets_global IS NOT NULL AND whale_wallets_global != ceil(0.01 * active_wallets)) AS STRING) FROM top
UNION ALL SELECT 'weekly_market_volume fills = trades rows', CAST((SELECT fills FROM silver) AS STRING), CAST((SELECT fills FROM mv) AS STRING)
UNION ALL SELECT 'weekly_market_volume notional vs trades (relative difference)', '< 1e-9',
       CAST(abs((SELECT notional FROM mv) / (SELECT notional FROM silver) - 1) AS STRING)
UNION ALL SELECT 'rows in weekly_market_volume', 'about 10,000', CAST((SELECT count(*) FROM workspace.polymarket_gold.weekly_market_volume) AS STRING)
UNION ALL SELECT 'wallet-weeks in staging with no wallet-side fills (dropped from the KPI)', '0',
       CAST((SELECT count(*) FROM (SELECT wallet_id, week_start FROM workspace.polymarket_silver.wallet_week_category
                                   GROUP BY 1, 2 HAVING min(first_month_in_week) IS NULL)) AS STRING)
;

SELECT week_start,
       max(CASE WHEN activity_status = 'ALL' THEN active_wallets END) AS active_wallets,
       max(CASE WHEN activity_status = 'new' THEN active_wallets END) AS new_wallets,
       max(CASE WHEN activity_status = 'returning' THEN active_wallets END) AS returning_wallets,
       max(CASE WHEN activity_status = 'resurrected' THEN active_wallets END) AS resurrected_wallets,
       CAST(max(CASE WHEN activity_status = 'ALL' THEN fills END) AS BIGINT) AS fills,
       round(max(CASE WHEN activity_status = 'ALL' THEN notional_shares END) / 1e6, 1) AS notional_m,
       round(max(CASE WHEN activity_status = 'ALL' THEN cash_traded END) / 1e6, 1) AS cash_traded_m,
       round(max(CASE WHEN activity_status = 'ALL' THEN cash_staked END) / 1e6, 1) AS cash_staked_m,
       round(max(CASE WHEN activity_status = 'ALL' THEN cash_traded / active_wallets END), 0) AS volume_per_trader,
       round(100 * max(CASE WHEN activity_status = 'ALL' THEN whale_cash_traded_global / cash_traded END), 1) AS whale_share_global_pct,
       round(100 * max(CASE WHEN activity_status = 'ALL' THEN whale_cash_traded_in_segment / cash_traded END), 1) AS whale_share_in_segment_pct
FROM workspace.polymarket_gold.weekly_kpi
WHERE segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL'
  AND week_start IN (DATE '2024-10-28', DATE '2024-11-04', DATE '2025-06-16', DATE '2026-01-19', DATE '2026-03-16',
                     DATE '2026-04-20', DATE '2026-04-27', DATE '2026-05-04', DATE '2026-06-15', DATE '2026-06-29',
                     DATE '2026-07-13', DATE '2026-08-17', DATE '2026-09-28')
GROUP BY week_start ORDER BY week_start
;

SELECT segment,
       active_wallets,
       round(100.0 * active_wallets / sum(active_wallets) OVER (), 2) AS pct_wallets,
       round(100.0 * fills / sum(fills) OVER (), 2) AS pct_fills,
       round(100.0 * notional_shares / sum(notional_shares) OVER (), 2) AS pct_notional,
       round(100.0 * cash_traded / sum(cash_traded) OVER (), 2) AS pct_cash_traded,
       round(cash_traded / active_wallets, 0) AS volume_per_trader,
       round(100 * whale_cash_traded_in_segment / cash_traded, 1) AS whale_share_in_segment_pct,
       round(100 * whale_cash_traded_global / cash_traded, 1) AS whale_share_global_pct
FROM workspace.polymarket_gold.weekly_kpi
WHERE week_start = DATE '2026-06-15' AND activity_status = 'ALL' AND scripted = 'ALL' AND category = 'ALL' AND segment != 'ALL'
ORDER BY segment
;

SELECT category, active_wallets,
       CAST(fills AS BIGINT) AS fills, round(notional_shares / 1e6, 1) AS notional_m, round(cash_traded / 1e6, 1) AS cash_traded_m
FROM workspace.polymarket_gold.weekly_kpi
WHERE week_start = DATE '2026-06-15' AND activity_status = 'ALL' AND scripted = 'ALL' AND segment = 'ALL'
ORDER BY notional_shares DESC
