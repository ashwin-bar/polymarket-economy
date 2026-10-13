-- gold.weekly_kpi (docs/GOLD_PLAN.md §3): weekly KPIs as a cube over activity status, segment,
-- scripted flag and category. Each of the four dimensions has an 'ALL' value, aggregated from
-- wallet-level staging before suppression, so every headline figure is exact. The dashboard
-- picks one value or 'ALL' per dimension and never sums rows of a dimension.
--
-- Per wallet and week:
--   activity_status  new          its first-ever fill is in this week
--                    returning    not new, and active in the previous week
--                    resurrected  not new, and not active in the previous week
--   segment          its monthly segment (rule v1) for the month of its first fill that week
--   whale flags      top CEIL(1%) of the week's active wallets by cash traded (all categories),
--                    ranked over all wallets (global) or within its segment; ties by wallet id
--
-- Measures: active_wallets (distinct wallets), wallet_fills (wallet sides), fills and
-- notional_shares (from trades, half to each side's wallet, so they sum to the headline),
-- cash_traded (by wallets, both sides), cash_staked (BUY fills only).
--
-- Suppression: a row with fewer than 10 active wallets is removed; whale counts of 1-9 are
-- nulled together with their cash. No wallet id leaves this statement.

CREATE OR REPLACE TABLE workspace.polymarket_gold.weekly_kpi AS
WITH wallet_week AS (
    SELECT wallet_id, week_start, min(first_month_in_week) AS segment_month,
           sum(wallet_fills) AS wallet_fills, sum(fills_attributed) AS fills,
           sum(notional_attributed) AS notional_shares, sum(cash_traded) AS cash_traded, sum(cash_staked) AS cash_staked
    FROM workspace.polymarket_silver.wallet_week_category
    GROUP BY wallet_id, week_start
),
labelled AS (
    SELECT ww.*,
           CASE WHEN ww.week_start = CAST(date_trunc('week', w.first_date) AS DATE) THEN 'new'
                WHEN lag(ww.week_start) OVER (PARTITION BY ww.wallet_id ORDER BY ww.week_start) = date_sub(ww.week_start, 7)
                     THEN 'returning'
                ELSE 'resurrected' END AS activity_status,
           wm.segment,
           CASE WHEN w.is_scripted_cohort THEN 'scripted' ELSE 'not scripted' END AS scripted
    FROM wallet_week ww
    JOIN workspace.polymarket_silver.dim_wallet w USING (wallet_id)
    JOIN workspace.polymarket_silver.wallet_month wm
      ON wm.wallet_id = ww.wallet_id AND wm.month_start = ww.segment_month
),
flagged AS (
    SELECT *,
           row_number() OVER (PARTITION BY week_start ORDER BY cash_traded DESC, wallet_id)
               <= ceil(0.01 * count(*) OVER (PARTITION BY week_start)) AS is_whale_global,
           row_number() OVER (PARTITION BY week_start, segment ORDER BY cash_traded DESC, wallet_id)
               <= ceil(0.01 * count(*) OVER (PARTITION BY week_start, segment)) AS is_whale_in_segment
    FROM labelled
),
-- One row per wallet, week and category (so count(*) is distinct wallets), plus one row per
-- wallet and week for category 'ALL'.
wallet_rows AS (
    SELECT c.week_start, f.activity_status, f.segment, f.scripted, c.category, f.is_whale_global, f.is_whale_in_segment,
           c.wallet_fills, c.fills_attributed AS fills, c.notional_attributed AS notional_shares, c.cash_traded, c.cash_staked
    FROM workspace.polymarket_silver.wallet_week_category c
    JOIN flagged f USING (wallet_id, week_start)
    UNION ALL
    SELECT week_start, activity_status, segment, scripted, 'ALL', is_whale_global, is_whale_in_segment,
           wallet_fills, fills, notional_shares, cash_traded, cash_staked
    FROM flagged
),
cube AS (
    SELECT week_start,
           coalesce(activity_status, 'ALL') AS activity_status,
           coalesce(segment, 'ALL') AS segment,
           coalesce(scripted, 'ALL') AS scripted,
           category,
           count(*) AS active_wallets,
           sum(wallet_fills) AS wallet_fills,
           sum(fills) AS fills,
           sum(notional_shares) AS notional_shares,
           sum(cash_traded) AS cash_traded,
           sum(cash_staked) AS cash_staked,
           count_if(is_whale_in_segment) AS whale_wallets_in_segment,
           sum(CASE WHEN is_whale_in_segment THEN cash_traded ELSE 0 END) AS whale_cash_traded_in_segment,
           count_if(is_whale_global) AS whale_wallets_global,
           sum(CASE WHEN is_whale_global THEN cash_traded ELSE 0 END) AS whale_cash_traded_global
    FROM wallet_rows
    GROUP BY week_start, category, CUBE (activity_status, segment, scripted)
)
SELECT week_start, activity_status, segment, scripted, category,
       active_wallets, wallet_fills, fills, notional_shares, cash_traded, cash_staked,
       CASE WHEN whale_wallets_in_segment BETWEEN 1 AND 9 THEN NULL ELSE whale_wallets_in_segment END AS whale_wallets_in_segment,
       CASE WHEN whale_wallets_in_segment BETWEEN 1 AND 9 THEN NULL ELSE whale_cash_traded_in_segment END AS whale_cash_traded_in_segment,
       CASE WHEN whale_wallets_global BETWEEN 1 AND 9 THEN NULL ELSE whale_wallets_global END AS whale_wallets_global,
       CASE WHEN whale_wallets_global BETWEEN 1 AND 9 THEN NULL ELSE whale_cash_traded_global END AS whale_cash_traded_global
FROM cube
WHERE active_wallets >= 10
