-- gold.series_month (docs/GOLD_PLAN.md §3): each recurring series over time, one row per series
-- and UTC month with fills.
--
--   events, traded_markets, fills, notional_shares   that month, from trades
--   traders            distinct wallets with a fill in the series that month
--   wallets_acquired   wallets whose first-ever fill was in the series, that month
--
-- Suppression: wallet counts under 10 are nulled.

CREATE OR REPLACE TABLE workspace.polymarket_gold.series_month AS
WITH volume AS (
    SELECT m.series_key, mm.month_start, any_value(m.category) AS category,
           count(DISTINCT m.event_id) AS events, count(*) AS traded_markets,
           sum(mm.fills) AS fills, sum(mm.notional_shares) AS notional_shares
    FROM workspace.polymarket_silver.market_month mm
    JOIN workspace.polymarket_silver.dim_market m USING (market_id)
    WHERE m.is_recurring_series
    GROUP BY m.series_key, mm.month_start
),
traders AS (
    SELECT listing_key AS series_key, month_start, count(*) AS traders
    FROM workspace.polymarket_silver.wallet_listing_month
    WHERE listing_type = 'series'
    GROUP BY listing_key, month_start
),
acquired AS (
    SELECT m.series_key, w.first_month AS month_start, count(*) AS wallets_acquired
    FROM workspace.polymarket_silver.dim_wallet w
    JOIN workspace.polymarket_silver.dim_market m ON m.market_id = w.first_market_id
    WHERE m.is_recurring_series
    GROUP BY m.series_key, w.first_month
)
SELECT v.series_key, v.month_start, v.category, v.events, v.traded_markets, v.fills, v.notional_shares,
       CASE WHEN t.traders BETWEEN 1 AND 9 THEN NULL ELSE coalesce(t.traders, 0) END AS traders,
       CASE WHEN a.wallets_acquired BETWEEN 1 AND 9 OR t.traders BETWEEN 1 AND 9 THEN NULL
            ELSE coalesce(a.wallets_acquired, 0) END AS wallets_acquired
FROM volume v
LEFT JOIN traders t USING (series_key, month_start)
LEFT JOIN acquired a USING (series_key, month_start)
