-- Silver staging for gold (docs/GOLD_PLAN.md §2): one row per wallet, UTC week (Monday start)
-- and market category. It carries wallet_id, so it stays in polymarket_silver and is never
-- published; gold.weekly_kpi is aggregated from it.
--
--   wallet_fills, cash_traded, cash_staked   from wallet_fills (each wallet side of each match)
--   fills_attributed, notional_attributed    from trades: each matched fill and its shares are
--                                            split half to the maker wallet and half to the taker
--                                            wallet, so any split of wallets sums to the headline
--   first_month_in_week                      month of the wallet's first fill that week; its
--                                            monthly segment for that month is its segment for
--                                            the week
--
-- default target = workspace.polymarket_silver.wallet_week_category
-- default month_filter = 1 = 1

CREATE OR REPLACE TABLE {{target}} AS
WITH wallet_side AS (
    SELECT f.wallet_id,
           CAST(date_trunc('week', f.date_utc) AS DATE) AS week_start,
           m.category,
           min(f.month_start) AS first_month_in_week,
           count(*) AS wallet_fills,
           sum(f.usd_amount) AS cash_traded,
           sum(CASE WHEN f.direction = 'BUY' THEN f.usd_amount ELSE 0 END) AS cash_staked
    FROM workspace.polymarket_silver.wallet_fills f
    JOIN workspace.polymarket_silver.dim_market m USING (market_id)
    WHERE {{month_filter}}
    GROUP BY f.wallet_id, CAST(date_trunc('week', f.date_utc) AS DATE), m.category
),
trade_sides AS (
    SELECT explode(array(maker_wallet_id, taker_wallet_id)) AS wallet_id, date_utc, market_id, token_amount
    FROM workspace.polymarket_silver.trades
    WHERE {{month_filter}}
),
match_side AS (
    SELECT t.wallet_id,
           CAST(date_trunc('week', t.date_utc) AS DATE) AS week_start,
           m.category,
           0.5 * count(*) AS fills_attributed,
           0.5 * sum(t.token_amount) AS notional_attributed
    FROM trade_sides t
    JOIN workspace.polymarket_silver.dim_market m USING (market_id)
    GROUP BY t.wallet_id, CAST(date_trunc('week', t.date_utc) AS DATE), m.category
)
SELECT coalesce(w.wallet_id, t.wallet_id) AS wallet_id,
       coalesce(w.week_start, t.week_start) AS week_start,
       coalesce(w.category, t.category) AS category,
       w.first_month_in_week,
       coalesce(w.wallet_fills, 0) AS wallet_fills,
       coalesce(w.cash_traded, 0) AS cash_traded,
       coalesce(w.cash_staked, 0) AS cash_staked,
       coalesce(t.fills_attributed, 0) AS fills_attributed,
       coalesce(t.notional_attributed, 0) AS notional_attributed
FROM wallet_side w
FULL JOIN match_side t
  ON t.wallet_id = w.wallet_id AND t.week_start = w.week_start AND t.category = w.category
