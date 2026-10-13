-- Silver staging for gold (docs/GOLD_PLAN.md §2): one row per wallet, week since its own first
-- fill (0-8) and market category. Carries wallet_id, so it stays in polymarket_silver;
-- gold.cohort_retention and gold.cohort_category_transition are aggregated from it.
--
-- week_n = whole weeks between the wallet's first fill and the fill (decision 27): week 0 is
-- the first 7 days. A fill timed a moment before first_fill_ts by rounding counts as week 0.
--
-- default target = workspace.polymarket_silver.wallet_entry_week
-- default wallet_filter = 1 = 1

CREATE OR REPLACE TABLE {{target}} AS
SELECT /*+ BROADCAST(w) */
       f.wallet_id,
       CAST(greatest(floor((unix_micros(f.ts_utc) - unix_micros(w.first_fill_ts)) / 604800000000), 0) AS INT) AS week_n,
       m.category,
       count(*) AS wallet_fills,
       sum(f.usd_amount) AS cash_traded
FROM workspace.polymarket_silver.wallet_fills f
JOIN (SELECT wallet_id, first_fill_ts FROM workspace.polymarket_silver.dim_wallet WHERE {{wallet_filter}}) w USING (wallet_id)
JOIN workspace.polymarket_silver.dim_market m USING (market_id)
WHERE f.ts_utc < w.first_fill_ts + INTERVAL 63 DAYS
GROUP BY 1, 2, 3
