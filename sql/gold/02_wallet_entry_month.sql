-- Silver staging for gold (docs/GOLD_PLAN.md §2): one row per wallet and 30-day period since its
-- own first fill (0-23). Carries wallet_id, so it stays in polymarket_silver;
-- gold.cohort_retention_monthly is aggregated from it.
--
-- month_n = whole 30-day periods between the wallet's first fill and the fill: period 0 is the
-- first 30 days. A fill timed a moment before first_fill_ts by rounding counts as period 0.

CREATE OR REPLACE TABLE workspace.polymarket_silver.wallet_entry_month AS
SELECT /*+ BROADCAST(w) */
       f.wallet_id,
       CAST(greatest(floor((unix_micros(f.ts_utc) - unix_micros(w.first_fill_ts)) / 2592000000000), 0) AS INT) AS month_n,
       count(*) AS wallet_fills,
       sum(f.usd_amount) AS cash_traded
FROM workspace.polymarket_silver.wallet_fills f
JOIN (SELECT wallet_id, first_fill_ts FROM workspace.polymarket_silver.dim_wallet) w USING (wallet_id)
WHERE f.ts_utc < w.first_fill_ts + INTERVAL 720 DAYS
GROUP BY 1, 2
