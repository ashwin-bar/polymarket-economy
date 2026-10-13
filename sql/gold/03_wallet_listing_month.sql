-- Silver staging for gold (docs/GOLD_PLAN.md §2): one row per wallet, listing and UTC month.
-- A listing is a one-off event (listing_key = event id) or a recurring series (listing_key =
-- the series' slug stem, decision 12). Carries wallet_id, so it stays in polymarket_silver;
-- gold.listing_scorecard and gold.series_month are aggregated from it.

CREATE OR REPLACE TABLE workspace.polymarket_silver.wallet_listing_month AS
SELECT f.wallet_id,
       CASE WHEN m.is_recurring_series THEN 'series' ELSE 'event' END AS listing_type,
       CASE WHEN m.is_recurring_series THEN m.series_key ELSE m.event_id END AS listing_key,
       f.month_start,
       count(*) AS wallet_fills,
       sum(f.usd_amount) AS cash_traded
FROM workspace.polymarket_silver.wallet_fills f
JOIN workspace.polymarket_silver.dim_market m USING (market_id)
GROUP BY 1, 2, 3, 4
