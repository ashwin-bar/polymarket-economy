-- gold.weekly_market_volume (docs/GOLD_PLAN.md §3): the market-side view of each week. Every
-- matched fill is counted once, from trades; there are no wallets here. notional_shares is the
-- platform headline (shares matched, what Polymarket and markets.volume report).

CREATE OR REPLACE TABLE workspace.polymarket_gold.weekly_market_volume AS
WITH market_week AS (
    SELECT CAST(date_trunc('week', date_utc) AS DATE) AS week_start, market_id, exchange_version,
           count(*) AS fills, sum(token_amount) AS notional_shares, sum(usd_amount) AS usd_amount
    FROM workspace.polymarket_silver.trades
    GROUP BY CAST(date_trunc('week', date_utc) AS DATE), market_id, exchange_version
)
SELECT w.week_start, m.category, m.subcategory, m.is_recurring_series, m.is_world_cup, m.is_us_election_2024,
       w.exchange_version,
       sum(w.fills) AS fills,
       sum(w.notional_shares) AS notional_shares,
       sum(w.usd_amount) AS usd_amount,
       count(DISTINCT w.market_id) AS markets_traded,
       count(DISTINCT m.event_id) AS events_traded,
       count(DISTINCT CASE WHEN CAST(date_trunc('week', m.first_fill_ts) AS DATE) = w.week_start THEN w.market_id END)
           AS markets_first_traded
FROM market_week w
JOIN workspace.polymarket_silver.dim_market m USING (market_id)
GROUP BY w.week_start, m.category, m.subcategory, m.is_recurring_series, m.is_world_cup, m.is_us_election_2024,
         w.exchange_version
