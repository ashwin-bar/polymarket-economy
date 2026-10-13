-- gold.listing_scorecard (docs/GOLD_PLAN.md §3): one row per listing. A listing is a one-off
-- event, or a recurring series taken as a whole (decision 12). Listings are public market
-- information; the wallet figures are counts.
--
--   fills, notional_shares      from trades, each fill once
--   first / last_fill_date      from trades (decision 13), active_life_days = last - first + 1
--   traders                     distinct wallets with at least one fill in the listing
--   wallets_acquired            wallets whose first-ever fill was in the listing
--   existing_traders            traders - wallets_acquired
--   acquired_share              wallets_acquired / traders
--   acquired_wallets_trading_elsewhere   acquired wallets with a fill in any other listing
--   median_traders_per_event    series only: median over its traded events of distinct traders
--   never_traded                listed in markets.parquet but no fill at all
--   low_traction                traded, but fewer than 10 distinct traders over its life
--                               (series: median under 10 traders per event)
--
-- Suppression: exact wallet counts under 10 are nulled (with the figures derived from them); the
-- band columns always show the range.

CREATE OR REPLACE TABLE workspace.polymarket_gold.listing_scorecard AS
WITH market AS (
    SELECT CASE WHEN is_recurring_series THEN 'series' ELSE 'event' END AS listing_type,
           CASE WHEN is_recurring_series THEN series_key ELSE event_id END AS listing_key,
           *
    FROM workspace.polymarket_silver.dim_market
),
listing AS (
    SELECT listing_type, listing_key,
           min(CASE WHEN listing_type = 'event' THEN event_slug END) AS event_slug,
           min(event_title) AS listing_title,
           -- a series takes one category (decision 32); a one-off event has exactly one
           max_by(category, notional_shares) AS category,
           max_by(subcategory, notional_shares) AS subcategory,
           max(is_world_cup) AS is_world_cup, max(is_us_election_2024) AS is_us_election_2024,
           count(DISTINCT event_id) AS events, count(*) AS markets, count_if(fills > 0) AS traded_markets,
           CAST(min(first_fill_ts) AS DATE) AS first_fill_date, CAST(max(last_fill_ts) AS DATE) AS last_fill_date,
           sum(fills) AS fills, sum(notional_shares) AS notional_shares,
           count_if(volume_recon_status = 'within_1pct')
               / nullif(count_if(volume_recon_status IN ('within_1pct', 'short', 'over')), 0) AS share_markets_within_1pct_of_source_volume
    FROM market
    GROUP BY listing_type, listing_key
),
wallet_listing AS (
    SELECT wallet_id, listing_type, listing_key
    FROM workspace.polymarket_silver.wallet_listing_month
    GROUP BY wallet_id, listing_type, listing_key
),
traders AS (
    SELECT listing_type, listing_key, count(*) AS traders FROM wallet_listing GROUP BY listing_type, listing_key
),
wallet_breadth AS (
    SELECT wallet_id, count(*) AS listings FROM wallet_listing GROUP BY wallet_id
),
acquired AS (
    SELECT m.listing_type, m.listing_key, count(*) AS wallets_acquired,
           count_if(b.listings > 1) AS acquired_wallets_trading_elsewhere
    FROM workspace.polymarket_silver.dim_wallet w
    JOIN market m ON m.market_id = w.first_market_id
    JOIN wallet_breadth b ON b.wallet_id = w.wallet_id
    GROUP BY m.listing_type, m.listing_key
),
event_traders AS (
    SELECT m.series_key, m.event_id, count(DISTINCT f.wallet_id) AS traders
    FROM workspace.polymarket_silver.wallet_fills f
    JOIN workspace.polymarket_silver.dim_market m USING (market_id)
    WHERE m.is_recurring_series
    GROUP BY m.series_key, m.event_id
),
series_median AS (
    SELECT series_key, percentile(traders, 0.5) AS median_traders_per_event FROM event_traders GROUP BY series_key
),
joined AS (
    SELECT l.*, coalesce(t.traders, 0) AS traders_exact, coalesce(a.wallets_acquired, 0) AS acquired_exact,
           coalesce(a.acquired_wallets_trading_elsewhere, 0) AS elsewhere_exact, s.median_traders_per_event
    FROM listing l
    LEFT JOIN traders t USING (listing_type, listing_key)
    LEFT JOIN acquired a USING (listing_type, listing_key)
    LEFT JOIN series_median s ON l.listing_type = 'series' AND s.series_key = l.listing_key
)
SELECT listing_type, listing_key, event_slug, listing_title, category, subcategory, is_world_cup, is_us_election_2024,
       events, markets, traded_markets, first_fill_date, last_fill_date,
       datediff(last_fill_date, first_fill_date) + 1 AS active_life_days,
       fills, notional_shares,
       CASE WHEN traders_exact BETWEEN 1 AND 9 THEN NULL ELSE traders_exact END AS traders,
       CASE WHEN acquired_exact BETWEEN 1 AND 9 OR traders_exact BETWEEN 1 AND 9 THEN NULL ELSE acquired_exact END AS wallets_acquired,
       CASE WHEN acquired_exact BETWEEN 1 AND 9 OR traders_exact BETWEEN 1 AND 9
                 OR traders_exact - acquired_exact BETWEEN 1 AND 9 THEN NULL
            ELSE traders_exact - acquired_exact END AS existing_traders,
       CASE WHEN acquired_exact BETWEEN 1 AND 9 OR traders_exact < 10 THEN NULL
            ELSE acquired_exact / traders_exact END AS acquired_share,
       CASE WHEN elsewhere_exact BETWEEN 1 AND 9 OR acquired_exact BETWEEN 1 AND 9 THEN NULL ELSE elsewhere_exact END
           AS acquired_wallets_trading_elsewhere,
       CASE WHEN traders_exact = 0 THEN '0' WHEN traders_exact < 10 THEN '1-9' WHEN traders_exact < 100 THEN '10-99'
            WHEN traders_exact < 1000 THEN '100-999' WHEN traders_exact < 10000 THEN '1,000-9,999' ELSE '10,000+' END AS traders_band,
       CASE WHEN acquired_exact = 0 THEN '0' WHEN acquired_exact < 10 THEN '1-9' WHEN acquired_exact < 100 THEN '10-99'
            WHEN acquired_exact < 1000 THEN '100-999' WHEN acquired_exact < 10000 THEN '1,000-9,999' ELSE '10,000+' END AS acquired_band,
       median_traders_per_event,
       fills = 0 AS never_traded,
       fills > 0 AND CASE WHEN listing_type = 'series' THEN median_traders_per_event < 10 ELSE traders_exact < 10 END AS low_traction,
       share_markets_within_1pct_of_source_volume
FROM joined
