-- Methodology tables (docs/GOLD_PLAN.md §3): how silver reconciles with bronze, with the source's
-- own market volumes, and how much of the volume the taxonomy covers.

-- One row per UTC month: bronze rows against silver rows for both fact tables, and the month's
-- data-quality facts recorded when it was loaded.
CREATE OR REPLACE TABLE workspace.polymarket_gold.recon_monthly AS
WITH l AS (
    SELECT month_start,
           max(CASE WHEN table_name = 'trades' THEN rows END) AS trades_rows_silver,
           max(CASE WHEN table_name = 'wallet_fills' THEN rows END) AS wallet_fills_rows_silver,
           max(CASE WHEN table_name = 'wallet_fills' THEN CAST(get_json_object(checks, '$.maker_rows') AS BIGINT) END) AS maker_rows,
           max(CASE WHEN table_name = 'wallet_fills' THEN CAST(get_json_object(checks, '$.wallets') AS BIGINT) END) AS active_wallets,
           max(CASE WHEN table_name = 'trades' THEN CAST(get_json_object(checks, '$.v2_rows') AS BIGINT) END) AS v2_fills,
           max(CASE WHEN table_name = 'trades' THEN CAST(get_json_object(checks, '$.max_abs_source_clock_error_s') AS BIGINT) END)
               AS max_source_clock_error_seconds,
           max(CASE WHEN table_name = 'trades' THEN CAST(get_json_object(checks, '$.at_first_second') AS BIGINT)
                                                    + CAST(get_json_object(checks, '$.at_last_second') AS BIGINT) END)
               AS fills_on_first_or_last_second
    FROM workspace.polymarket_silver.load_log GROUP BY month_start
),
b AS (
    SELECT month_start,
           max(CASE WHEN table_name = 'trades' THEN CAST(expected AS BIGINT) END) AS trades_rows_bronze,
           max(CASE WHEN table_name = 'wallet_fills' THEN CAST(expected AS BIGINT) END) AS wallet_fills_rows_bronze
    FROM workspace.polymarket_silver.dq_results WHERE test = 'rows = bronze' GROUP BY month_start
),
q AS (
    SELECT month_start, count(*) AS checks_run, count_if(passed) AS checks_passed
    FROM workspace.polymarket_silver.dq_results WHERE month_start IS NOT NULL GROUP BY month_start
),
v AS (
    SELECT month_start, sum(notional_shares) AS notional_shares, count(*) AS markets_traded
    FROM workspace.polymarket_silver.market_month GROUP BY month_start
)
SELECT l.month_start, b.trades_rows_bronze, l.trades_rows_silver, b.wallet_fills_rows_bronze, l.wallet_fills_rows_silver,
       l.maker_rows, l.maker_rows = l.trades_rows_silver AS maker_rows_equal_fills,
       l.active_wallets, v.markets_traded, v.notional_shares, l.v2_fills,
       l.max_source_clock_error_seconds, l.fills_on_first_or_last_second,
       q.checks_run, q.checks_passed,
       l.month_start IN (DATE '2022-11-01', DATE '2026-10-01') AS is_partial_month
FROM l JOIN b USING (month_start) JOIN q USING (month_start) JOIN v USING (month_start)
;

-- Fills against the source's own market volume, for closed markets with a source volume, by
-- period relative to the V2 exchange cutover of 28 April 2026.
CREATE OR REPLACE TABLE workspace.polymarket_gold.recon_volume AS
WITH m AS (
    SELECT CASE WHEN last_fill_ts < TIMESTAMP '2026-04-28' THEN '1 before the cutover'
                WHEN first_fill_ts >= TIMESTAMP '2026-04-29' THEN '3 after the cutover'
                ELSE '2 spans the cutover' END AS period,
           volume_recon_status AS status, notional_shares, markets_volume
    FROM workspace.polymarket_silver.dim_market
    WHERE volume_recon_status IN ('within_1pct', 'short', 'over')
)
SELECT period, status,
       count(*) AS markets,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY period), 2) AS pct_of_period_markets,
       sum(notional_shares) AS notional_shares, sum(markets_volume) AS source_volume
FROM m
GROUP BY period, status
;

-- Taxonomy coverage: how each year's traded volume got its category.
CREATE OR REPLACE TABLE workspace.polymarket_gold.taxonomy_coverage AS
SELECT year(mm.month_start) AS year, d.category_source,
       count(DISTINCT mm.market_id) AS markets, sum(mm.fills) AS fills, sum(mm.notional_shares) AS notional_shares,
       round(100.0 * sum(mm.notional_shares) / sum(sum(mm.notional_shares)) OVER (PARTITION BY year(mm.month_start)), 2) AS pct_notional,
       round(100.0 * sum(mm.fills) / sum(sum(mm.fills)) OVER (PARTITION BY year(mm.month_start)), 2) AS pct_fills
FROM workspace.polymarket_silver.market_month mm
JOIN workspace.polymarket_silver.dim_market d USING (market_id)
GROUP BY year(mm.month_start), d.category_source
;

-- The same for entering wallets: how the market of each cohort's first fill got its category.
CREATE OR REPLACE TABLE workspace.polymarket_gold.taxonomy_entry_coverage AS
SELECT coalesce(w.cohort, 'all other wallets') AS cohort, d.category_source,
       CASE WHEN count(*) < 10 THEN NULL ELSE count(*) END AS wallets,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY coalesce(w.cohort, 'all other wallets')), 2) AS pct_wallets
FROM workspace.polymarket_silver.dim_wallet w
JOIN workspace.polymarket_silver.dim_market d ON d.market_id = w.first_market_id
GROUP BY coalesce(w.cohort, 'all other wallets'), d.category_source
;

-- How big each wallet segment is, month by month. Shares are computed before suppression.
CREATE OR REPLACE TABLE workspace.polymarket_gold.segment_summary AS
SELECT month_start, segment, wallets, pct_wallets, pct_wallet_fills, pct_cash_traded
FROM (
    SELECT month_start, segment, count(*) AS wallets,
           round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY month_start), 3) AS pct_wallets,
           round(100.0 * sum(fills) / sum(sum(fills)) OVER (PARTITION BY month_start), 2) AS pct_wallet_fills,
           round(100.0 * sum(cash_traded) / sum(sum(cash_traded)) OVER (PARTITION BY month_start), 2) AS pct_cash_traded
    FROM workspace.polymarket_silver.wallet_month
    GROUP BY month_start, segment)
WHERE wallets >= 10
