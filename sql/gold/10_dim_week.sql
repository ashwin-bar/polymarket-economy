-- gold.dim_week (docs/GOLD_PLAN.md §3): one row per UTC week, Monday start. The data runs
-- Monday 21 Nov 2022 to Sunday 4 Oct 2026, so all 202 weeks are complete.

CREATE OR REPLACE TABLE workspace.polymarket_gold.dim_week AS
SELECT week_start,
       max(date_utc) AS week_end,
       count(*) AS days,
       CAST(date_trunc('month', week_start) AS DATE) AS month_start,
       min(is_v2) AS is_v2,                                   -- the whole week is on the V2 exchange
       max(date_utc = DATE '2026-04-28') AS contains_v2_cutover,
       max(is_known_gap) AS contains_known_gap,               -- 29-30 Apr 2026 (decision 24)
       max(is_partial_month) AS in_partial_month,             -- Nov 2022, Oct 2026: segments under-classified
       max(event_window) AS event_window,                     -- a week touching an event window carries its name
       max(date_utc BETWEEN DATE '2024-10-01' AND DATE '2025-10-31') AS source_clock_unreliable,
       min(date_utc) < DATE '2023-03-01' AS is_left_censored_period
FROM workspace.polymarket_silver.dim_date
GROUP BY week_start
