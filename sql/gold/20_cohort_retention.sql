-- gold.cohort_retention (docs/GOLD_PLAN.md §3): retention by weeks since each wallet's own first
-- fill (weeks 0-8, decision 27), for the three event cohorts and for monthly acquisition cohorts.
-- A cube: cohort, entry_category, segment_at_entry and scripted each have an 'ALL' value,
-- aggregated before suppression, so headline curves are exact. Pick one value or 'ALL' per
-- dimension; never sum rows of a dimension.
--
--   cohort_wallets      wallets in the cell
--   observable_wallets  wallets whose week n lies wholly inside the data:
--                       first_fill_ts + (n + 1) x 7 days <= 5 Oct 2026 00:00 UTC
--   active_wallets      observable wallets with at least one fill in week n (week 0 = all)
--   wallet_fills, cash_traded   of those wallets in week n
--   retention (week n) = active_wallets / observable_wallets
--
-- Suppression: rows with fewer than 10 cohort wallets are removed; a count of 1-9 inside a kept
-- row is nulled together with its measures. No wallet id leaves this statement.

CREATE OR REPLACE TABLE workspace.polymarket_gold.cohort_retention AS
WITH w AS (
    SELECT wallet_id, cohort AS event_cohort, date_format(first_month, 'yyyy-MM') AS month_cohort,
           entry_category, segment_at_entry,
           CASE WHEN is_scripted_cohort THEN 'scripted' ELSE 'not scripted' END AS scripted,
           entered_via_world_cup_market AS via_world_cup, entered_via_us_election_2024_market AS via_election,
           CAST(floor((unix_micros(TIMESTAMP '2026-10-05 00:00:00') - unix_micros(first_fill_ts)) / 604800000000) AS INT) - 1
               AS last_observable
    FROM workspace.polymarket_silver.dim_wallet
),
typed AS (
    SELECT 'event' AS cohort_type, event_cohort AS cohort, wallet_id, entry_category, segment_at_entry, scripted,
           via_world_cup, via_election, last_observable
    FROM w WHERE event_cohort IS NOT NULL
    UNION ALL
    SELECT 'month', month_cohort, wallet_id, entry_category, segment_at_entry, scripted,
           via_world_cup, via_election, last_observable
    FROM w
),
activity AS (
    SELECT wallet_id, week_n AS n, sum(wallet_fills) AS wallet_fills, sum(cash_traded) AS cash_traded
    FROM workspace.polymarket_silver.wallet_entry_week
    GROUP BY wallet_id, week_n
),
periods AS (SELECT explode(sequence(0, 8)) AS n),
base AS (
    SELECT t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, t.scripted, p.n,
           count(*) AS cohort_wallets,
           count_if(t.last_observable >= p.n) AS observable_wallets,
           count_if(t.via_world_cup) AS via_world_cup_wallets,
           count_if(t.via_election) AS via_election_wallets
    FROM typed t CROSS JOIN periods p
    GROUP BY t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, t.scripted, p.n
),
active AS (
    SELECT t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, t.scripted, a.n,
           count(*) AS active_wallets, sum(a.wallet_fills) AS wallet_fills, sum(a.cash_traded) AS cash_traded
    FROM typed t JOIN activity a USING (wallet_id)
    WHERE a.n <= t.last_observable
    GROUP BY t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, t.scripted, a.n
),
fine AS (
    SELECT b.*, coalesce(a.active_wallets, 0) AS active_wallets, coalesce(a.wallet_fills, 0) AS wallet_fills,
           coalesce(a.cash_traded, 0) AS cash_traded
    FROM base b LEFT JOIN active a USING (cohort_type, cohort, entry_category, segment_at_entry, scripted, n)
),
cube AS (
    SELECT cohort_type, n,
           coalesce(cohort, 'ALL') AS cohort,
           coalesce(entry_category, 'ALL') AS entry_category,
           coalesce(segment_at_entry, 'ALL') AS segment_at_entry,
           coalesce(scripted, 'ALL') AS scripted,
           sum(cohort_wallets) AS cohort_wallets, sum(observable_wallets) AS observable_wallets,
           sum(active_wallets) AS active_wallets, sum(wallet_fills) AS wallet_fills, sum(cash_traded) AS cash_traded,
           sum(via_world_cup_wallets) AS via_world_cup_wallets, sum(via_election_wallets) AS via_election_wallets
    FROM fine
    GROUP BY cohort_type, n, CUBE (cohort, entry_category, segment_at_entry, scripted)
),
shaped AS (
    SELECT *,
           observable_wallets BETWEEN 1 AND 9 OR active_wallets BETWEEN 1 AND 9 AS hide_activity,
           CASE cohort WHEN 'world_cup_2026' THEN via_world_cup_wallets
                       WHEN 'us_election_2024' THEN via_election_wallets END AS via_event
    FROM cube
)
SELECT cohort_type, cohort, entry_category, segment_at_entry, scripted, n AS week_n,
       cohort_wallets,
       CASE WHEN observable_wallets BETWEEN 1 AND 9 THEN NULL ELSE observable_wallets END AS observable_wallets,
       CASE WHEN hide_activity THEN NULL ELSE active_wallets END AS active_wallets,
       CASE WHEN hide_activity THEN NULL ELSE wallet_fills END AS wallet_fills,
       CASE WHEN hide_activity THEN NULL ELSE cash_traded END AS cash_traded,
       CASE WHEN via_event BETWEEN 1 AND 9 THEN NULL ELSE via_event END AS entered_via_event_market_wallets,
       cohort_type = 'month' AND cohort < '2023-03' AS is_left_censored_cohort
FROM shaped
WHERE cohort_wallets >= 10
