-- gold.cohort_calendar_baseline (docs/GOLD_PLAN.md §3): a control for the event cohorts. How
-- much of a cohort's drop-off is the calendar (the tournament or election ending) and how much
-- is the cohort itself?
--
-- Baseline wallets for a cohort: wallets first seen at least 12 weeks (84 days) before the
-- cohort's window opened, with at least one fill inside the window.
--
-- The cohort's week n is counted from each wallet's own first fill, so it falls in different
-- calendar weeks for different wallets. The baseline is matched to that: for a cohort wallet
-- that entered on UTC day d, the same calendar days are d + 7n .. d + 7n + 6. For each entry day
-- d the baseline's retention is, of the baseline wallets active in d .. d + 6 (their "week 0"),
-- the share also active in d + 7n .. d + 7n + 6. Entry days are then weighted by the number of
-- cohort wallets that entered on each day.
--
--   segment                    'ALL', or 'retail': cohort wallets retail at entry, against
--                              baseline wallets never classed as automated in any month from
--                              the window's first month to the month of its last day + 62
--   cohort_retention           active / observable wallets, from gold.cohort_retention
--   baseline_wallets           baseline wallets (fixed set per cohort and segment)
--   baseline_week0_share       weighted share of baseline wallets active in d .. d + 6
--   baseline_retention         weighted share of those still active in the matching week n
--   relative_retention         cohort_retention / baseline_retention
--
-- The cohort side counts weeks from the exact first fill; the baseline side works in whole UTC
-- days. All figures are rates or counts of thousands of wallets; nothing is wallet-level.

CREATE OR REPLACE TABLE workspace.polymarket_gold.cohort_calendar_baseline AS
WITH windows AS (
    SELECT cohort, CAST(window_start AS DATE) AS window_start, CAST(window_end AS DATE) AS window_end
    FROM VALUES ('world_cup_2026', '2026-06-11', '2026-07-19'),
                ('us_election_2024', '2024-09-28', '2024-11-05'),
                ('benchmark_q1_2026', '2026-01-01', '2026-03-31') AS t(cohort, window_start, window_end)
),
established AS (
    SELECT /*+ BROADCAST(x) */ x.cohort, w.wallet_id
    FROM workspace.polymarket_silver.dim_wallet w
    CROSS JOIN windows x
    WHERE w.first_fill_ts < CAST(x.window_start AS TIMESTAMP) - INTERVAL 84 DAYS
),
-- Wallet-days with a fill, from each window's first day to its last day + 62 (week 8 of the
-- last entry day). Literal month and day ranges so the scan prunes to the months needed.
wallet_days AS (
    SELECT 'world_cup_2026' AS cohort, wallet_id, date_utc FROM workspace.polymarket_silver.wallet_fills
    WHERE month_start BETWEEN DATE '2026-06-01' AND DATE '2026-09-01' AND date_utc BETWEEN DATE '2026-06-11' AND DATE '2026-09-19'
    GROUP BY wallet_id, date_utc
    UNION ALL
    SELECT 'us_election_2024', wallet_id, date_utc FROM workspace.polymarket_silver.wallet_fills
    WHERE month_start BETWEEN DATE '2024-09-01' AND DATE '2025-01-01' AND date_utc BETWEEN DATE '2024-09-28' AND DATE '2025-01-06'
    GROUP BY wallet_id, date_utc
    UNION ALL
    SELECT 'benchmark_q1_2026', wallet_id, date_utc FROM workspace.polymarket_silver.wallet_fills
    WHERE month_start BETWEEN DATE '2026-01-01' AND DATE '2026-06-01' AND date_utc BETWEEN DATE '2026-01-01' AND DATE '2026-06-01'
    GROUP BY wallet_id, date_utc
),
baseline_days AS (
    SELECT d.cohort, d.wallet_id, d.date_utc
    FROM wallet_days d JOIN established e USING (cohort, wallet_id)
),
members AS (  -- active inside the window
    SELECT /*+ BROADCAST(x) */ b.cohort, b.wallet_id
    FROM baseline_days b JOIN windows x USING (cohort)
    WHERE b.date_utc BETWEEN x.window_start AND x.window_end
    GROUP BY b.cohort, b.wallet_id
),
automated AS (  -- baseline wallets classed as automated in any month around the window
    SELECT /*+ BROADCAST(x) */ x.cohort, m.wallet_id
    FROM workspace.polymarket_silver.wallet_month m JOIN windows x
      ON m.month_start BETWEEN CAST(date_trunc('month', x.window_start) AS DATE) AND date_add(x.window_end, 62)
    WHERE m.segment != 'retail'
    GROUP BY x.cohort, m.wallet_id
),
member_segment AS (
    SELECT m.cohort, m.wallet_id, a.wallet_id IS NULL AS is_retail
    FROM members m LEFT JOIN automated a USING (cohort, wallet_id)
),
-- (wallet, c): the wallet has a fill somewhere in the 7 days c .. c + 6
active_from AS (
    SELECT DISTINCT b.cohort, b.wallet_id, date_sub(b.date_utc, k.k) AS c
    FROM baseline_days b
    JOIN members m USING (cohort, wallet_id)
    CROSS JOIN (SELECT explode(sequence(0, 6)) AS k) k
),
periods AS (SELECT explode(sequence(0, 8)) AS n),
-- For each entry day d in the window and week n: baseline wallets active in week 0 from d, and
-- whether they are active in the matching week n.
matched AS (
    SELECT /*+ BROADCAST(x) */ a0.cohort, a0.wallet_id, a0.c AS entry_day, p.n, a1.wallet_id IS NOT NULL AS active_week_n
    FROM active_from a0
    JOIN windows x ON x.cohort = a0.cohort AND a0.c BETWEEN x.window_start AND x.window_end
    CROSS JOIN periods p
    LEFT JOIN active_from a1
      ON a1.cohort = a0.cohort AND a1.wallet_id = a0.wallet_id AND a1.c = date_add(a0.c, 7 * p.n)
),
by_day AS (
    SELECT m.cohort, 'ALL' AS segment, m.entry_day, m.n, count(*) AS week0_wallets, count_if(m.active_week_n) AS week_n_wallets
    FROM matched m GROUP BY m.cohort, m.entry_day, m.n
    UNION ALL
    SELECT m.cohort, 'retail', m.entry_day, m.n, count(*), count_if(m.active_week_n)
    FROM matched m JOIN member_segment s USING (cohort, wallet_id)
    WHERE s.is_retail GROUP BY m.cohort, m.entry_day, m.n
),
baseline_size AS (
    SELECT cohort, 'ALL' AS segment, count(*) AS baseline_wallets FROM member_segment GROUP BY cohort
    UNION ALL
    SELECT cohort, 'retail', count_if(is_retail) FROM member_segment GROUP BY cohort
),
cohort_entries AS (  -- weights: cohort wallets by entry day
    SELECT cohort, 'ALL' AS segment, first_date AS entry_day, count(*) AS entrants
    FROM workspace.polymarket_silver.dim_wallet WHERE cohort IS NOT NULL GROUP BY cohort, first_date
    UNION ALL
    SELECT cohort, 'retail', first_date, count(*)
    FROM workspace.polymarket_silver.dim_wallet WHERE cohort IS NOT NULL AND segment_at_entry = 'retail' GROUP BY cohort, first_date
),
baseline AS (
    SELECT d.cohort, d.segment, d.n,
           sum(e.entrants * d.week_n_wallets / d.week0_wallets) / sum(e.entrants) AS baseline_retention,
           sum(e.entrants * d.week0_wallets / s.baseline_wallets) / sum(e.entrants) AS baseline_week0_share,
           max(s.baseline_wallets) AS baseline_wallets,
           min(d.week0_wallets) AS smallest_week0_group, count(*) AS entry_days
    FROM by_day d
    JOIN cohort_entries e USING (cohort, segment, entry_day)
    JOIN baseline_size s USING (cohort, segment)
    GROUP BY d.cohort, d.segment, d.n
),
cohort_side AS (
    SELECT cohort, segment_at_entry AS segment, week_n AS n, cohort_wallets, observable_wallets, active_wallets
    FROM workspace.polymarket_gold.cohort_retention
    WHERE cohort_type = 'event' AND cohort != 'ALL' AND entry_category = 'ALL' AND scripted = 'ALL' AND entry_channel = 'ALL'
      AND segment_at_entry IN ('ALL', 'retail')
)
SELECT b.cohort, b.segment, b.n AS week_n,
       c.cohort_wallets, c.active_wallets AS cohort_active_wallets,
       c.active_wallets / c.observable_wallets AS cohort_retention,
       b.baseline_wallets, b.baseline_week0_share, b.baseline_retention,
       (c.active_wallets / c.observable_wallets) / b.baseline_retention AS relative_retention,
       b.entry_days, b.smallest_week0_group
FROM baseline b
JOIN cohort_side c USING (cohort, segment, n)
