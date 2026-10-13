-- gold.cohort_category_transition (docs/GOLD_PLAN.md §3): where a cohort trades after its entry.
-- For wallets observable through week 8 (first fill at least 63 days before the data ends):
-- entry category -> categories traded in weeks 1-8 from their own first fill.
--
--   traded_category   a taxonomy category (the entry category itself means staying put), or
--                     'Any category'                        at least one fill in weeks 1-8
--                     'Any other category'                  at least one fill in weeks 1-8 outside
--                                                           the entry category (cross-sell)
--                     'None (did not trade in weeks 1-8)'
--   base_wallets      wallets in the cohort cell observable through week 8
--   wallets           of those, wallets with at least one fill in traded_category in weeks 1-8
--   wallet_fills, cash_traded   theirs, in that category, in weeks 1-8
--   transition share = wallets / base_wallets (a row's shares do not sum to 100%)
--
-- A cube: cohort, entry_category and segment_at_entry each have an 'ALL' value, aggregated
-- before suppression. Rows with fewer than 10 base wallets are removed; a wallets count of 1-9
-- is nulled with its measures. No wallet id leaves this statement.

CREATE OR REPLACE TABLE workspace.polymarket_gold.cohort_category_transition AS
WITH w AS (
    SELECT wallet_id, cohort AS event_cohort, date_format(first_month, 'yyyy-MM') AS month_cohort,
           entry_category, segment_at_entry
    FROM workspace.polymarket_silver.dim_wallet
    WHERE first_fill_ts + INTERVAL 63 DAYS <= TIMESTAMP '2026-10-05 00:00:00'
),
typed AS (
    SELECT 'event' AS cohort_type, event_cohort AS cohort, wallet_id, entry_category, segment_at_entry
    FROM w WHERE event_cohort IS NOT NULL
    UNION ALL
    SELECT 'month', month_cohort, wallet_id, entry_category, segment_at_entry FROM w
),
later AS (
    SELECT wallet_id, category, sum(wallet_fills) AS wallet_fills, sum(cash_traded) AS cash_traded
    FROM workspace.polymarket_silver.wallet_entry_week
    WHERE week_n BETWEEN 1 AND 8
    GROUP BY wallet_id, category
),
wallet_rows AS (
    SELECT t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, l.category AS traded_category,
           l.wallet_fills, l.cash_traded
    FROM typed t JOIN later l USING (wallet_id)
    UNION ALL
    SELECT t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, 'Any category',
           sum(l.wallet_fills), sum(l.cash_traded)
    FROM typed t JOIN later l USING (wallet_id)
    GROUP BY t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, t.wallet_id
    UNION ALL
    SELECT t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, 'Any other category',
           sum(l.wallet_fills), sum(l.cash_traded)
    FROM typed t JOIN later l USING (wallet_id)
    WHERE l.category != t.entry_category
    GROUP BY t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, t.wallet_id
    UNION ALL
    SELECT t.cohort_type, t.cohort, t.entry_category, t.segment_at_entry, 'None (did not trade in weeks 1-8)', 0, 0
    FROM typed t LEFT ANTI JOIN (SELECT DISTINCT wallet_id FROM later) l USING (wallet_id)
),
moved AS (
    SELECT cohort_type, traded_category,
           coalesce(cohort, 'ALL') AS cohort,
           coalesce(entry_category, 'ALL') AS entry_category,
           coalesce(segment_at_entry, 'ALL') AS segment_at_entry,
           count(*) AS wallets, sum(wallet_fills) AS wallet_fills, sum(cash_traded) AS cash_traded
    FROM wallet_rows
    GROUP BY cohort_type, traded_category, CUBE (cohort, entry_category, segment_at_entry)
),
base AS (
    SELECT cohort_type,
           coalesce(cohort, 'ALL') AS cohort,
           coalesce(entry_category, 'ALL') AS entry_category,
           coalesce(segment_at_entry, 'ALL') AS segment_at_entry,
           count(*) AS base_wallets
    FROM typed
    GROUP BY cohort_type, CUBE (cohort, entry_category, segment_at_entry)
)
SELECT b.cohort_type, b.cohort, b.entry_category, b.segment_at_entry, m.traded_category,
       b.base_wallets,
       CASE WHEN m.wallets BETWEEN 1 AND 9 THEN NULL ELSE m.wallets END AS wallets,
       CASE WHEN m.wallets BETWEEN 1 AND 9 THEN NULL ELSE m.wallet_fills END AS wallet_fills,
       CASE WHEN m.wallets BETWEEN 1 AND 9 THEN NULL ELSE m.cash_traded END AS cash_traded
FROM base b
JOIN moved m USING (cohort_type, cohort, entry_category, segment_at_entry)
WHERE b.base_wallets >= 10
