-- Review digest of the cohort and listing tables (not a test: gold.dq_gold holds the tests).

-- Retention at weeks 1, 4 and 8 for the three event cohorts: all wallets and retail at entry.
SELECT cohort, segment_at_entry, max(cohort_wallets) AS cohort_wallets,
       round(100 * max(CASE WHEN week_n = 1 THEN active_wallets / observable_wallets END), 2) AS week_1_pct,
       round(100 * max(CASE WHEN week_n = 4 THEN active_wallets / observable_wallets END), 2) AS week_4_pct,
       round(100 * max(CASE WHEN week_n = 8 THEN active_wallets / observable_wallets END), 2) AS week_8_pct,
       max(CASE WHEN week_n = 8 THEN observable_wallets END) AS observable_week_8
FROM workspace.polymarket_gold.cohort_retention
WHERE cohort_type = 'event' AND cohort != 'ALL' AND entry_category = 'ALL' AND scripted = 'ALL'
  AND segment_at_entry IN ('ALL', 'retail')
GROUP BY cohort, segment_at_entry ORDER BY cohort, segment_at_entry
;

-- The same excluding scripted wallets, and by entry category for the World Cup cohort.
SELECT cohort, scripted, entry_category, max(cohort_wallets) AS cohort_wallets,
       round(100 * max(CASE WHEN week_n = 1 THEN active_wallets / observable_wallets END), 2) AS week_1_pct,
       round(100 * max(CASE WHEN week_n = 4 THEN active_wallets / observable_wallets END), 2) AS week_4_pct,
       round(100 * max(CASE WHEN week_n = 8 THEN active_wallets / observable_wallets END), 2) AS week_8_pct
FROM workspace.polymarket_gold.cohort_retention
WHERE cohort_type = 'event' AND segment_at_entry = 'retail'
  AND ((cohort != 'ALL' AND scripted = 'not scripted' AND entry_category = 'ALL')
       OR (cohort = 'world_cup_2026' AND scripted = 'ALL' AND entry_category != 'ALL'))
GROUP BY cohort, scripted, entry_category ORDER BY cohort, scripted, cohort_wallets DESC
;

-- Cross-sell: summary rows per cohort, all wallets and retail at entry.
SELECT cohort, segment_at_entry, traded_category, base_wallets, wallets, round(100.0 * wallets / base_wallets, 2) AS pct
FROM workspace.polymarket_gold.cohort_category_transition
WHERE cohort_type = 'event' AND cohort != 'ALL' AND entry_category = 'ALL' AND segment_at_entry IN ('ALL', 'retail')
  AND traded_category IN ('Any category', 'Any other category', 'None (did not trade in weeks 1-8)')
ORDER BY cohort, segment_at_entry, traded_category
;

-- Cross-sell: the largest entry category -> other category transitions, retail at entry.
SELECT cohort, entry_category, traded_category, base_wallets, wallets, round(100.0 * wallets / base_wallets, 2) AS pct
FROM workspace.polymarket_gold.cohort_category_transition
WHERE cohort_type = 'event' AND cohort != 'ALL' AND segment_at_entry = 'retail' AND entry_category != 'ALL'
  AND traded_category NOT IN ('Any category', 'Any other category', 'None (did not trade in weeks 1-8)')
  AND base_wallets >= 5000
QUALIFY row_number() OVER (PARTITION BY cohort ORDER BY CASE WHEN traded_category != entry_category THEN wallets END DESC NULLS LAST) <= 8
     OR traded_category = entry_category
ORDER BY cohort, base_wallets DESC, wallets DESC
;

-- Listing scorecard: how many listings of each kind, and what they hold.
SELECT listing_type, never_traded, low_traction, count(*) AS listings, sum(events) AS events,
       round(sum(notional_shares) / 1e6, 1) AS notional_m, sum(wallets_acquired) AS wallets_acquired_shown
FROM workspace.polymarket_gold.listing_scorecard
GROUP BY listing_type, never_traded, low_traction ORDER BY 1, 2, 3
;

-- Listing scorecard: the listings that acquired the most wallets.
SELECT listing_type, listing_key, listing_title, category, traders, wallets_acquired,
       round(100 * acquired_share, 1) AS acquired_pct,
       round(100.0 * try_divide(acquired_wallets_trading_elsewhere, wallets_acquired), 1) AS acquired_trading_elsewhere_pct,
       round(notional_shares / 1e6, 1) AS notional_m, active_life_days
FROM workspace.polymarket_gold.listing_scorecard
ORDER BY wallets_acquired DESC NULLS LAST LIMIT 12
