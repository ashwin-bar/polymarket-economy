-- gold.dq_gold (docs/GOLD_PLAN.md §7): tests of the gold tables against silver and against the
-- known totals. One row per test; passed must be true everywhere.
--
-- The first three weeks of the data (21 Nov-11 Dec 2022) have fewer than 10 active wallets and
-- are suppressed in weekly_kpi, so its totals are compared with silver less those weeks.

CREATE OR REPLACE TABLE workspace.polymarket_gold.dq_gold AS
WITH k AS (SELECT * FROM workspace.polymarket_gold.weekly_kpi),
top AS (SELECT * FROM k WHERE activity_status = 'ALL' AND segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL'),
staging_week AS (
    SELECT week_start, count(DISTINCT wallet_id) AS wallets, sum(wallet_fills) AS wallet_fills,
           sum(fills_attributed) AS fills, sum(notional_attributed) AS notional, sum(cash_traded) AS cash_traded,
           sum(cash_staked) AS cash_staked
    FROM workspace.polymarket_silver.wallet_week_category GROUP BY week_start
),
shown AS (
    SELECT sum(wallet_fills) AS wallet_fills, sum(fills) AS fills, sum(notional) AS notional, sum(cash_traded) AS cash_traded,
           sum(cash_staked) AS cash_staked, count(*) AS weeks
    FROM staging_week WHERE wallets >= 10
),
silver AS (
    SELECT (SELECT count(*) FROM workspace.polymarket_silver.trades) AS fills,
           (SELECT sum(token_amount) FROM workspace.polymarket_silver.trades) AS notional,
           (SELECT sum(fills) FROM workspace.polymarket_silver.wallet_month) AS wallet_fills,
           (SELECT sum(cash_traded) FROM workspace.polymarket_silver.wallet_month) AS cash_traded
),
staging_total AS (
    SELECT sum(wallet_fills) AS wallet_fills, sum(fills) AS fills, sum(notional) AS notional, sum(cash_traded) AS cash_traded
    FROM staging_week
),
new_by_week AS (
    SELECT CAST(date_trunc('week', first_date) AS DATE) AS week_start, count(*) AS wallets
    FROM workspace.polymarket_silver.dim_wallet GROUP BY 1
),
mv AS (SELECT sum(fills) AS fills, sum(notional_shares) AS notional FROM workspace.polymarket_gold.weekly_market_volume),
r AS (SELECT * FROM workspace.polymarket_gold.cohort_retention),
rm AS (SELECT * FROM workspace.polymarket_gold.cohort_retention_monthly),
tr AS (SELECT * FROM workspace.polymarket_gold.cohort_category_transition),
sc AS (SELECT * FROM workspace.polymarket_gold.listing_scorecard),
month_cohorts AS (
    SELECT date_format(first_month, 'yyyy-MM') AS cohort, count(*) AS wallets,
           count_if(first_fill_ts + INTERVAL 63 DAYS <= TIMESTAMP '2026-10-05 00:00:00') AS observable_week_8
    FROM workspace.polymarket_silver.dim_wallet GROUP BY 1
),
tests AS (
    -- staging against silver
    SELECT 'wallet_week_category' AS tbl, 'wallet fills = silver wallet_fills rows' AS test,
           CAST((SELECT wallet_fills FROM silver) AS STRING) AS expected, CAST(wallet_fills AS STRING) AS actual,
           wallet_fills = (SELECT wallet_fills FROM silver) AS passed FROM staging_total
    UNION ALL SELECT 'wallet_week_category', 'attributed fills = silver trades rows', CAST((SELECT fills FROM silver) AS STRING),
           CAST(CAST(fills AS BIGINT) AS STRING), fills = (SELECT fills FROM silver) FROM staging_total
    UNION ALL SELECT 'wallet_week_category', 'attributed notional = silver trades notional (relative difference)', '< 1e-9',
           CAST(abs(notional / (SELECT notional FROM silver) - 1) AS STRING), abs(notional / (SELECT notional FROM silver) - 1) < 1e-9 FROM staging_total
    UNION ALL SELECT 'wallet_week_category', 'cash traded = silver wallet_month (relative difference)', '< 1e-9',
           CAST(abs(cash_traded / (SELECT cash_traded FROM silver) - 1) AS STRING), abs(cash_traded / (SELECT cash_traded FROM silver) - 1) < 1e-9 FROM staging_total
    -- weekly_kpi
    UNION ALL SELECT 'weekly_kpi', 'weeks published (weeks with 10+ active wallets)', CAST((SELECT weeks FROM shown) AS STRING),
           CAST(count(*) AS STRING), count(*) = (SELECT weeks FROM shown) FROM top
    UNION ALL SELECT 'weekly_kpi', 'fills, all-ALL rows = staging for published weeks', CAST(CAST((SELECT fills FROM shown) AS BIGINT) AS STRING),
           CAST(CAST(sum(fills) AS BIGINT) AS STRING), sum(fills) = (SELECT fills FROM shown) FROM top
    UNION ALL SELECT 'weekly_kpi', 'wallet fills, all-ALL rows = staging for published weeks', CAST((SELECT wallet_fills FROM shown) AS STRING),
           CAST(sum(wallet_fills) AS STRING), sum(wallet_fills) = (SELECT wallet_fills FROM shown) FROM top
    UNION ALL SELECT 'weekly_kpi', 'notional, all-ALL rows vs staging (relative difference)', '< 1e-9',
           CAST(abs(sum(notional_shares) / (SELECT notional FROM shown) - 1) AS STRING), abs(sum(notional_shares) / (SELECT notional FROM shown) - 1) < 1e-9 FROM top
    UNION ALL SELECT 'weekly_kpi', 'cash traded, all-ALL rows vs staging (relative difference)', '< 1e-9',
           CAST(abs(sum(cash_traded) / (SELECT cash_traded FROM shown) - 1) AS STRING), abs(sum(cash_traded) / (SELECT cash_traded FROM shown) - 1) < 1e-9 FROM top
    UNION ALL SELECT 'weekly_kpi', 'weeks where active wallets != distinct wallets in staging', '0',
           CAST(count_if(t.active_wallets != s.wallets) AS STRING), count_if(t.active_wallets != s.wallets) = 0
           FROM top t JOIN staging_week s USING (week_start)
    UNION ALL SELECT 'weekly_kpi', 'weeks where new wallets != first fills in dim_wallet (weeks with 10+ new)', '0',
           CAST(count_if(n.wallets >= 10 AND x.active_wallets IS DISTINCT FROM n.wallets) AS STRING),
           count_if(n.wallets >= 10 AND x.active_wallets IS DISTINCT FROM n.wallets) = 0
           FROM new_by_week n LEFT JOIN (SELECT * FROM k WHERE activity_status = 'new' AND segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL') x USING (week_start)
    UNION ALL SELECT 'weekly_kpi', 'weeks where new + returning + resurrected != active', '0',
           CAST(count_if(parts = 3 AND split_sum != total) AS STRING), count_if(parts = 3 AND split_sum != total) = 0
           FROM (SELECT week_start, count_if(activity_status != 'ALL') AS parts,
                        sum(CASE WHEN activity_status != 'ALL' THEN active_wallets END) AS split_sum,
                        max(CASE WHEN activity_status = 'ALL' THEN active_wallets END) AS total
                 FROM k WHERE segment = 'ALL' AND scripted = 'ALL' AND category = 'ALL' GROUP BY week_start)
    UNION ALL SELECT 'weekly_kpi', 'weeks where global whales != 1% of active wallets, rounded up', '0',
           CAST(count_if(whale_wallets_global != ceil(0.01 * active_wallets)) AS STRING),
           count_if(whale_wallets_global != ceil(0.01 * active_wallets)) = 0 FROM top
    UNION ALL SELECT 'weekly_kpi', 'rows with fewer than 10 active wallets', '0', CAST(count_if(active_wallets < 10) AS STRING),
           count_if(active_wallets < 10) = 0 FROM k
    UNION ALL SELECT 'weekly_kpi', 'whale counts between 1 and 9 left in the table', '0',
           CAST(count_if(whale_wallets_global BETWEEN 1 AND 9 OR whale_wallets_in_segment BETWEEN 1 AND 9) AS STRING),
           count_if(whale_wallets_global BETWEEN 1 AND 9 OR whale_wallets_in_segment BETWEEN 1 AND 9) = 0 FROM k
    -- weekly_market_volume
    UNION ALL SELECT 'weekly_market_volume', 'fills = silver trades rows', CAST((SELECT fills FROM silver) AS STRING),
           CAST(fills AS STRING), fills = (SELECT fills FROM silver) FROM mv
    UNION ALL SELECT 'weekly_market_volume', 'notional vs silver trades (relative difference)', '< 1e-9',
           CAST(abs(notional / (SELECT notional FROM silver) - 1) AS STRING), abs(notional / (SELECT notional FROM silver) - 1) < 1e-9 FROM mv
    -- cohort_retention
    UNION ALL SELECT 'cohort_retention', 'World Cup cohort wallets', '226923', CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 226923
           FROM r WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', '2024 election cohort wallets', '218361', CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 218361
           FROM r WHERE cohort_type = 'event' AND cohort = 'us_election_2024' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'Q1 2026 cohort wallets', '713066', CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 713066
           FROM r WHERE cohort_type = 'event' AND cohort = 'benchmark_q1_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'all monthly cohorts together = all wallets', '3192135', CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 3192135
           FROM r WHERE cohort_type = 'month' AND cohort = 'ALL' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'monthly cohorts with 10+ wallets: cohort size differs from dim_wallet', '0',
           CAST(count_if(m.wallets >= 10 AND x.cohort_wallets IS DISTINCT FROM m.wallets) AS STRING),
           count_if(m.wallets >= 10 AND x.cohort_wallets IS DISTINCT FROM m.wallets) = 0
           FROM month_cohorts m LEFT JOIN (SELECT * FROM r WHERE cohort_type = 'month' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0) x USING (cohort)
    UNION ALL SELECT 'cohort_retention', 'rows where week 0 active != observable', '0',
           CAST(count_if(week_n = 0 AND active_wallets != observable_wallets) AS STRING), count_if(week_n = 0 AND active_wallets != observable_wallets) = 0 FROM r
    UNION ALL SELECT 'cohort_retention', 'rows where active > observable or observable > cohort', '0',
           CAST(count_if(active_wallets > observable_wallets OR observable_wallets > cohort_wallets) AS STRING),
           count_if(active_wallets > observable_wallets OR observable_wallets > cohort_wallets) = 0 FROM r
    UNION ALL SELECT 'cohort_retention', 'World Cup cohort fully observable at week 8', '226923', CAST(max(observable_wallets) AS STRING), max(observable_wallets) = 226923
           FROM r WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 8
    UNION ALL SELECT 'cohort_retention', 'World Cup cohort entered via a World Cup market', '129299', CAST(max(entered_via_event_market_wallets) AS STRING),
           max(entered_via_event_market_wallets) = 129299
           FROM r WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'election cohort entered via a 2024 election market', '160958', CAST(max(entered_via_event_market_wallets) AS STRING),
           max(entered_via_event_market_wallets) = 160958
           FROM r WHERE cohort_type = 'event' AND cohort = 'us_election_2024' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'rows with fewer than 10 cohort wallets, or a count of 1-9 shown', '0',
           CAST(count_if(cohort_wallets < 10 OR active_wallets BETWEEN 1 AND 9 OR observable_wallets BETWEEN 1 AND 9) AS STRING),
           count_if(cohort_wallets < 10 OR active_wallets BETWEEN 1 AND 9 OR observable_wallets BETWEEN 1 AND 9) = 0 FROM r
    -- entry_channel: the two channels partition the World Cup and election cohorts, in every week
    UNION ALL SELECT 'cohort_retention', 'World Cup cohort entering via a World Cup market (entry_channel)', '129299',
           CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 129299
           FROM r WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL'
             AND entry_channel = 'via_event_market' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'election cohort entering via a 2024 election market (entry_channel)', '160958',
           CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 160958
           FROM r WHERE cohort_type = 'event' AND cohort = 'us_election_2024' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL'
             AND entry_channel = 'via_event_market' AND week_n = 0
    UNION ALL SELECT 'cohort_retention', 'cohort-weeks where via_event_market + other != ALL (cohort, observable or active wallets)', '0',
           CAST(count_if(parts != 2 OR cw != cw_all OR ow != ow_all OR aw != aw_all) AS STRING),
           count_if(parts != 2 OR cw != cw_all OR ow != ow_all OR aw != aw_all) = 0
           FROM (SELECT cohort, week_n, count_if(entry_channel != 'ALL') AS parts,
                        sum(CASE WHEN entry_channel != 'ALL' THEN cohort_wallets END) AS cw, max(CASE WHEN entry_channel = 'ALL' THEN cohort_wallets END) AS cw_all,
                        sum(CASE WHEN entry_channel != 'ALL' THEN observable_wallets END) AS ow, max(CASE WHEN entry_channel = 'ALL' THEN observable_wallets END) AS ow_all,
                        sum(CASE WHEN entry_channel != 'ALL' THEN active_wallets END) AS aw, max(CASE WHEN entry_channel = 'ALL' THEN active_wallets END) AS aw_all
                 FROM r WHERE cohort_type = 'event' AND cohort IN ('world_cup_2026', 'us_election_2024')
                   AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' GROUP BY cohort, week_n)
    UNION ALL SELECT 'cohort_retention', 'rows with an entry_channel outside the World Cup and election cohorts', '0',
           CAST(count_if(entry_channel != 'ALL' AND cohort NOT IN ('world_cup_2026', 'us_election_2024')) AS STRING),
           count_if(entry_channel != 'ALL' AND cohort NOT IN ('world_cup_2026', 'us_election_2024')) = 0 FROM r
    -- cohort_calendar_baseline
    UNION ALL SELECT 'cohort_calendar_baseline', 'rows (3 cohorts x 2 segments x weeks 0-8)', '54', CAST(count(*) AS STRING), count(*) = 54
           FROM workspace.polymarket_gold.cohort_calendar_baseline
    UNION ALL SELECT 'cohort_calendar_baseline', 'rows where week 0 baseline or cohort retention is not 100%', '0',
           CAST(count_if(week_n = 0 AND (abs(baseline_retention - 1) > 1e-12 OR abs(cohort_retention - 1) > 1e-12)) AS STRING),
           count_if(week_n = 0 AND (abs(baseline_retention - 1) > 1e-12 OR abs(cohort_retention - 1) > 1e-12)) = 0
           FROM workspace.polymarket_gold.cohort_calendar_baseline
    UNION ALL SELECT 'cohort_calendar_baseline', 'rows with a rate outside 0-1, a missing ratio, or a week-0 group under 10 wallets', '0',
           CAST(count_if(NOT (baseline_retention BETWEEN 0 AND 1) OR NOT (baseline_week0_share BETWEEN 0 AND 1)
                         OR relative_retention IS NULL OR smallest_week0_group < 10) AS STRING),
           count_if(NOT (baseline_retention BETWEEN 0 AND 1) OR NOT (baseline_week0_share BETWEEN 0 AND 1)
                    OR relative_retention IS NULL OR smallest_week0_group < 10) = 0
           FROM workspace.polymarket_gold.cohort_calendar_baseline
    UNION ALL SELECT 'cohort_calendar_baseline', 'entry days covered (39, 39 and 90)', '39, 39, 90',
           array_join(array_sort(collect_list(entry_days)), ', '),
           array_join(array_sort(collect_list(entry_days)), ', ') = '39, 39, 90'
           FROM (SELECT cohort, max(entry_days) AS entry_days FROM workspace.polymarket_gold.cohort_calendar_baseline
                 WHERE segment = 'ALL' GROUP BY cohort)
    -- cohort_retention_monthly
    UNION ALL SELECT 'cohort_retention_monthly', 'all monthly cohorts together = all wallets', '3192135', CAST(max(cohort_wallets) AS STRING), max(cohort_wallets) = 3192135
           FROM rm WHERE cohort_type = 'month' AND cohort = 'ALL' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND month_n = 0
    UNION ALL SELECT 'cohort_retention_monthly', 'rows where period 0 active != observable', '0',
           CAST(count_if(month_n = 0 AND active_wallets != observable_wallets) AS STRING), count_if(month_n = 0 AND active_wallets != observable_wallets) = 0 FROM rm
    UNION ALL SELECT 'cohort_retention_monthly', 'rows where active > observable or observable > cohort', '0',
           CAST(count_if(active_wallets > observable_wallets OR observable_wallets > cohort_wallets) AS STRING),
           count_if(active_wallets > observable_wallets OR observable_wallets > cohort_wallets) = 0 FROM rm
    -- the election cohort's last first fill is 5 Nov 2024; + 23 x 30 days = 26 Sep 2026, inside the data
    UNION ALL SELECT 'cohort_retention_monthly', 'election cohort fully observable in periods 0-22', '218361', CAST(min(observable_wallets) AS STRING), min(observable_wallets) = 218361
           FROM rm WHERE cohort_type = 'event' AND cohort = 'us_election_2024' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL'
             AND entry_channel = 'ALL' AND month_n <= 22
    UNION ALL SELECT 'cohort_retention_monthly', 'World Cup cohort: via_event_market + other = all (period 0)', '226923',
           CAST(sum(cohort_wallets) AS STRING), sum(cohort_wallets) = 226923 AND count(*) = 2
           FROM rm WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL' AND scripted = 'ALL'
             AND entry_channel != 'ALL' AND month_n = 0
    UNION ALL SELECT 'cohort_retention_monthly', 'rows with fewer than 10 cohort wallets, or a count of 1-9 shown', '0',
           CAST(count_if(cohort_wallets < 10 OR active_wallets BETWEEN 1 AND 9 OR observable_wallets BETWEEN 1 AND 9) AS STRING),
           count_if(cohort_wallets < 10 OR active_wallets BETWEEN 1 AND 9 OR observable_wallets BETWEEN 1 AND 9) = 0 FROM rm
    -- cohort_category_transition
    UNION ALL SELECT 'cohort_category_transition', 'World Cup cohort base = wallets observable to week 8', '226923', CAST(max(base_wallets) AS STRING), max(base_wallets) = 226923
           FROM tr WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL' AND segment_at_entry = 'ALL'
    UNION ALL SELECT 'cohort_category_transition', 'all wallets observable to week 8',
           CAST((SELECT sum(observable_week_8) FROM month_cohorts) AS STRING), CAST(max(base_wallets) AS STRING),
           max(base_wallets) = (SELECT sum(observable_week_8) FROM month_cohorts)
           FROM tr WHERE cohort_type = 'month' AND cohort = 'ALL' AND entry_category = 'ALL' AND segment_at_entry = 'ALL'
    UNION ALL SELECT 'cohort_category_transition', 'cells where "Any category" + "None" != base (both shown)', '0',
           CAST(count_if(any_w + none_w != base_wallets) AS STRING), count_if(any_w + none_w != base_wallets) = 0
           FROM (SELECT cohort_type, cohort, entry_category, segment_at_entry, max(base_wallets) AS base_wallets,
                        max(CASE WHEN traded_category = 'Any category' THEN wallets END) AS any_w,
                        max(CASE WHEN traded_category LIKE 'None%' THEN wallets END) AS none_w
                 FROM tr GROUP BY 1, 2, 3, 4)
    UNION ALL SELECT 'cohort_category_transition', 'World Cup "Any category" in weeks 1-8 >= week-1 active wallets', 'true',
           CAST(max(t.wallets) >= max(x.active_wallets) AS STRING), max(t.wallets) >= max(x.active_wallets)
           FROM tr t CROSS JOIN (SELECT active_wallets FROM r WHERE cohort_type = 'event' AND cohort = 'world_cup_2026' AND entry_category = 'ALL'
                                 AND segment_at_entry = 'ALL' AND scripted = 'ALL' AND week_n = 1) x
           WHERE t.cohort_type = 'event' AND t.cohort = 'world_cup_2026' AND t.entry_category = 'ALL' AND t.segment_at_entry = 'ALL' AND t.traded_category = 'Any category'
    UNION ALL SELECT 'cohort_category_transition', 'rows with fewer than 10 base wallets, or a count of 1-9 shown', '0',
           CAST(count_if(base_wallets < 10 OR wallets BETWEEN 1 AND 9) AS STRING), count_if(base_wallets < 10 OR wallets BETWEEN 1 AND 9) = 0 FROM tr
    -- listing_scorecard
    UNION ALL SELECT 'listing_scorecard', 'fills = silver trades rows', CAST((SELECT fills FROM silver) AS STRING), CAST(sum(fills) AS STRING),
           sum(fills) = (SELECT fills FROM silver) FROM sc
    UNION ALL SELECT 'listing_scorecard', 'notional vs silver trades (relative difference)', '< 1e-9',
           CAST(abs(sum(notional_shares) / (SELECT notional FROM silver) - 1) AS STRING), abs(sum(notional_shares) / (SELECT notional FROM silver) - 1) < 1e-9 FROM sc
    UNION ALL SELECT 'listing_scorecard', 'one row per listing',
           CAST((SELECT count(DISTINCT CASE WHEN is_recurring_series THEN 'series' ELSE 'event' END, CASE WHEN is_recurring_series THEN series_key ELSE event_id END)
                 FROM workspace.polymarket_silver.dim_market) AS STRING),
           CAST(count(*) AS STRING),
           count(*) = count(DISTINCT listing_type, listing_key)
               AND count(*) = (SELECT count(DISTINCT CASE WHEN is_recurring_series THEN 'series' ELSE 'event' END, CASE WHEN is_recurring_series THEN series_key ELSE event_id END)
                               FROM workspace.polymarket_silver.dim_market) FROM sc
    UNION ALL SELECT 'listing_scorecard', 'wallets acquired shown (cannot exceed all wallets)', '<= 3192135', CAST(sum(wallets_acquired) AS STRING),
           sum(wallets_acquired) <= 3192135 FROM sc
    UNION ALL SELECT 'listing_scorecard', 'listings both never traded and low traction', '0', CAST(count_if(never_traded AND low_traction) AS STRING),
           count_if(never_traded AND low_traction) = 0 FROM sc
    UNION ALL SELECT 'listing_scorecard', 'wallet counts between 1 and 9 shown', '0',
           CAST(count_if(traders BETWEEN 1 AND 9 OR wallets_acquired BETWEEN 1 AND 9 OR existing_traders BETWEEN 1 AND 9
                         OR acquired_wallets_trading_elsewhere BETWEEN 1 AND 9) AS STRING),
           count_if(traders BETWEEN 1 AND 9 OR wallets_acquired BETWEEN 1 AND 9 OR existing_traders BETWEEN 1 AND 9
                    OR acquired_wallets_trading_elsewhere BETWEEN 1 AND 9) = 0 FROM sc
    UNION ALL SELECT 'series_month', 'fills = fills of recurring series in the scorecard',
           CAST((SELECT sum(fills) FROM sc WHERE listing_type = 'series') AS STRING), CAST(sum(fills) AS STRING),
           sum(fills) = (SELECT sum(fills) FROM sc WHERE listing_type = 'series') FROM workspace.polymarket_gold.series_month
    -- privacy and size, over the whole gold schema
    UNION ALL SELECT 'gold schema', 'columns that could identify a wallet', '0', CAST(count(*) AS STRING), count(*) = 0
           FROM workspace.information_schema.columns
           WHERE table_schema = 'polymarket_gold'
             AND (column_name IN ('wallet_id', 'address', 'maker_wallet_id', 'taker_wallet_id', 'first_fill_ts', 'first_block',
                                  'first_log_index', 'transaction_hash')
                  OR column_name LIKE '%address%')
    UNION ALL SELECT 'gold schema', 'largest table, rows (Tableau Public limit 15,000,000)', '< 1000000', CAST(max(n) AS STRING), max(n) < 1000000
           FROM (SELECT count(*) AS n FROM k UNION ALL SELECT count(*) FROM r UNION ALL SELECT count(*) FROM rm
                 UNION ALL SELECT count(*) FROM tr UNION ALL SELECT count(*) FROM sc
                 UNION ALL SELECT count(*) FROM workspace.polymarket_gold.series_month
                 UNION ALL SELECT count(*) FROM workspace.polymarket_gold.weekly_market_volume)
)
SELECT tbl AS table_name, test, expected, actual, coalesce(passed, false) AS passed, current_timestamp() AS run_at
FROM tests
