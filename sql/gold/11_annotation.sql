-- gold.annotation (docs/GOLD_PLAN.md §3): reference lines and bands for the time-series charts.
-- Dates are UTC days; a single-day event has date_from = date_to.

CREATE OR REPLACE TABLE workspace.polymarket_gold.annotation AS
SELECT CAST(date_from AS DATE) AS date_from, CAST(date_to AS DATE) AS date_to, label, kind, note
FROM VALUES
    ('2026-04-28', '2026-04-28', 'V2 exchange cutover', 'data break',
     'Active established wallets step down on this day (74,041 to 47,814). Annotated, not adjusted.'),
    ('2026-04-29', '2026-04-30', 'Known gap', 'data gap',
     'Fills are short of the published market volume for markets first traded on these two days.'),
    ('2026-05-04', '2026-05-04', 'Deposit Wallet launch', 'platform event',
     'No spike in new wallets followed; new wallets are not migrated established wallets.'),
    ('2022-11-21', '2022-11-30', 'Partial month', 'partial period',
     'Data starts on 21 Nov 2022. Monthly figures and wallet segments understate this month.'),
    ('2026-10-01', '2026-10-04', 'Partial month', 'partial period',
     'Data ends on 4 Oct 2026. Monthly figures and wallet segments understate this month.'),
    ('2022-11-21', '2023-02-28', 'Left-censored start', 'partial period',
     'Wallets first seen here may have traded before the data begins.'),
    ('2024-09-28', '2024-11-05', '2024 US election window', 'cohort window',
     'Benchmark cohort: wallets whose first fill falls in this window.'),
    ('2026-01-01', '2026-03-31', 'Q1 2026 benchmark', 'cohort window',
     'Benchmark cohort: wallets whose first fill falls in this window.'),
    ('2026-06-11', '2026-07-19', 'World Cup 2026', 'cohort window',
     'Headline cohort: wallets whose first fill falls in this window.')
    AS t(date_from, date_to, label, kind, note)
