# Gold layer plan — Polymarket Player Economy

## Context

Silver is complete and verified (13 Oct 2026): fill-level `trades` (1,266,885,846 rows) and `wallet_fills` (2,026,673,731), `dim_wallet` (3,192,135, with cohort, entry category, segment at entry, scripted flag), `dim_market` / `dim_event` (taxonomy v1), `wallet_month` (segment rule v1), `market_month`, `dim_date`; 53 DQ tests passing. Gold is the last warehouse layer: small, pre-aggregated tables in `workspace.polymarket_gold` that feed three Tableau dashboards and a methodology page, live from Tableau Free Edition and as CSV files for Tableau Public Edition. Approved 13 Oct 2026 with the changes folded in below (decisions in §9; recorded in the decision log of `docs/PROJECT_BRIEF.md` as each table is built).

Data span note: the data runs Monday 21 Nov 2022 to Sunday 4 Oct 2026, exactly 202 complete ISO weeks (Monday start, as in `dim_date.week_start`). No week is partial.

---

## 1. Layout and how gold is built

- **SQL files in the repo**, one per table, numbered in run order: `sql/gold/NN_<table>.sql`. Each file is one or more Databricks SQL statements (`CREATE OR REPLACE TABLE … AS`), using window functions for activity status, cohort weeks and retention.
- **Runner** `scripts/17_build_gold.py`: sends each file's statements to the SQL warehouse (`748ff4e9a4a1ce9b`) through the Statement Execution API, polls to completion, logs rows and seconds to `eda\gold_runs.jsonl`, then runs the gold tests (§7). Same resumable, logged pattern as `scripts/16_load_silver.py`. If the 2X-Small warehouse is too slow for the two wallet-level scans, the same SQL runs unchanged from a serverless notebook job (fallback; timing is measured in the pilot).
- **Exporter** `scripts/18_export_gold.py`: writes every gold table to `D:\polymarket-economy-data\gold_export\<table>.csv` for Tableau Public Edition (§8).
- **Two wallet-level staging tables stay in silver** (they carry `wallet_id`, so they cannot be gold): §2. Gold itself has no wallet id or address anywhere.

**Dashboard rule: every filter is single-value, with "All" as its default.** The cube tables (`weekly_kpi`, `cohort_retention`, `cohort_retention_monthly`, `cohort_category_transition`) hold a ready-made `ALL` row for every dimension. A sheet must therefore fix each dimension to exactly one value, `ALL` unless the user picks another, and must never sum over a dimension's rows: adding `ALL` to its parts double-counts, and wallets that trade several categories would be counted once per category. Multi-select filters are not offered.

## 2. Silver staging (wallet-level, not published)

| Table | Grain | Built from | Rows (estimate) | Used by |
|---|---|---|---|---|
| `polymarket_silver.wallet_week_category` | wallet × week × category | `wallet_fills`, `trades`, `dim_market` | 30,604,057 | weekly KPIs, whale share |
| `polymarket_silver.wallet_entry_week` | wallet × week since first fill (0–8) × category | `wallet_fills`, `dim_wallet`, `dim_market` | about 10M (measured at build) | retention, transition matrix |
| `polymarket_silver.wallet_entry_month` | wallet × 30-day period since first fill (0–23) | `wallet_fills`, `dim_wallet` | about 10M (measured at build) | monthly retention |
| `polymarket_silver.wallet_listing_month` | wallet × listing (one-off event or recurring series) × month | `wallet_fills`, `dim_market` | measured at build | listing scorecard, series by month |

Columns
- `wallet_week_category`: `wallet_id`, `week_start`, `category`, `first_month_in_week` (month of the wallet's first fill that week, for its segment), `wallet_fills` (rows in `wallet_fills`, both sides), `cash_traded`, `cash_staked`, `fills_attributed`, `notional_attributed` (from `trades`: each matched fill and its shares split half to the maker wallet, half to the taker wallet, so any split sums back to the headline).
- `wallet_entry_week`: `wallet_id`, `week_n` = floor((`ts_utc` − `first_fill_ts`) / 7 days), kept for 0–8, `category`, `wallet_fills`, `cash_traded`.

## 3. Gold tables

Row counts are upper bounds from the dimension sizes; actuals are recorded at build. "ALL" rows are explained under each table.

### Dashboard 1 — weekly KPIs

**`gold.weekly_kpi`** — grain: `week_start` × `activity_status` × `segment` × `scripted` × `category`, **each dimension carrying an `ALL` value** (a cube computed from wallet-level data before suppression). At most 202 × 4 × 4 × 3 × 8 = 77,568 rows; 36,814 as built. The dashboard picks one value or `ALL` per dimension and never sums across rows of a dimension, so every headline figure is exact; only fine cells under 10 wallets are missing.

| Column | Definition |
|---|---|
| `week_start` | Monday of the UTC week |
| `activity_status` | Per wallet and week. **new**: the wallet's first-ever fill is in this week. **returning**: not new, and active in the previous week. **resurrected**: not new, and not active in the previous week (`LAG(week_start)` over the wallet's active weeks ≠ this week − 7 days) |
| `segment` | The wallet's monthly segment (rule v1, `wallet_month`) for the month of its first fill in that week, so every wallet-week has exactly one segment |
| `scripted` | `scripted` / `not scripted` from `dim_wallet.is_scripted_cohort`, or `ALL` |
| `category` | Taxonomy v1 category of the market traded, or `ALL` |
| `active_wallets` | Distinct wallets with at least 1 fill in the week (in that category, or any for `ALL`) |
| `wallet_fills` | Rows in `wallet_fills` (each wallet side of each match) |
| `fills` | Matched fills from `trades`, half attributed to each side's wallet |
| `notional_shares` | `trades.token_amount`, half attributed to each side's wallet |
| `cash_traded` | Sum of `usd_amount` over the wallets' fills, buys and sells (brief §7) |
| `cash_staked` | Sum of `usd_amount` on BUY fills only |
| `whale_wallets_in_segment`, `whale_cash_traded_in_segment` | Wallets in the top 1% of the week's active wallets **within their own segment**, and their cash traded in this cell (the dashboard default) |
| `whale_wallets_global`, `whale_cash_traded_global` | The same with the top 1% taken across all of the week's active wallets |

Derived in Tableau as ratios of sums, so they stay correct under any filter:
- **Volume per trader** = `SUM(cash_traded) / SUM(active_wallets)`.
- **Whale share** = `whale_cash_traded_in_segment / cash_traded` (default) or `whale_cash_traded_global / cash_traded`. A wallet is a whale in a week if it is among the top `CEIL(0.01 × N)` of the N wallets active that week, ranked by cash traded across all categories, either within its segment (`is_whale_in_segment`) or over all wallets (`is_whale_global`); ties are broken by wallet id. With a filter on, the figure reads "share of this slice's cash that comes from the week's top-1% wallets".

Wallet counts cannot be summed across categories (a wallet can trade several), which is why `ALL` rows are computed from wallet-level data rather than added up. Fills, notional and cash would add, but the dashboard uses the `ALL` rows for them too, for consistency.

**`gold.weekly_market_volume`** — the market-side view, each fill once, no wallets. Grain: `week_start` × `category` × `subcategory` × `is_recurring_series` × `is_world_cup` × `is_us_election_2024` × `exchange_version`. Columns: `fills`, `notional_shares`, `usd_amount`, `markets_traded`, `events_traded`, `markets_first_traded` (first fill that week). About 202 × ~45 combinations ≈ 10,000 rows. Gives the headline notional series and the subcategory breakdown (e.g. "Soccer: World Cup 2026").

**`gold.dim_week`** — 202 rows: `week_start`, `week_end`, `month_start` (of the Monday), `is_v2` , `contains_v2_cutover` (28 Apr 2026), `contains_known_gap` (29–30 Apr 2026), `in_partial_month` (Nov 2022, Oct 2026: segment thresholds under-classify), `event_window`, `source_clock_unreliable` (Oct 2024–Oct 2025, informational).

**`gold.annotation`** — about 10 rows for chart reference lines and bands: `date_from`, `date_to`, `label`, `kind`, `note`. Rows: V2 cutover 28 Apr 2026 (veteran step, annotated not adjusted); known gap 29–30 Apr 2026; Deposit Wallet launch 4 May 2026; partial months Nov 2022 and Oct 2026; World Cup window 11 Jun–19 Jul 2026; election window 28 Sep–5 Nov 2024; Q1 2026 benchmark; left-censored start (before 1 Mar 2023).

### Dashboard 2 — cohorts and cross-sell

**`gold.cohort_retention`** — grain: `cohort_type` × `cohort` × `entry_category` × `segment_at_entry` × `scripted` × `week_n`, built with `GROUPING SETS` so that **`ALL` rows for cohort, entry category, segment at entry and scripted flag are computed before suppression**: headline curves are exact and only fine cells are suppressed. At most (4 + 49) × 8 × 4 × 3 × 9 ≈ 45,800 rows.

| Column | Definition |
|---|---|
| `cohort_type`, `cohort` | `event`: `world_cup_2026` (first fill 11 Jun–19 Jul 2026), `us_election_2024` (28 Sep–5 Nov 2024), `benchmark_q1_2026` (Jan–Mar 2026). `month`: month of first fill (`first_month`). A wallet is in one monthly cohort and at most one event cohort |
| `entry_category`, `segment_at_entry`, `scripted` | From `dim_wallet` (fixed at acquisition), or `ALL` |
| `week_n` | 0–8. Week n covers days 7n to 7n+6 from the wallet's own first fill (decision 27) |
| `cohort_wallets` | Wallets in the cell |
| `observable_wallets` | Wallets whose week n lies wholly inside the data: `first_fill_ts + (n+1) × 7 days ≤ 5 Oct 2026 00:00` |
| `active_wallets` | Observable wallets with at least 1 fill in week n. Week 0 is 100% by construction |
| `wallet_fills`, `cash_traded` | Those wallets' fills and cash traded in week n |
| `entered_via_event_market_wallets` | Wallets in the cell whose first fill was in a World Cup market (or a 2024 election market for the election cohort); null for other cohorts |
| `is_left_censored_cohort` | Monthly cohorts before Mar 2023 (concern 15) |

Derived in Tableau: **retention (week n)** = `SUM(active_wallets) / SUM(observable_wallets)`; **cash traded per acquired wallet** = `SUM(cash_traded) / SUM(observable_wallets)`. The dashboard reads the `ALL` rows for headline curves and a single value for a split; it does not sum rows. The World Cup cohort is fully observable to week 8 (latest first fill 19 Jul + 63 days = 20 Sep 2026).

**`gold.cohort_retention_monthly`** — the long view. Same dimensions, `ALL` rows and observable-wallets logic as `cohort_retention`, with `month_n` 0–23 instead of `week_n`: period n covers days 30n to 30n+29 from the wallet's own first fill. `observable_wallets` = wallets with `first_fill_ts + (n+1) × 30 days ≤ 5 Oct 2026 00:00`. At most (4 + 49) × 8 × 4 × 3 × 24 ≈ 122,000 rows. The World Cup cohort is observable for periods 0–1 in full and period 2 only for first fills up to 7 Jul 2026; the election cohort for periods 0–22 in full and period 23 only for first fills up to 15 Oct 2024.

**`gold.cohort_category_transition`** — grain: `cohort_type` × `cohort` × `entry_category` × `segment_at_entry` × `traded_category`, with `ALL` rows for cohort, entry category and segment at entry computed before suppression. At most (4 + 49) × 8 × 4 × 9 ≈ 15,300 rows.

| Column | Definition |
|---|---|
| `traded_category` | A taxonomy category, or `None (did not trade in weeks 1–8)` |
| `base_wallets` | Wallets in the cohort cell that are observable through week 8 (first fill at least 63 days before the data ends) |
| `wallets` | Of those, wallets with at least 1 fill in `traded_category` during weeks 1–8 (the entry category itself is one of the columns: staying put) |
| `wallet_fills`, `cash_traded` | Their fills and cash in that category in weeks 1–8 |

Derived: **transition share** = `wallets / base_wallets`. A row's shares do not sum to 100% (a wallet can trade several categories). **Cross-sell rate** = share with at least 1 fill in weeks 1–8 in a category other than the entry category, given as its own `traded_category` value `Any other category`.

### Dashboard 3 — listing scorecard

**`gold.listing_scorecard`** — grain: one listing = one one-off event, or one recurring series (`series_key`, decision 12). About 218,000 traded one-off events + about 345 series; with never-traded one-off events (decision D7) up to about 350,000 rows.

| Column | Definition |
|---|---|
| `listing_type`, `listing_key`, `listing_title` | `event` (event id, slug, title) or `series` (slug stem, example title) — public market information, no wallets |
| `category`, `subcategory`, `is_world_cup`, `is_us_election_2024` | Taxonomy v1 |
| `events`, `markets`, `traded_markets` | 1 event for a one-off; number of events for a series |
| `first_fill_date`, `last_fill_date`, `active_life_days` | From `trades` (decision 13: not `markets.end_date`). Active life = last − first + 1 |
| `fills`, `notional_shares` | From `trades`, each fill once |
| `traders` | Distinct wallets with at least 1 fill in the listing |
| `wallets_acquired` | Wallets whose first-ever fill was in this listing (`dim_wallet.first_market_id`) |
| `existing_traders` | `traders − wallets_acquired` |
| `acquired_share` | `wallets_acquired / traders` (share new vs existing) |
| `acquired_wallets_trading_elsewhere` | Acquired wallets that later filled in any other listing (does the listing feed the platform or stand alone) |
| `traders_band`, `acquired_band` | `0`, `1–9`, `10–99`, `100–999`, `1,000–9,999`, `10,000+`. Exact counts under 10 are suppressed; the band is always shown |
| `never_traded` | The listing is in `markets.parquet` but has no fill at all (about 128,000 events); kept as zero rows, separate from `low_traction` |
| `low_traction` | Flag for "attracts almost no one" among listings that did trade: fewer than 10 distinct traders over its life (one-off), or a median of fewer than 10 traders per event (series). False when `never_traded` |
| `volume_recon_status_share` | Share of the listing's closed markets within 1% of `markets.volume` (data-quality context) |

**`gold.series_month`** — recurring series over time. Grain: `series_key` × `month_start`: `events`, `traded_markets`, `fills`, `notional_shares`, `traders`, `wallets_acquired`, `median_traders_per_event`. About 345 × up to 20 months ≈ 4,000 rows.

### Methodology page

| Table | Grain | Rows | Content |
|---|---|---|---|
| `gold.metric_definition` | one metric | about 25 | Name, exact definition, source table, caveat. Written as a `VALUES` list in SQL so the definitions in the dashboard are the ones in the repo. States that **notional shares is the platform headline** (what Polymarket and `markets.volume` report) and labels `cash_traded` as **"cash traded by wallets (both sides)"**, not comparable with the headline |
| `gold.recon_monthly` | one UTC month | 48 | Bronze vs silver rows for both facts, maker rows = trades rows, wallets active, notional, V2 rows, largest source-clock error, rows on a month's first or last second, partial-month flag (from `load_log` and `dq_results`) |
| `gold.recon_volume` | period (pre / spans / post cutover) × status | about 10 | Markets by `volume_recon_status`, ratio of totals, share within 1% (92.6% / 96.6%) |
| `gold.taxonomy_coverage` | year × `category_source` | about 20 | Markets, fills, notional and their shares (test H), plus entering wallets by cohort |
| `gold.dq_summary` | one test | about 60 silver + gold tests | Table, test, expected, actual, passed, from the latest `dq_results` run and the gold tests |
| `gold.suppression_log` | gold table | about 8 | Cells and wallets suppressed or folded into residual rows, and the share of each measure affected |
| `gold.segment_summary` | month × segment | 144 | Wallets, fills share, cash share (how big each segment is; context for the segment filter) |

## 4. Small-cell suppression (under 10 wallets)

Applied inside gold, so the live and the exported versions are the same.
- **`ALL` rows first.** `weekly_kpi`, `cohort_retention`, `cohort_retention_monthly` and `cohort_category_transition` are cubes: every `ALL` combination is aggregated from wallet-level staging before anything is suppressed, so totals and headline curves are exact and do not depend on the fine cells.
- **Fine cells:** a row whose base count (`active_wallets`, `cohort_wallets`, `base_wallets`) is under 10 is removed. There is no residual row; the `ALL` rows carry the totals.
- **Counts inside a kept row** (`active_wallets` in a late week of a small cohort cell, `wallets` in the transition matrix, whale counts): values 1–9 are set to null together with the measures of those wallets; 0 stays 0.
- **Listing scorecard:** exact `traders` and `wallets_acquired` under 10 are null; the band column says `1–9`.
- `gold.suppression_log` reports rows removed and values nulled per table. No gold column holds a wallet id, an address, a first-fill timestamp or anything else at wallet grain; the gold tests check this.

## 5. Run order and expected compute

| # | Step | Compute |
|---|---|---|
| 1 | `sql/gold/00_wallet_week_category.sql` — **pilot on one month (Dec 2024) first**, then all | two scans of the fact tables with a join to `dim_market`; silver's comparable jobs took 80–115 s on serverless; warehouse time is what the pilot measures |
| 2 | `01_wallet_entry_week.sql`, `02_wallet_entry_month.sql` | one scan each of `wallet_fills` joined to `dim_wallet` (stage 2 did this in 28 s) |
| 3 | `10_dim_week.sql`, `11_annotation.sql`, `12_weekly_kpi.sql`, `13_weekly_market_volume.sql` | from staging; `13` is one scan of `trades` |
| 4 | **Checkpoint:** weekly KPI numbers against known figures before the cohort tables | — |
| 5 | `20_cohort_retention.sql`, `21_cohort_retention_monthly.sql`, `22_cohort_category_transition.sql` | from the entry staging tables |
| 6 | `30_listing_scorecard.sql`, `31_series_month.sql` | one scan of `wallet_fills` (distinct wallets per listing) |
| 7 | `40_…` methodology tables | small |
| 8 | Gold tests, then CSV export; brief, README, decision log | small |

DBU cost is still unknown (billing lag); each run is logged with its statement or job run id so cost can be attached.

## 6. Critical files

- New: `sql/gold/*.sql` (about 16 files), `scripts/17_build_gold.py`, `scripts/18_export_gold.py`, `docs/GOLD_PLAN.md` (this plan, as approved).
- Reused: the Databricks CLI wrapper `cli()` in `scripts/14_test_c.py`; the run-log and save pattern in `scripts/16_load_silver.py` (`run_notebook`, `save`); the Statement Execution API call already proven in this project (warehouse id `748ff4e9a4a1ce9b`); DQ result shape of `polymarket_silver.dq_results`.
- Updated: `docs/PROJECT_BRIEF.md` (§7 metric definitions for activity status, whale share and attribution; decision log), `.gitignore` unchanged (CSV exports stay on D:).

## 7. Verification (gold tests, run by `17_build_gold.py`)

- **Totals back to silver:** `weekly_kpi` `ALL` rows summed over every dimension: `fills` = 1,266,885,846; `wallet_fills` = 2,026,673,731; `notional_shares`, `cash_traded`, `cash_staked` equal silver totals (relative 1e-9); category rows sum to the same. `weekly_market_volume` fills and notional equal `trades`.
- **Wallet counts:** per week, `SUM(active_wallets)` over `ALL` rows equals distinct wallets in `wallet_fills` that week; `new` wallets over all weeks = 3,192,135; new wallets per week equal `dim_wallet.first_date` rolled to weeks (and so test C).
- **Whale definition:** global whale wallets per week = `CEIL(0.01 × active wallets)`; in-segment whales per week and segment = `CEIL(0.01 × that segment's active wallets)`.
- **Cohorts:** `cohort_wallets` at week 0 = 226,923 / 218,361 / 713,066; monthly cohorts sum to 3,192,135; week 0 retention = 100%; `active ≤ observable ≤ cohort`; World Cup cohort fully observable at week 8; `entered_via_event_market_wallets` = 129,299 and 160,958.
- **Transition:** `base_wallets` consistent with retention's week-8 observable count; staying-put share ≤ 100%.
- **Scorecard:** `SUM(wallets_acquired)` = 3,192,135 before suppression; fills and notional sum to `trades`; listings count = one-off events + series.
- **Cube consistency:** in every cube, the `ALL` row equals the sum of its unsuppressed-and-suppressed parts as recomputed from staging.
- **Privacy:** no column named or typed like a wallet id or address in any gold table; no kept cell with a base count under 10; every table under 1M rows (limit 15M); `suppression_log` shows the affected share.
- Checked in Tableau Free Edition live on one table, and the CSV row counts equal the tables.

## 8. CSV exports for Tableau Public Edition

`scripts/18_export_gold.py` downloads each gold table through the SQL warehouse to `D:\polymarket-economy-data\gold_export\<table>.csv` (UTF-8, header row, ISO dates), writes `_export_manifest.json` (table, rows, bytes, SHA-256, exported at), and stops if a table has a forbidden column name, more than 15M rows, or a row count different from Databricks. Expected total size is well under 200 MB (the listing scorecard is the only table over 100,000 rows), so it stays far below the 1 GB ask-first limit. Files stay on D:, not in the repo.

## 9. Decisions (settled 13 Oct 2026)

| # | Decision |
|---|---|
| D1 | Matched fills and notional are attributed half to the maker wallet and half to the taker wallet, from `trades`, so every filter sums to the headline |
| D2 | A wallet's segment in a week is its monthly segment for the month of its first fill in that week |
| D3 | Resurrected = active this week, not new, not active the previous week |
| D4 | Two whale flags: `is_whale_global` and `is_whale_in_segment` (top 1% of the week's active wallets within the same segment, by cash traded). The dashboard defaults to within-segment |
| D5 | The category filter is single-select (plus All) |
| D6 | Suppression: `ALL` rows are computed by `GROUPING SETS` before suppression, so headline figures are exact; only fine cells under 10 wallets are suppressed, and counts of 1–9 inside kept rows are nulled |
| D7 | The scorecard includes never-traded listings as zero rows with a `never_traded` flag, distinct from `low_traction` |
| D8 | `low_traction` = fewer than 10 distinct traders over the listing's life (series: median under 10 per event) |
| D9 | Wallet-level staging for gold lives in `polymarket_silver` (`wallet_week_category`, `wallet_entry_week`, `wallet_entry_month`) |
| D10 | Repo: `sql/gold/`, `scripts/17_build_gold.py`, `scripts/18_export_gold.py`, this document; gold runs on the SQL warehouse, with a notebook-job fallback |
| D11 | Monthly cohorts: all 48 months, with a left-censored flag before Mar 2023 and observable counts for recent months |
| — | Added on approval: `gold.cohort_retention_monthly` (30-day periods 0–23 since first fill); `metric_definition` names notional shares as the platform headline and labels cash traded as "cash traded by wallets (both sides)" |
