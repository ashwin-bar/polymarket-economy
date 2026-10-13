# Silver layer plan — Polymarket Player Economy

## Context

Bronze is complete and verified in Databricks (`trades` 1,266,885,846 rows, `users` 2,026,673,731 rows, 48 true-UTC months, `/Volumes/workspace/polymarket/bronze`). Test C and its three follow-ups are done, and the checkpoint decisions are made (World Cup 2026 headline; Jan–Mar 2026 and the 2024 US election as benchmark cohorts). The next layer is silver: typed, chain-timed, deduplicated Delta tables plus the market and wallet dimensions that gold and the three dashboards need. This document is the design for silver.

Approved 13 Oct 2026. Decisions are recorded in the decision log of `docs/PROJECT_BRIEF.md` (9, 23–29). §4 was revised the same day to describe the taxonomy as built (decisions 31–35).

---

## 1. Tables

All managed Delta tables in a new schema `workspace.polymarket_silver` (gold will go in `workspace.polymarket_gold`; bronze stays in the Volume under `workspace.polymarket`). Table names keep the `silver_` prefix below for readability; in the schema they are created without it (`polymarket_silver.trades`, …). Wallet addresses exist only in `silver_dim_wallet`; facts carry an integer `wallet_id`.

| Table | Grain | Key | Rows (expected) | Partitioning |
|---|---|---|---|---|
| `silver_block_anchor` | one anchor block (every 1,000 blocks) | `anchor_block` | 59,074 | none |
| `silver_dim_date` | one UTC day, 2022-11-21 → 2026-10-04 | `date_utc` | ~1,414 | none |
| `silver_dim_market` | one market | `market_id` | 4,049,900 | none |
| `silver_dim_event` | one event (category is decided here) | `event_id` | 1,112,116 | none |
| `silver_market_month` (staging, kept for gold) | one market × UTC month with fills | `(market_id, month_start)` | 2,786,605 | none |
| `silver_dim_wallet` | one wallet | `wallet_id` (and `address` unique) | 3,192,135 | none |
| `silver_trades` | one matched fill (volume) | `(block_number, log_index)` | 1,266,885,846 | `month_start` |
| `silver_wallet_fills` | one wallet-side of a fill (people) | `(block_number, log_index, role)` — to be tested | 2,026,673,731 | `month_start` |
| `silver_wallet_month` | one wallet × UTC month | `(wallet_id, month_start)` | measured in pilot | `month_start` |
| `silver_load_log` | one table × month load | `(table, month_start)` | ~150 | none |
| `silver_dq_results` | one test × table × month × run | — | small | none |

**Columns**

- `silver_block_anchor`: `anchor_block` bigint, `ts_unix` bigint, `next_ts_unix` bigint (the following anchor's time, so interpolation needs one row).
- `silver_dim_date`: `date_utc`, `week_start` (Monday), `month_start`, `first_block`, `end_block`, `is_partial_month` (Nov 2022, Oct 2026), `is_v2` (≥ 28 Apr 2026), `is_known_gap` (29–30 Apr 2026), `event_window` (`world_cup_2026` 11 Jun–19 Jul 2026 / `us_election_2024` / `benchmark_q1_2026` / null).
- `silver_dim_market`: `market_id`, `question`, `slug`, `event_id`, `event_slug`, `event_title`, `closed`, `neg_risk`, `markets_volume`, `source_created_at`, `source_end_date` (kept, labelled unreliable, concern 14); from `silver_trades`: `first_fill_ts`, `last_fill_ts`, `fills`, `notional_shares`, `volume_ratio`, `volume_recon_status` (`within_1pct` / `short` / `over` / `no_fills` / `not_closed`); taxonomy: `category`, `subcategory`, `category_source` (`slug_series` / `gamma_tag` / `slug_keyword` / `uncategorised`), `gamma_tags` array, `gamma_series_slug`, `gamma_series_recurrence`, `is_recurring_series`, `series_key`, `is_world_cup`, `is_us_election_2024`, `taxonomy_version`.
- `silver_dim_event`: `event_id`, `event_slug`, `event_title`, `markets_in_file`, `traded_markets`, `fills`, `notional_shares`, `first_fill_ts`, `last_fill_ts`, `category`, `subcategory`, `category_source`, `in_gamma`, `gamma_tags`, `gamma_series_slug`, `gamma_series_recurrence`, `is_recurring_series`, `series_key`, `world_cup_candidate`, `is_us_election_2024`, `taxonomy_version`. (`is_world_cup` and the final subcategory are on `silver_dim_market`.)
- `silver_market_month`: `market_id`, `month_start`, `fills`, `notional_shares`, `usd_amount`, `first_fill_ts`, `last_fill_ts`. Built from `silver_trades`; the market dimension reads it, and gold uses it for volume by market, category and month without rescanning the fills.
- `silver_dim_wallet`: `wallet_id` bigint, `address`, `first_fill_ts`, `first_date`, `first_month`, `first_market_id`, `entry_category`, `last_date`, `lifetime_fills`, `lifetime_cash_traded`, `first_day_cash`, `first_day_fills`, `is_scripted_cohort`, `is_left_censored` (first seen before 1 Mar 2023, concern 15), `last_seen_v2_eve` (last fill 27 Apr 2026, concern 16), `cohort` (`world_cup_2026` / `us_election_2024` / `benchmark_q1_2026` / null), `entry_subcategory`, `entered_via_world_cup_market` and `entered_via_us_election_2024_market` (first fill in a market with that flag), and the entry profile over the wallet's first 30 days from its first fill: `entry_fills`, `entry_maker_share`, `entry_hours_active`, `entry_hours_available`, `entry_hours_share`, `entry_cash_traded`, `entry_window_complete` (false when fewer than 30 days of data follow the first fill), `segment_at_entry`, `segment_rule_version`, `taxonomy_version`. Built in two stages; the stage 1 staging table `wallet_base` is dropped (decision 37).
- `silver_trades`: `month_start`, `date_utc`, `ts_utc` timestamp, `block_number` bigint, `log_index` int, `market_id`, `maker_wallet_id`, `taker_wallet_id` (null where the address is not in `users`), `price`, `usd_amount`, `token_amount`, `maker_direction`, `taker_direction`, `exchange_version` (V1/V2 from `contract`), `source_timestamp` bigint (audit only).
- `silver_wallet_fills`: `month_start`, `date_utc`, `ts_utc`, `block_number`, `log_index`, `wallet_id`, `role`, `direction`, `market_id`, `price`, `usd_amount`, `token_amount`.
- `silver_wallet_month`: `wallet_id`, `month_start`, `fills`, `maker_fills`, `maker_share`, `days_active`, `hours_active`, `hours_available` (hours from the wallet's first fill in the month to the end of the month's data, so a wallet starting mid-month, and partial months, are not under-classified), `hours_share` = `hours_active / hours_available`, `markets_traded`, `cash_traded`, `cash_staked`, `cash_per_fill`, `segment`, `segment_rule_version` (`v1`).

**Dropped from silver:** `transaction_hash` (57% / 45.5% of bytes), `condition_id`, `event_id`, `asset_id`, `nonusdc_side`. Lineage back to bronze is by `(block_number, log_index)`, which is unique per on-chain event; uniqueness is tested on bronze against `(transaction_hash, log_index)` before the hash is dropped.

## 2. Partitioning and resumability

- Facts and `silver_wallet_month` are partitioned by `month_start`, matching bronze. Each month is written with `replaceWhere month_start = …`, so a re-run replaces exactly one month and never duplicates.
- `silver_load_log` records each loaded table-month (rows, checksums, job run id, seconds). The laptop driver reads it and skips done months, the same pattern as `scripts/13_load_bronze.py` and the Volume manifest.
- Dimensions are small and rebuilt whole (`CREATE OR REPLACE`).
- No Z-order or liquid clustering in v1; add only if a gold build proves slow.

## 3. Chain time

- Source: `D:\polymarket-economy-data\lookup\block_anchors.csv` (59,074 anchors, exactly every 1,000 blocks, 1.2 MB) → `silver_block_anchor`.
- Join: equi-join on `anchor_block = floor(block_number / 1000) * 1000`, broadcast (no range join needed).
  `ts_unix = ts0 + (block_number − anchor_block) × (next_ts − ts0) / 1000`; `ts_utc`, `date_utc` and hour derive from it.
- **Month is authoritative from the bronze partition** (exact boundaries by bisection). Where interpolation puts a fill a few seconds into the neighbouring month, `ts_utc` is clamped to the month's first or last second; the number of clamped rows is a DQ metric.
- Dashboard caveat stays: fills within about ±4 minutes of midnight UTC may land on the neighbouring day (concern 11).

## 4. Market dimension and taxonomy (test H folded in)

**Inputs**
- `markets.parquet` in `bronze/markets/` (0.55 GB, copied byte-for-byte inside Databricks).
- `silver_market_month`, from `silver_trades`.
- Gamma `/events`, fetched by event id from serverless (100 ids per request) and kept slim in `bronze/gamma_events/chunk-NNNN.jsonl` (id, slug, title, tags, series; not the embedded markets or descriptions). Fetched: every traded one-off event, largest notional first, plus the five largest events of each recurring series. The fetch stops before 950 MB received; it covered 160,000 of 219,622 target events (893 MB), and the rest are left unfetched (decision 31).

**Category assignment, per event, in order of precedence** (every market takes its event's category)
1. **`slug_series`**: the event's slug stem (numbers and month names blanked) is shared by 20 or more traded events, so it is a recurring series (`is_recurring_series`, `series_key`). The series takes the most common category among its sampled events, so a series is never split across categories.
2. **`gamma_tag`**: the event's own tags through `config/tag_category_map.csv` (tag slug → category, category priority, subcategory, subcategory priority). The matched tag with the lowest category priority decides the category; within that category the lowest subcategory priority decides the subcategory.
3. **`slug_keyword`**: `config/slug_category_rules.csv`, regular expressions on the event slug, first match wins. Only for events with no usable tags.
4. Otherwise `uncategorised`.

Categories (taxonomy v1, frozen 13 Oct 2026): Sports, Politics, Crypto, Economy & Finance, Culture & Entertainment, Tech & Science, Uncategorised. Weather is under Tech & Science; mention and tweet-count markets are "Mentions & posts" under Culture & Entertainment; Geopolitics is a Politics subcategory; "Soccer: World Cup 2026" is a Sports subcategory; recurring crypto series are "Recurring price series".

Flags: `is_world_cup` (World Cup tag, `fifwc-` match slug or "world-cup" in the slug; still trading on or after 1 Dec 2025; not a club, cricket, esports, chess or 2030 world cup; friendlies excluded) and `is_us_election_2024` (an explicit 2024-election tag, or a US-election tag on an event whose last fill is before 1 Feb 2025).

Changes from the approved plan: recurring series take their category from Gamma rather than all being Crypto (decision 32); there is no "event sibling" step because category is per event (decision 33).

**Test H (coverage check), run 13 Oct 2026** — results in brief §9 concern 5 and `eda\H_taxonomy_coverage_v1.json`
- Share of notional shares, of fills and of markets classified, by method and by year, with 2025–26 as the headline figure.
- Top 50 uncategorised events by notional, to extend the rules.
- Share of wallets whose first fill is in an uncategorised market (this caps how good "entry category" can be).

## 5. Wallet-month segmentation and the scripted flag

**Rule shape (decision 19):** market maker / high-frequency taker / retail.
- **Monthly segment** (`silver_wallet_month.segment`) for the KPI dashboard.
- **`segment_at_entry`** (`silver_dim_wallet`) for the cohort dashboard: the same rule applied to each wallet's first 30 days from its first fill, so a cohort's composition is fixed at acquisition. Its `hours_share` uses the hours in that 30-day window (fewer if the data ends first).
- In both, `hours_share` counts hours from the wallet's first fill in the period, not from the start of the period. The fills threshold is scaled for the 30-day window in the same way as for a month.

**Rule v1 (decision 11, set from the calibration grid on 13 Oct 2026)**
- *Automated* = (`fills ≥ 1,000` in the period **and** `hours_share ≥ 0.25`) **or** `fills ≥ 10,000`.
- *Market maker* = automated and `maker_share ≥ 0.50`; *high-frequency taker* = automated and `maker_share < 0.50`; *retail* = everyone else.
- Known limit: wallets with 1,000–9,999 fills active in under a quarter of hours stay retail (about 4–6% of fills in the calibrated months).
- The same numbers apply to the 30-day entry window. They are not scaled for the partial months (Nov 2022, Oct 2026) or for an incomplete entry window (`entry_window_complete = false`), which are under-classified.
- Calibration (`scripts/dbx_silver_segment_calibration.py`, results in `eda\silver_segment_calibration.json`): Dec 2024, Mar 2026, Jun 2026, Jul 2026 and Sep 2026 over `hours_share` {0.25, 0.50, 0.75, 0.90} × `fills` {300, 1,000, 3,000} × `maker_share` split {0.5, 0.7}, with wallets, share of fills, share of cash traded and the share of wallets keeping their segment the next month.

**Cohort retention (for gold, fixed here because silver must support it):** measured in weeks since each wallet's own first fill (week 0 = the first 7 days), capped at week 8 for every cohort, so the World Cup, Q1 2026 and election cohorts are compared over the same horizon. A wallet whose first fill is less than 8 full weeks before 4 Oct 2026 has its later weeks marked not observable, not zero.

**Cohort definitions:** World Cup = first fill 11 Jun–19 Jul 2026; election benchmark = first fill 28 Sep–5 Nov 2024 (both 39 days); Q1 2026 benchmark = first fill Jan–Mar 2026. The window alone defines the cohort; `entered_via_world_cup_market` and entry category are filters.

**Scripted-cohort flag (decisions 26 and 36):** 20 or more wallets first seen on the same UTC day with an identical first-day cash amount of at least $100. A whole multiple of $50 is exempt **only while fewer than 200 wallets share that day and amount**. This exempts ordinary $100 and $200 first days, keeps $999.50, $1,001.50, $10,169.40, $30,538.80 and $31,137.60, and flags the 460 wallets of 26 Jan 2026 at exactly $100.

## 6. Data quality tests and reconciliations

Hard failures stop the load; everything is written to `silver_dq_results`.

| Table | Tests |
|---|---|
| `silver_block_anchor` | 59,074 rows; blocks exactly 1,000 apart; time strictly increasing |
| `silver_trades` | rows per month = bronze manifest (total 1,266,885,846); exact integer checksums of `block_number`, `log_index`, `usd_amount` and `token_amount` in millionths = bronze (same method as `scripts/dbx_month_stats.py`); `(block_number, log_index)` unique; no null `ts_utc`, `market_id`; `ts_utc` inside the month; clamped-row count; every `market_id` in `silver_dim_market` |
| `silver_wallet_fills` | rows per month = bronze (total 2,026,673,731); same checksums; key unique; `role` ∈ {maker, taker}, `direction` ∈ {BUY, SELL}; no null `wallet_id`; maker rows per month = `silver_trades` rows per month (concern 7) |
| `silver_dim_wallet` | 3,192,135 rows; `address` unique; no exchange-contract address present; `first_date ≤ last_date`; lifetime fills sum = `silver_wallet_fills` rows |
| `silver_dim_market` | 4,049,900 rows; `market_id` unique; `category` never null; **reconciliation:** notional shares vs `markets_volume` by period reproduces the V2 follow-up (96.6% / 92.6% within 1%), with the 29–30 April gap and the crypto over-count reported as known issues |
| `silver_wallet_month` | sums of fills and cash per month = `silver_wallet_fills`; `hours_active ≤ hours_available`; every row has a segment; segment shares per month reported |
| Cross-layer | new wallets per day from silver = test C daily table (`eda\C_wallet_identity_daily.csv`); Dec 2024 and Oct 2026 wallet counts = 358,846 and 83,592 (concern 7) |

## 7. Run order and expected compute

| # | Step | Compute (known or to be measured) |
|---|---|---|
| 1 | Small sources: anchors → Volume; `markets.parquet` → bronze; Gamma reachability test, then fetch | seconds to a few minutes each |
| 2 | `silver_block_anchor`, `silver_dim_date` | under 1 min |
| 3 | **Test F pilot:** `silver_trades` + `silver_wallet_fills` for Dec 2024, then March 2026 (largest), with DQ | unknown — this is what test F measures. Reference: a 3-column Spark scan of all `users` took 127 s; a full-history two-pass wallet job took 241 s |
| 4 | **Checkpoint:** time and DBU per month → confirm history window (decision 10) | — |
| 5 | `silver_dim_wallet` stage 1 (ids, first/last fill) | about one full `users` scan |
| 6 | Fact load, one month per job, oldest first, March 2026 last. **Reminder to you at the start of this step: run test G (Tableau Free Edition → SQL warehouse) while it loads, before gold** | extrapolated from step 3 |
| 7 | `silver_dim_market` + taxonomy + test H → **checkpoint** on categories | one aggregation of `silver_trades` |
| 8 | `silver_wallet_month` + calibration grid → **checkpoint** on thresholds | one pass of `silver_wallet_fills` |
| 9 | `silver_dim_wallet` stage 2 (entry category, flags, 30-day entry profile, `segment_at_entry`) | one pass of `silver_wallet_fills` joined to first-fill time |
| 10 | Full DQ and reconciliation run; brief, README, decision log | a few scans |

**DBU cost is not known.** Billing lags by more than 12 hours and has costed nothing beyond test E; the bronze load and test C must be costed before step 6 so the full fact load is sized against real figures. Compute has not been cut off so far.

**Code**: Databricks notebooks `scripts/dbx_silver_*.py` (one per table) and one laptop driver `scripts/16_load_silver.py`, reusing the submit / poll / retry / idempotency pattern in `scripts/13_load_bronze.py` and the day and month bounds helpers in `scripts/14_test_c.py`.

## 8. Decisions (settled 13 Oct 2026)

1. Fill-level silver for the full history, after the test F pilot (closes decision 10 once the pilot cost is seen).
2. `transaction_hash` is dropped from silver; key `(block_number, log_index)`, lineage through bronze.
3. Approved uploads: `block_anchors.csv`, and the Gamma extract if Gamma is not reachable from serverless.
4. Categories as in §4; Sports carries a soccer / World Cup subcategory.
5. Cohorts are defined by the window alone, with an "entered via World Cup market" filter; 39-day windows.
6. `segment_at_entry` (first 30 days) for cohorts; monthly segment for the KPI dashboard.
7. Scripted flag exempts whole multiples of $50.
8. Schemas `workspace.polymarket_silver` and `workspace.polymarket_gold`; bronze stays where it is.
9. Retention in weeks since first fill, capped at week 8 for all cohorts.

Decision 11 (segment thresholds) was settled on 13 Oct 2026; nothing in this plan is still open.

## Verification

- Step 3 pilot: Dec 2024 silver must reproduce 11,399,126 `trades` rows, 21,104,996 `users` rows, 358,846 wallets and the bronze checksums already recorded for that month.
- After step 6: `silver_dq_results` shows no hard failures; totals equal the bronze manifest.
- After step 7: test H coverage table reviewed with you.
- After step 8: calibration table reviewed with you; decision 11 recorded.
- Cross-check: silver's new-wallets-per-day equals test C's saved daily table.
