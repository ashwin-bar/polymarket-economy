# Polymarket Player Economy — Project Brief

*The guiding document for this project. Last updated 12 October 2026.*

---

## 1. Purpose

A portfolio project aimed at **analytics roles** (data/product/BI analyst) in prediction markets, sports betting, iGaming and gaming. It sits alongside two other exhibits:

| Exhibit | Signal |
|---|---|
| MSc dissertation (VPIN on Polymarket order flow) | Microstructure, econometrics |
| Kalshi hierarchical demand forecast (keystone, in progress) | Forecasting, demand planning, ML tooling |
| **This project** | **Big data, ETL, warehouse modelling, Tableau dashboards, business insight** |

**What must come through clearly:**
1. Pulling from a large data store (Databricks lakehouse built from a 1B+ row source).
2. A real ETL pipeline (bronze → silver → gold, incremental, tested, reconciled).
3. Interactive Tableau dashboards that answer a stakeholder's question.

**Accuracy stance:** the analysis does not need to be definitive. It does need to be **honest**: every known data limitation is measured where possible, documented, and shown on the dashboard where it affects a number. Interviewers trust stated limitations more than clean-looking results.

---

## 2. The headline question

> **"Tourists or residents? Polymarket's player economy after the World Cup."**

You are the analyst reporting to Polymarket's Head of Growth after a record summer (combined Kalshi and Polymarket monthly **share volume** roughly doubled from April to July 2026, helped by the World Cup). Share volume counts contracts matched at $1 each, not dollars paid: Polymarket's `markets.volume` follows shares matched (decision 18). How the combined two-platform figure was compiled has not been checked.

- Did traders acquired through World Cup markets stay?
- Did they cross into other categories (sports → politics, crypto, etc.)?
- Which kinds of market listing bring in **new** traders, rather than recycling existing ones?

**Fallback headline:** if the wallet-identity test (§9, test C) shows the 2026 cohorts are badly distorted, lead with the **2024 US election cohort** instead. It predates both 2026 wallet changes and has about two years of follow-up. The World Cup then becomes a secondary cohort shown with a caveat.

**How this maps to iGaming and betting analytics** (use this in the README and interviews):

| Polymarket concept | Operator equivalent |
|---|---|
| Entry category of first trade | Acquisition channel / first product |
| Moving sports → politics | Cross-sell sportsbook → casino |
| Top-1% wallet concentration | VIP management |
| New market listing | Product launch |
| Cash traded per trader | Turnover per customer |
| Cash staked per trader | Handle per customer |

---

## 3. Architecture

```
SOURCE SYSTEMS                    WAREHOUSE (Databricks Free Edition)           PRESENTATION
─────────────────                 ─────────────────────────────────────         ──────────────────────────────
Hugging Face dump  ──┐            bronze: raw monthly Parquet in a UC Volume    Tableau Desktop FREE Edition
(1.27B trades,       │  monthly   silver: cleaned, deduped, typed Delta tables  → live connection to the
 2.03B user fills)   ├─ column ─► gold:   aggregated marts (KPIs, cohorts,        SQL warehouse (dev + demo video)
Polymarket Gamma API │  subsets           listings), well under 15M rows
(event tags,        ─┤                                                         Tableau Desktop PUBLIC Edition
 categories)         │            Data quality tests + reconciliation at         → reads exported gold files
Polymarket Data API ─┘            every layer                                   → publishes to Tableau Public
(spot checks)                                                                     (the shareable link)
```

**Framing for the CV:** Hugging Face and the Polymarket APIs are the *source systems*. **Databricks is the warehouse we build.** Tableau pulls from the warehouse.

**Why Spark is justified here:** in the Kalshi project we deliberately avoided Spark because the data fits on a laptop. Here it doesn't. "I used Spark where the data needed it and not where it didn't" is an interview answer.

---

## 4. Data sources

### 4.1 Bulk history: `SII-WANGZJ/Polymarket_data` (Hugging Face)

- MIT licence. Coverage: CLOB launch (21 Nov 2022) to **4 Oct 2026 23:59:59 UTC**. Refreshed "as needed", so treat it as a **static snapshot**.
- Built from Polygon `OrderFilled` events on four exchange contracts (V1 ×2, V2 ×2 since 28 Apr 2026). Pre-CLOB AMM-era trades (2020–Nov 2022) are not included.
- **Always read the `main` branch.** The `refs/convert/parquet` copies are an older release with different sizes.
- **Each dataset is ONE large Parquet file** (not sharded):

| File (main) | Size | Rows | Use |
|---|---|---|---|
| `markets.parquet` | 0.55 GB | 4,049,900 | Market metadata, one row per market. **Start here.** |
| `trades.parquet` | 45.85 GB | 1,266,885,846 | One row per matched fill. **Source of truth for volume.** |
| `users.parquet` | 60.76 GB | 2,026,673,731 | Every fill keyed by the wallet whose order filled. **Source for people.** |
| `orderfilled.parquet` | 127.20 GB | 2,026,704,633 | Raw events (fees as 32-byte binary). Avoid unless needed. |
| `quant.parquet` | 44.66 GB | 1,266,885,845 | YES-normalised view. Not needed. |

**Exact sizes and checksums** (1 GB = 10^9 bytes). Source: Hugging Face API, `GET https://huggingface.co/api/datasets/SII-WANGZJ/Polymarket_data/tree/main`, read 12 Oct 2026 at commit `73bc1adc43b05709d2cde3bf41ef4111eda3f4fc` (last modified 5 Oct 2026). These replace the earlier size figures, which were all too low.

| File (main) | Bytes | SHA-256 |
|---|---|---|
| `markets.parquet` | 545,510,579 | `ce153f7ffa523911dc46d7a8cbcf324f4fd7a39e1bca0b6d396ee572595ad35f` |
| `trades.parquet` | 45,846,785,710 | `c7ab409744dcafacbf1fe1d018f72fdcfede26e2c1626039fd7f0d5b8a632b09` |
| `users.parquet` | 60,757,064,436 | `773c9fb7735bb7a8efa670c7fb90e5f25e5ce0f950ef0de49207e269bd7bf4d3` |
| `orderfilled.parquet` | 127,200,963,857 | `ee81933332977deb43aaa5b5f1257697f50dadb1f8943b629d4bdb19d2068677` |
| `quant.parquet` | 44,657,939,800 | `1700f0153d642e89d7868c5f6b288e0c513634faea45516826069b361026722a` |

**Row counts:** `markets` (4,049,900) is verified against a local, checksum-verified copy (test 0). `trades` (1,266,885,846) and `users` (2,026,673,731) are verified from the Parquet footers (test A). `orderfilled` and `quant` are the dataset card's figures and are unverified.

**Where the bytes are** (test A, footer column sizes): `transaction_hash` is 26.02 GB of `trades` (56.8%) and 27.63 GB of `users` (45.5%). Dropping the columns that can be re-derived from `markets` (`condition_id`, `event_id`, `asset_id`) would save only 1.1 GB on `trades` and 4.7 GB on `users`.

**Key columns**
- `trades`: `timestamp` (unix s), `block_number`, `transaction_hash`, `log_index`, `contract`, `market_id`, `condition_id`, `event_id`, `maker`, `taker`, `price`, `usd_amount`, `token_amount`, `maker_direction`, `taker_direction`, `nonusdc_side`, `asset_id`
- `users`: `timestamp`, `block_number`, `transaction_hash`, `log_index`, `address`, `role` (maker / taker), `direction`, `usd_amount`, `token_amount`, `price`, `market_id`, `condition_id`, `event_id`, `nonusdc_side`
- `markets`: `id` (joins to `market_id`), `question`, `slug`, `condition_id`, `token1`, `token2`, `answer1`, `answer2`, `closed`, `active`, `archived`, `outcome_prices`, `volume`, `event_id`, `event_slug`, `event_title`, `created_at`, `end_date`, `updated_at`, `neg_risk`
- **There is no category or tag field anywhere.** The taxonomy must be built (§4.2).

**Counting semantics (must be explicit in the pipeline):**
- `trades` omits the taker-order summary events, so each fill is counted once. Use it for **volume**.
- `users` includes both sides of every match. Taker rows are the taker's transaction with the exchange contract (`role = taker`). Use it for **people and activity**. Its total USD is *not* comparable one-to-one with `trades`; reconcile and document the ratio.

### 4.2 Taxonomy: Polymarket Gamma API

- `GET https://gamma-api.polymarket.com/events` returns `tags[]`, `categories[]`, `category`, `subcategory`, `volume`, and `active` / `closed` / `archived` flags. Filters: `closed`, `archived`, `tag_id`, `tag_slug`. Pagination: `limit` + `offset` (no cursor).
- Rate limits: `/events` 500 req / 10 s, `/markets` 300 req / 10 s, `/tags` 200 req / 10 s (Cloudflare, sliding windows; excess is queued, not rejected).
- Plan: tags for high-volume events + rules on `event_slug` for recurring series (e.g. short crypto up/down markets). Report the "uncategorised" share openly.

### 4.3 Spot checks: Polymarket Data API

- `/trades`: max `limit` 10,000; offsets above 10,000 are rejected (HTTP 400). Fine for spot-checking a handful of markets against the dump. **Not usable for bulk history**, which is why the Hugging Face dump is the bulk source.

---

## 5. Tool constraints (verified October 2026)

### Databricks Free Edition
- Serverless only. One SQL warehouse, 2X-Small. Max 5 concurrent job tasks. One active pipeline per type.
- **Compute quota is undisclosed.** Exceeding it shuts compute for the rest of the day (data is kept).
- **Outbound internet is limited to an unpublished list of trusted domains.** PyPI works. **Hugging Face is reachable** (test E step 1, 12 Oct 2026, `scripts/dbx_test_e1_hf_reach.py` run as a one-time serverless notebook job, 24 s): all four routes read the `markets.parquet` footer on `main` and returned 4,049,900 rows: a plain HTTPS call to the `huggingface.co` API; HTTPS range requests on the file URL, redirected to the CDN `us.aws.cdn.hf.co`; `hf://` through `huggingface_hub`; and `hf://` through DuckDB 1.5.6 (both packages pip-installed in the session, and DuckDB's extension download also worked). **Test E steps 2 and 3 (12–13 Oct 2026):** a 97 KB laptop upload of Dec 2022 `trades` to the Volume read back in Spark with 1,666 rows (`scripts/dbx_test_e2_readback.py`). Then Dec 2024 `trades` was extracted **directly from Hugging Face inside a serverless notebook** by block range (`scripts/dbx_test_e3_direct_extract.py`, DuckDB 1.5.6): 0.542 GB received in 38 requests, extract 19.9 s, copy into the Volume 8.2 s, Spark read-back 10.9 s, 68 s of job execution in total, and 11,399,126 rows by the remote count, by Spark and in the laptop's manifest. The same month took 255 s to extract on the laptop. Spark reads the source's unsigned 64-bit `timestamp` and `block_number` as `decimal(20,0)`; cast to `bigint` in silver. On serverless, `dbutils.fs.cp` cannot read the session's local disk; a plain file copy to the `/Volumes` path works. `system.billing.usage` is readable but lags, so the DBU cost of a run is only visible later; the quota limit itself is still undisclosed. **Scale-up, July 2026 (13 Oct 2026, `scripts/dbx_bronze_month.py`):** `trades` 106,257,333 rows, 3.91 GB received, 2 parts (1.96 and 1.92 GB), 333 s of job time (extract 167 s, split 100 s, copy 21 s, verify 12 s); `users` 168,286,213 rows, 2 parts (2.34 and 2.22 GB), 345 s. Both row counts match the remote count, Spark, and the source footer's July figure. The session has 4 CPUs, 16.4 GB RAM and 64 GB of free local disk. Files in a Volume cannot be appended to (`OSError 29, Illegal seek`), so the manifest is rewritten whole each time. Billing so far: a 24 s run without Spark cost 0.005 DBU; two short runs that used Spark cost 0.14 and 0.33 DBU. `system.billing.usage` was more than 12 hours behind, so the July runs are not costed yet.
- **This project's workspace** (not secrets; no token is stored in the repo): host `dbc-0f3802cc-f8e5.cloud.databricks.com`; SQL warehouse "Serverless Starter Warehouse", id `748ff4e9a4a1ce9b`, Server Hostname `dbc-0f3802cc-f8e5.cloud.databricks.com`, port 443, HTTP Path `/sql/1.0/warehouses/748ff4e9a4a1ce9b` (2X-Small, auto-stop 10 min). Catalog `workspace`, schema `workspace.polymarket`, managed Volume `workspace.polymarket.bronze` (path `/Volumes/workspace/polymarket/bronze`). Laptop access is through the Databricks CLI's `DEFAULT` profile (browser login).
- Not supported: R, Scala, custom workspace storage locations (so no external ADLS), legacy features. Non-commercial use only.
- Inactive accounts may be deleted. Log in regularly and keep code in Git.

### Unity Catalog Volumes
- UI upload: **5 GB per file.** The CLI has reportedly hit the same 5 GB cap. Design every bronze file to be under about 3 GB.

### Tableau
| Edition | Connects to Databricks | Publishes to Tableau Public | Row cap |
|---|---|---|---|
| **Public Edition** (free; also what Tableau for Students now provides) | No: files only (text, JSON, Excel, OData, PDF, spatial, statistical, Google Drive) | **Yes** | 15M rows per source; 10 GB profile storage |
| **Free Edition** | **Yes**, any source | No | Unlimited |
| Professional | Yes | Yes | Paid |

- Tableau ↔ Databricks auth: OAuth (U2M, on by default) or personal access token. Needs the Server Hostname and HTTP Path of the SQL warehouse. Untested against Free Edition (test G).
- **Consequence:** develop and demo on Free Edition (live to Databricks); publish from Public Edition using exported gold files. State this design in the README. Dashboards on pre-aggregated gold tables are standard practice, not a workaround.

**Azure Data Lake: not used.** Free Edition can't use custom storage locations, and a second platform adds no signal for analyst roles.

---

## 6. Storage and ingestion design

The source is files, not a database, so "pull only what we need" works through Parquet itself:
- **Column projection always works.** Read only the columns a step needs.
- **Row-group skipping works at month level (test A, 12 Oct 2026).** In both `trades` (1,261 row groups) and `users` (2,005), every row group lies inside one UTC calendar month, so a one-month filter reads only that month's row groups. Within a month the groups are not time-sorted (about 93% overlap an earlier group), so day-level filters skip little. The footers are 2.8 MB and 3.4 MB.
- **2026 dominates.** 86% of `trades` rows fall in Jan–Oct 2026; March 2026 alone is 16.2% (205.0M rows, 6.90 GB compressed in the source with all columns). Months over the file limit are split into several part files inside the month folder, not trimmed by dropping columns.

**Ingestion path (decision 20): direct extraction inside Databricks.** A serverless notebook job installs DuckDB, reads `hf://datasets/SII-WANGZJ/Polymarket_data@main/<file>` filtered to the block range of one true UTC month, writes `part-NN.parquet` files to the session's local disk, copies them into `/Volumes/workspace/polymarket/bronze/<table>/year=YYYY/month=MM/`, and checks the row count with Spark against a separate remote count. The laptop submits the job through the Databricks CLI and supplies the month's block range from `lookup\month_bounds.csv`. No data passes through the laptop.

**Load order (13 Oct 2026):** the full bronze layer for `trades` and `users`, oldest month first, driven from the laptop by `scripts/13_load_bronze.py` (one sequential serverless job per month and table). Batch 1 is Nov 2022–Dec 2025; then 2026 one month at a time, with March 2026 (the largest) last. Each run checks the files in the Volume against a separate remote row count; a Spark count per month against the manifest runs once per batch (`scripts/dbx_bronze_verify.py`). The load stops if compute is cut off or a count check fails. Every job run id is logged so its DBU cost can be attached from `system.billing.usage`.

**Fallback: the laptop route below.** It is tested end to end (extract with `scripts/04_bronze_month.py`, upload with `databricks fs cp`, read back in Spark) and is kept for the case where Hugging Face stops being reachable from Databricks or the compute quota runs out.

**Laptop ingestion loop (fallback; one month at a time):**
1. On the laptop, DuckDB reads `hf://datasets/SII-WANGZJ/Polymarket_data@main/<file>` remotely, filtering to the block range of one true UTC month (decision 17; the dataset's own `timestamp` is not used to cut months). Bronze keeps all source columns (decision 16).
2. Write `D:\polymarket-economy-data\bronze\<table>\year=YYYY\month=MM\part-NN.parquet` (`part-00`, `part-01`, …; planning target 2.5 GB per file, limit 3 GB). From the source's compressed sizes this is 58 files for `trades` and 63 for `users` over 48 months; only 2026 months need more than one.
3. Upload to a Unity Catalog Volume.
4. Log the month as done in a manifest, so runs resume after interruption. Optionally delete the local copy.

Local footprint on the fallback route: a few GB at a time; the full history (106.6 GB) does not fit on D: (about 96 GB free) at once, so months would be uploaded and removed as they go.

---

## 7. Metric definitions (draft; finalise after EDA)

| Metric | Draft definition |
|---|---|
| Trader | A wallet address appearing in `users` (after bot / contract exclusions in §9) |
| Active trader (period) | Trader with at least 1 fill in the period |
| New trader | Active trader whose first-ever fill is in the period |
| Returning | Active this period and in the previous period |
| Resurrected | Active this period, not in the previous period, but active at some earlier point |
| Acquisition cohort | Month (or event window) of first fill |
| Entry category | Category of the market of the trader's first fill |
| Retention (week n / month n) | Share of a cohort active in period n after acquisition |
| Notional volume (headline and reconciliation figure) | Shares matched: sum of `token_amount` from `trades`, each fill once, at $1 per share. This is what `markets.volume` and Polymarket's published volume count. Not dollars paid. |
| Cash traded | A wallet's USD across all its fills, buys and sells: sum of `usd_amount` from `users` for that wallet. Basis for volume per trader and whale share. Summed over all wallets it counts both legs of every match, so it is not comparable with notional volume. |
| Cash staked | A wallet's USD on BUY fills only: sum of `usd_amount` from `users` where `direction = 'BUY'`. The handle analogue. |
| Volume per trader | Cash traded ÷ active traders |
| Whale share | Share of cash traded from the top 1% of wallets in the period |
| Wallet segment | A **segmentation, not an exclusion**: every wallet is labelled **market maker**, **high-frequency taker** or **retail**, so dashboards can filter or split by segment and nothing is silently dropped. Planned inputs, all measured per wallet per month: fills, maker share of fills, share of hours active (chain time), markets traded, cash per fill. Intended shape: market maker = very active, mostly maker, active most hours; high-frequency taker = very active and active most hours but mostly taker; retail = everyone else. **Thresholds not set yet** (decision 11). The rule will be documented and versioned. |

**Day definition:** UTC calendar day (on-chain timestamps are UTC). State this on every dashboard.

---

## 8. Dashboards (three, each tied to one decision)

1. **Weekly KPIs:** active traders (new / returning / resurrected), volume, volume per trader, whale share, with week-on-week variance.
2. **Cohorts and cross-sell:** retention heatmap by acquisition month × entry category, plus a category transition matrix (first category → later categories). Parameter to switch cohort definition.
3. **Market listing scorecard:** which events and markets bring in new traders vs recycle existing ones; how many listings attract almost no one.

**Craft:** LOD expressions for cohorts, parameter actions, consistent design, mobile layout. **Aggregates only:** no wallet-level tables on Tableau Public; suppress cells with fewer than 10 wallets.

---

## 9. Known data issues and concerns (ranked)

| # | Concern | Severity | How we handle it |
|---|---|---|---|
| 1 | **Wallet identity break.** Deposit Wallets launched 4 May 2026 for new users and are being rolled out gradually to existing users. Migrated veterans would look like new traders in the World Cup window. Linking wallets to owners is hard; commercial vendors use heuristic confidence scores. | High for the headline; caveat for the project | Measure in aggregate (spike in first-seen wallets + matching cliff of dormant veterans). Flag likely migrants (first seen after 4 May with veteran size and frequency from day one). Report with and without them. Choose the headline cohort from the result. |
| 2 | **History gaps.** Past releases had missing blocks (one report: ~21% of 2024 blocks; another: 1.37M fills missing across 11 days, Dec 2025–Jan 2026). The Oct 2026 release claims re-verification; the maintainer gave no details. | High until tested | Block-gap scan + daily fill counts; compare with independent figures; exclude or flag failing periods. |
| 3 | **Bots and market makers** dominate fill counts and distort user metrics. Test D and the 24/7 check (12 Oct 2026, `scripts/10_test_d_concentration.py`, `scripts/11_time_checks.py`): in Dec 2024 the top 100 wallets by fills made 32.0% of fills but 8.2% of cash traded, were makers on 90% of fills and were active in a median 91% of hours; 61 wallets active in 90%+ of hours made 19% of fills. In 1–4 Oct 2026 automation is much wider: 1,382 wallets active in 90%+ of hours made 57% of fills and 34% of cash traded, with a 72% maker share, so many are takers. Top 1% of wallets: 57% of cash traded (Dec 2024), 70% (Oct 2026). | Medium–high | Three-way wallet segmentation (§7), not an exclusion; thresholds pending (decision 11). |
| 4 | **Ingestion path:** HF reachability from Databricks, 5 GB upload cap, undisclosed compute quota, unknown row-group layout. | Medium (true blocker) | Tests A, E, F. Worst case: narrower history window. |
| 5 | **No category field.** | Medium | Gamma tags + slug rules; report the uncategorised share. |
| 6 | **Trades only:** no deposits, splits, merges, redemptions, or P&L; fees only in raw binary. | Medium (scope) | Value = volume per trader, not revenue or LTV. Stated on the dashboard. |
| 7 | **`users` vs `trades` counting** differs. Reconciled for Dec 2024 (true UTC month, 12 Oct 2026, `scripts/08_reconcile_month.py`): `users` maker rows are exactly the `trades` rows (11,399,126; same key, wallet, USD, tokens, market); `users` adds one taker row per transaction (9,705,870), giving 1.85 rows and 2.50× the USD per `trades` row. 358,846 distinct wallets. No exchange contract address appears as a wallet (four addresses from the dataset card); repeated on 1–4 Oct 2026, where all fills are on the V2 contracts, with the same result and an exact reconciliation (9,448,016 maker rows, 5,331,293 taker rows, 83,592 wallets). 451 taker rows ($53.8K) have no fill in `trades`, unexplained. **`trades.usd_amount` is the maker's leg only:** $770.9M, against $1,155.0M on the taker's leg. They differ because 59% of transactions are complementary matches (maker buys the opposite outcome), where the two sides pay different amounts; that rule reproduces taker USD to within $14K for the month. | Medium–high (the volume definition depends on it) | People from `users`. Volume definition to be fixed by comparing maker-leg and taker-leg USD with `markets.volume`; until then state which leg any volume figure uses. |
| 8 | **V2 cutover, 28 Apr 2026** (~1 h downtime, all open orders wiped, collateral moved from USDC.e to pUSD). | Low–medium | Continuity check on daily metrics. |
| 9 | **Tableau Free Edition ↔ Databricks Free Edition** auth untested. | Low–medium (true blocker for live demo only) | Test G. Public version works from files regardless. |
| 10 | **Public exposure** of pseudonymous wallet data. | Low, but matters in interviews | Aggregates only; small-cell suppression. |
| 11 | **Dataset timestamps are wrong by hours in places.** Where the source pipeline lacked a block time it assumed exactly 2 s per block across whole 100,000-block chunks, while real blocks took about 2.15 s, so the clock falls steadily behind and then snaps back. Test B on `trades`, Dec 2024 (12 Oct 2026), checked against real block times from a public Polygon RPC node: block 65,899,999 is stamped 13.5 h early; the month's last block (66,185,466, stamped 31 Dec 23:59:58) was really mined on 1 Jan 2025 16:17:42 UTC, 16.3 h later; 4 of 13 chunks run at exactly 2.000 s per block and a fifth is partly affected; the dataset shows a false 13.5 h hole in fills on 24–25 Dec. Correct stretches are a constant 104 s ahead of chain time. **Extent:** full check on two months: Dec 2024 has 53.7% of fills off by over an hour and 15.6% on the wrong UTC day; 1–4 Oct 2026 is within 7 s throughout. A free sample from footer statistics (two points per `trades` row group against the lookup) shows errors over an hour in Oct 2024–Jan 2025 and May–Oct 2025, a steady offset of about 100 s from Feb 2024, and 0–1 s from Dec 2025 onwards. So the 2024 election window is badly affected and the 2026 World Cup window looks clean; the sample is indicative, not a full scan. The source files are partitioned by this faulty clock. | **High** (days, weeks, cohort dates and month partitions all depend on it) | Do not use the dataset `timestamp` for any metric. Rebuild time from the chain: a block → UTC lookup from real Polygon block times, anchored every 1,000 blocks and interpolated. Bronze months are cut by the block range of each true UTC month. `block_number` is trusted (Dec 2024: no duplicates, longest run of blocks without fills is 30). **Lookup built 12 Oct 2026:** 59,074 anchors (blocks 35,896,000–94,969,000) from `polygon-bor-rpc.publicnode.com`, time strictly increasing across anchors. Validated on 500 random blocks against their exact chain times: absolute interpolation error median 1.2 s, p95 32 s, p99 59 s, max 248 s (496 of 500 within 60 s). Month boundaries are exact (found by bisection on real block times), so only within-month day assignment carries the interpolation error. **Caveat to state on dashboards:** a fill within about ±4 minutes of midnight UTC can be assigned to the neighbouring day (worst case seen: 248 s; typical error about 1 s). Month totals are unaffected. |
| 12 | **Snapshot ends 4 Oct 2026:** about 11 weeks after the World Cup final (19 Jul). | Low | 4- and 8-week retention only for the World Cup cohort. |
| 13 | **Recurring crypto series swamp the listing counts.** Test 0: "up or down" slugs are 19.71% of markets and 10.82% of `markets.volume`; hourly "bitcoin above" / "ethereum above" series add thousands more near-zero-volume markets. 93.2% of all markets were created in 2026 and 44.19% have zero volume. | Medium–high for the listing scorecard | Separate recurring crypto series from one-off events in the listing scorecard (slug rules, §4.2). Never report an unweighted per-market average across both. |
| 14 | **`end_date` and `created_at` are not reliable.** Test 0: 32,720 markets have `end_date` before `created_at`; `end_date` ranges from 2011 to 2031; `created_at` runs to 5 Oct 2026, past the trades cut-off. | Medium | Close times (and first-activity times) come from `trades`, not from `markets`. |
| 15 | **Left-censoring at the CLOB launch.** The data begins with the CLOB on 21 Nov 2022; AMM-era trading (2020–Nov 2022) is not in it. A wallet that was active before then appears to make its "first-ever" fill in late 2022, so new-trader counts and acquisition cohorts for the first months overstate genuine newcomers. | Medium for early cohorts; low for 2024–26 | State it wherever "new trader" or first-seen dates are shown. Treat cohorts from Nov 2022 to early 2023 as "first seen on the CLOB", not as acquisitions, and do not use them as headline cohorts. |

**Only concerns 4 and 9 can block the project.** All others are caveats to measure and document.

---

## 10. EDA / spike plan (do in this order)

Pilot with the smallest data first. Read only the columns each test needs. Save outputs to D:.

| Test | What | Decides |
|---|---|---|
| 0 | Load `markets.parquet` (0.55 GB): row counts, date ranges, null rates, `volume` distribution, event and slug patterns | Basic orientation |
| A | Read Parquet footers remotely (row groups; min/max `timestamp` and `block_number` per group) for `trades` and `users` | Whether monthly filtering skips data or needs full column scans |
| B | Block-gap scan on `block_number`; daily fill counts and volume; uniqueness of (`transaction_hash`, `log_index`) | Completeness, duplicates, which periods to trust |
| C | First-seen wallets per day, Mar–Aug 2026; dormancy cliff of established wallets after 4 May; first-trade size of new wallets | **Headline cohort** (World Cup vs 2024 election) |
| D | Share of fills and volume from top 100 / 1,000 addresses | Bot / market-maker rule |
| E | From a Databricks notebook: read `hf://` directly? If not, run one month through laptop → Volume upload | **Ingestion path** (blocker) |
| F | Time one month through Spark (bronze → silver → gold) and note quota usage | **History window** in scope |
| G | Connect Tableau Free Edition to the SQL warehouse | **Live demo** (blocker for demo only) |
| H | Gamma tags: share of 2025–26 volume classifiable | Taxonomy approach |
| I | Continuity of daily metrics across 28 Apr 2026 | V2 caveat |

**🔍 Robustness checkpoint after tests C, E and F:** decide headline cohort, ingestion path and history window. Record in the decision log before building silver and gold.

**To-do**
- Once January 2025 is in bronze, re-run `scripts/09_volume_definition.py` logic on the 763 of 2,302 December 2024 markets (about 35%) that matched no measure within 1%. They all sit below `markets.volume`, which later trading would explain; confirm that adding their January fills brings shares matched up to `markets.volume`.
- Run test C as a Spark job over the complete bronze layer in Databricks, not on the laptop and not as a separate scan of the source: it needs each wallet's first-ever fill across the whole `users` history.
- Attach the DBU cost of every ingestion run from `system.billing.usage` once it catches up, and summarise it in the README.
- Set the wallet-segment thresholds (decision 11) from the joint distribution of fills, maker share and share of hours active across several months, including a World Cup month.

---

## 11. Decision log

| # | Date | Decision | Evidence |
|---|---|---|---|
| 1 | 2026-10-12 | Project = Polymarket player economy (cohorts, cross-sell, listings) | User-level data at 1B+ scale; maps to iGaming/betting analytics |
| 2 | 2026-10-12 | Bulk source = Hugging Face dump, not the Polymarket API | Data API caps at 10,000 per call and rejects offsets above 10,000; bulk crawl impractical |
| 3 | 2026-10-12 | Warehouse = Databricks Free Edition; no Azure | Free Edition lacks custom storage locations; second platform adds no signal |
| 4 | 2026-10-12 | Tableau: Free Edition for live dev/demo; Public Edition for publishing | Public Edition can't connect to Databricks; Free Edition can't publish |
| 5 | 2026-10-12 | Ingest one month at a time, column subsets, files under 3 GB | 5 GB upload cap; undisclosed compute quota |
| 6 | 2026-10-12 | Volume from `trades`; people from `users` (volume part refined by decision 18) | Different counting semantics |
| 7 | 2026-10-12 | Value metric = volume per trader, not revenue | No deposits, P&L or decoded fees in trades data |
| 8 | 2026-10-12 | Publish aggregates only; suppress cells under 10 wallets | Pseudonymous data on a public platform |
| 12 | 2026-10-12 | Listing scorecard separates recurring crypto series from one-off events | Test 0: "up or down" slugs are 19.71% of markets, 10.82% of `markets.volume`; 44.19% of markets have zero volume |
| 13 | 2026-10-12 | Market close times come from `trades`, not `markets.end_date` / `created_at` | Test 0: 32,720 markets with `end_date` before `created_at`; `end_date` spans 2011–2031 |
| 14 | 2026-10-12 | The calendar month is the ingestion unit; monthly filters rely on row-group skipping | Test A: no row group in `trades` or `users` spans a month boundary; groups are unsorted within a month |
| 15 | 2026-10-12 | A month over the 3 GB file limit is split into multiple part files in its month folder; needed columns are not dropped to fit | Test A column sizes: dropping re-derivable columns saves 1.1 GB of 45.84 (`trades`) and 4.7 GB of 60.75 (`users`), and does not change the file count for any month |
| 16 | 2026-10-12 | Bronze keeps all source columns (17 in `trades`, 14 in `users`); files are named `part-NN.parquet` for every month | Test A column sizes: all columns = 45.84 GB and 60.75 GB; dropping re-derivable columns leaves the per-month file counts unchanged (58 and 63) |
| 17 | 2026-10-12 | Time comes from the chain, not the dataset: build a block → UTC lookup (real Polygon block times, anchors every 1,000 blocks, linear interpolation); cut bronze months by the block range of each true UTC month; keep the dataset `timestamp` in bronze untouched | Test B, `trades` Dec 2024: dataset clock up to 16.3 h behind real block time, verified on nine blocks against a public Polygon RPC node (concern 11). Lookup validated on 500 random blocks: median error 1.2 s, max 248 s. Re-cut Dec 2024 `trades` has 11,399,126 rows against 11,568,669 in the dataset's own December; the difference is fills really made on 1 Jan 2025 (and 49 blocks from 30 Nov) |
| 18 | 2026-10-12 | Three volume measures (§7). **Notional volume** = shares matched from `trades.token_amount`: headline and reconciliation figure. **Cash traded** = a wallet's USD on buys and sells from `users`: basis for volume per trader and whale share. **Cash staked** = a wallet's USD on BUY fills from `users`: handle analogue. `trades.usd_amount` alone is not used as "volume" | Dec 2024, `scripts/09_volume_definition.py`: of 2,302 markets created in the month, 1,493 (64.9%) match shares matched to within 0.1% of `markets.volume`, 1 is above it, and none matches maker-leg or taker-leg USD. `scripts/08_reconcile_month.py`: maker leg $770.9M vs taker leg $1,155.0M |
| 19 | 2026-10-12 | Bots are handled by a three-way wallet segmentation (market maker / high-frequency taker / retail) that dashboards can filter on, not by excluding wallets. Thresholds are left open (decision 11) | Tests D and 24/7 on Dec 2024 and 1–4 Oct 2026 (concern 3): the most active wallets split into mostly-maker and mostly-taker groups, and the automated share grew from 61 to 1,382 wallets active in 90%+ of hours |
| 20 | 2026-10-13 | Ingestion path = direct extraction from Hugging Face inside Databricks serverless (DuckDB in a notebook job, by true-UTC block range, into the Volume). The laptop route (extract on D:, upload with the CLI) is the documented fallback | Test E: all four routes to Hugging Face work from serverless; Dec 2024 `trades` extracted in 68 s of job time against 255 s on the laptop before upload. Content check of the Databricks file against the laptop file: identical row count (11,399,126), distinct keys, distinct transactions, block range, and exact integer checksums of `block_number`, `log_index`, and `usd_amount` / `token_amount` in millionths; the floating-point sums agree to 12 significant figures. Not yet tried above 0.54 GB; quota undisclosed |
| 9 | pending | Headline cohort: World Cup 2026 or US election 2024 | Test C |
| 10 | pending | History window in scope | Tests A, F |
| 11 | pending | Bot / market-maker rule | Test D |

---

## 12. What makes it stand out

- **Start and end with a memo.** The brief is the stakeholder's question; the output is 3–5 findings, each with a recommended action and a rough size of the prize. Dashboards are the evidence.
- **Published metric definitions** (§7). Analytics interviews probe exactly this.
- **Reconciliation page:** gold volume ties back to `trades` and to `markets.volume`; row counts tested between layers; `users` vs `trades` ratio explained.
- **Decision log** of the messy parts (wallet change, V2 cutover, bots, unclassifiable markets).
- **SQL in the repo:** cohort assignment, retention and first-touch attribution with window functions in Databricks SQL.
- **Honest scope:** limitations stated on the dashboards, not buried in the README.

## 13. Out of scope (v1)

P&L, deposits/withdrawals, fee revenue, splits/merges/redemptions, wallet-to-person resolution beyond the flagging rule, real-time refresh, predictive models.

---

## 14. Working principles

1. **Small steps.** One step at a time; check the result before moving on.
2. **Pilot with the smallest feasible data** before scaling.
3. **Flag robustness checkpoints.** Small checks are handled inline; big-picture ones are raised for a separate review.
4. **Everything resumable.** Each step checks what's already on disk and skips it.
5. **No placeholders or guesses.** Use real paths and verified facts; say when something is unverified.
6. **Data never goes in the repo.** Code and data are separate sibling folders on D:.

## 15. Environment

- Windows laptop, Python 3.12, VS Code, project virtual environment `.venv`.
- Everything lives on the USB drive D: (about 107 GB free as of Oct 2026):
  - Code: `D:\polymarket-economy` (Git repo, pushed to GitHub; includes `.venv`, which is git-ignored).
  - Data: `D:\polymarket-economy-data` (never committed).
- Because the code is on a removable drive, **push to GitHub at the end of every session**. If Windows ever assigns the drive a letter other than D:, reassign it to D: in Disk Management, or recreate `.venv` (virtual environments store absolute paths).
- Accounts needed: Databricks Free Edition, Tableau Public, Tableau Desktop Free Edition and Public Edition, Hugging Face (optional; public files don't need a login).

## 16. Sources

- Dataset card: https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data
- Dataset issues: HF discussions #4, #6, #7, #8; GitHub https://github.com/SII-WANGZJ/Polymarket_data issues #1, #3
- Databricks Free Edition limits: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- Volume uploads: https://docs.databricks.com/aws/en/ingestion/file-upload/upload-to-volume
- Tableau editions: https://help.tableau.com/current/pro/desktop/en-us/desktop_comparison.htm
- Tableau ↔ Databricks: https://docs.databricks.com/aws/en/partners/bi/tableau
- Polymarket wallets: https://docs.polymarket.com/developers/proxy-wallet
- Gamma events: https://docs.polymarket.com/api-reference/events/list-events
- Rate limits: https://docs.polymarket.com/api-reference/rate-limits
- Data API trades: https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets
