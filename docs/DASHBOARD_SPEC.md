# Dashboard specification — Tableau Public Edition

Build guide for the three dashboards and the methodology page, from the CSV files in `D:\polymarket-economy-data\gold_export` (written by `scripts/18_export_gold.py`; table designs in `docs/GOLD_PLAN.md`). Figures quoted here are from the 13 Oct 2026 build and are there to check the dashboards against, not to be typed in: every number on a dashboard comes from the data.

## 0. Rules that apply to every dashboard

**Field names.** Tableau renames CSV columns on import: `active_wallets` becomes `Active Wallets`, `week_start` becomes `Week Start`. This document uses the Tableau names. Each CSV is its own data source, named after the file (for example `weekly_kpi`).

**Data types to set on import.** `true` / `false` columns as Boolean. `Week Start`, `Week End`, `Month Start`, `Date From`, `Date To`, `First Fill Date`, `Last Fill Date` as Date. `Week N`, `Month N` as whole-number **dimensions** (discrete). `Cohort` in the monthly cohorts is text like `2026-03`; leave it as a string. `Listing Key` as string.

**Filters are single-value, with "All" as the default (decisions 39 and 40).** Four tables are cubes: `weekly_kpi`, `cohort_retention`, `cohort_retention_monthly` and `cohort_category_transition`. Every dimension in them carries a ready-made row with the value `ALL`, computed before small cells were suppressed. So:

1. Every filter on a cube dimension is **Single Value (dropdown)**, and its default selection is the data value `ALL`.
2. In the filter's *Customize* menu, **untick "Show 'All' Value"**. Tableau's own "(All)" would select `ALL` and its parts together and double-count.
3. A sheet must pin every cube dimension to exactly one value, except a dimension it deliberately splits by (on Colour, Rows or Columns). A split dimension is filtered to **exclude** `ALL` on that sheet.
4. Never total across a split by category: a wallet that trades two categories is counted in both. Use the `ALL` row for the total.
5. Rename `ALL` to "All" with an alias on each dimension for display.

Add this guard to every sheet built on a cube table (put it on Detail, and show it as a red text sheet-title suffix when it is not blank):

```
// Double-count guard (weekly_kpi version; swap in the table's own dimensions)
IF COUNTD([Activity Status]) > 1 AND MIN([Activity Status]) = "ALL" THEN "CHECK FILTERS: ALL mixed with parts"
ELSEIF COUNTD([Segment]) > 1 AND MIN([Segment]) = "ALL" THEN "CHECK FILTERS: ALL mixed with parts"
ELSEIF COUNTD([Scripted]) > 1 AND MIN([Scripted]) = "ALL" THEN "CHECK FILTERS: ALL mixed with parts"
ELSEIF COUNTD([Category]) > 1 AND MIN([Category]) = "ALL" THEN "CHECK FILTERS: ALL mixed with parts"
END
```

The listing scorecard is not a cube (one row per listing, no `ALL` rows), so its filters may use Tableau's normal "(All)"; they are still single-value dropdowns for consistency.

**Suppressed values.** No figure is published for fewer than 10 wallets. Suppressed cells are empty in the CSV and arrive as Null. On every wallet count and anything derived from one: *Format → Pane → Special Values → Text:* `<10`, *Marks:* "Show at Indicator" off (hide). Do not replace nulls with zero.

**Wording.** Call `Notional Shares` "Notional volume (shares)": it is the platform headline, what Polymarket publishes as volume. Call `Cash Traded` "Cash traded by wallets (both sides)"; never put the two on one axis or call cash traded "volume" without the qualifier. Say "wallets", not "users" or "people".

**Annotations on every time-series chart** (dates in `annotation.csv`; add them as reference lines and bands on the date axis):

| What | Where | Label |
|---|---|---|
| Reference line | 28 Apr 2026 (on weekly charts: the week starting 27 Apr 2026) | "V2 exchange cutover: active established wallets step down. Annotated, not adjusted." |
| Reference band | 29–30 Apr 2026 (same week) | "Known gap: fills short of published volume" |
| Reference band | 21–30 Nov 2022 and 1–4 Oct 2026 | "Partial month: wallet segments understated" |
| Reference band, light | 21 Nov 2022 – 28 Feb 2023 | "Left-censored start" |
| Reference band, light | 28 Sep–5 Nov 2024; 1 Jan–31 Mar 2026; 11 Jun–19 Jul 2026 | "2024 election window", "Q1 2026 benchmark", "World Cup 2026" |

Weekly charts start at the week of 12 Dec 2022: the three earlier weeks have fewer than 10 active wallets and are not published. All 202 weeks in the data are complete weeks, so there is no partial week to flag; the partial-month bands matter because segments are assigned by month.

**Caption on every dashboard:** "Source: Polymarket on-chain fills, 21 Nov 2022 – 4 Oct 2026 (UTC). Wallets are on-chain identities, not verified people. Figures under 10 wallets are not shown."

---

## 1. Dashboard 1 — Weekly KPIs

**Question it answers:** how big is the platform week by week, who is trading, and how concentrated is it?

### Data sources and relationships

| Data source | Used for | Relationship |
|---|---|---|
| `weekly_kpi` | wallets, activity status, segments, cash, whale share | to `dim_week` on `Week Start` = `Week Start` (many to one) |
| `dim_week` | week flags for tooltips and annotation logic | — |
| `weekly_market_volume` | headline notional and the subcategory view (each fill once) | separate source; to `dim_week` on `Week Start` if flags are needed |
| `annotation` | the notes sheet | separate source, no relationship |

Do not relate `weekly_kpi` to `weekly_market_volume`: they are two views of the same weeks at different grains.

### Filters (dashboard-level, applied to all sheets using `weekly_kpi` unless a sheet says otherwise)

| Filter | Field | Values | Default |
|---|---|---|---|
| Segment | `Segment` | All, retail, market_maker, high_frequency_taker | All |
| Scripted wallets | `Scripted` | All, not scripted, scripted | All |
| Category | `Category` | All, Sports, Politics, Crypto, Economy & Finance, Culture & Entertainment, Tech & Science, Uncategorised | All |
| Date range | `Week Start` | range slider | everything |

`Activity Status` is not a user filter: each sheet pins it.

### Calculated fields (`weekly_kpi`)

```
// Volume per trader ($)
SUM([Cash Traded]) / SUM([Active Wallets])

// Whale share, within segment (default)
SUM([Whale Cash Traded In Segment]) / SUM([Cash Traded])

// Whale share, global
SUM([Whale Cash Traded Global]) / SUM([Cash Traded])

// Whale basis (parameter [Whale Basis]: "Within segment" | "All wallets", default "Within segment")
IF [Whale Basis] = "Within segment"
THEN SUM([Whale Cash Traded In Segment]) / SUM([Cash Traded])
ELSE SUM([Whale Cash Traded Global]) / SUM([Cash Traded]) END

// Cash staked share of cash traded
SUM([Cash Staked]) / SUM([Cash Traded])

// Fills per active wallet
SUM([Fills]) / SUM([Active Wallets])

// New wallets share of active (use only on sheet 1.2, where Activity Status has ALL and new)
SUM(IF [Activity Status] = "new" THEN [Active Wallets] END)
/ SUM(IF [Activity Status] = "ALL" THEN [Active Wallets] END)

// Week-on-week change in active wallets (table calculation along Week Start)
(SUM([Active Wallets]) - LOOKUP(SUM([Active Wallets]), -1)) / LOOKUP(SUM([Active Wallets]), -1)
```

### Sheets

| # | Sheet | Source | Chart | Fields | Pinned on this sheet |
|---|---|---|---|---|---|
| 1.1 | Headline tiles | `weekly_kpi` | 5 big numbers, latest week in range, with change on the previous week | `Active Wallets`, `Notional Shares`, `Cash Traded`, Volume per trader, Whale basis | `Activity Status` = ALL |
| 1.2 | Active wallets: new, returning, resurrected | `weekly_kpi` | stacked area by week | Columns `Week Start` (week, continuous); Rows `SUM([Active Wallets])`; Colour `Activity Status` | `Activity Status` excludes ALL (order: returning, resurrected, new) |
| 1.3 | Notional volume by category | `weekly_market_volume` | stacked bars by week | Columns `Week Start`; Rows `SUM([Notional Shares])`; Colour `Category` | none (additive: each fill once). The Category dropdown does not apply; add a separate highlight action |
| 1.4 | Fills and cash traded | `weekly_kpi` | two line charts stacked, shared date axis | `SUM([Fills])`; `SUM([Cash Traded])`, `SUM([Cash Staked])` | `Activity Status` = ALL |
| 1.5 | Volume per trader | `weekly_kpi` | line | Volume per trader | `Activity Status` = ALL |
| 1.6 | Whale share | `weekly_kpi` | line, with the `Whale Basis` parameter control | Whale basis | `Activity Status` = ALL. Default view reads `Segment` = retail if the user has picked it; with Segment = All the within-segment figure is the cash share of each segment's own top 1% combined |
| 1.7 | Who trades: segments | `weekly_kpi` | three 100% stacked areas (wallets, fills, cash traded) by week | Colour `Segment` | `Segment` excludes ALL; `Activity Status` = ALL. The Segment dropdown does **not** apply to this sheet |
| 1.8 | Category of the week | `weekly_kpi` | horizontal bars for one selected week | Rows `Category`; `SUM([Active Wallets])`, `SUM([Notional Shares])` | `Category` excludes ALL; `Activity Status` = ALL. The Category dropdown does not apply. Title note: "Wallets are counted in each category they traded; bars do not add to the total." |
| 1.9 | Subcategory detail | `weekly_market_volume` | treemap or bars for the range | `Category`, `Subcategory`, `SUM([Notional Shares])`, `SUM([Fills])` | — |
| 1.10 | Notes | `annotation` | text table | `Date From`, `Date To`, `Label`, `Note` | — |

Annotations: all of §0 on sheets 1.2–1.7. On 1.7 add the caption "Segments are monthly; the first and last months are partial and understate automated wallets."

### Headline numbers this dashboard must show

1. **Notional volume, whole period: 111.9 billion shares over 1.27 billion fills** (tile, from `weekly_market_volume`).
2. **Active wallets: peak 395,215 in the week of 23 Mar 2026; 120,070 in the last week (28 Sep 2026).**
3. **The 28 April step: 321,340 active wallets in the week of 20 Apr 2026, 266,111 the week after** (annotated on 1.2).
4. **World Cup peak: 2.94 billion shares in the week of 15 Jun 2026, the largest week in the data; 60% of it in World Cup markets.**
5. **Retail is 95% of wallets but 22% of fills and 34% of cash traded** (last week: 94.6% / 21.6% / 34.1%). Within-segment whale share, all segments together, runs at about half of cash traded in 2026 (47–58% in the weeks checked).

---

## 2. Dashboard 2 — Cohorts and cross-sell

**Lead with this, as the dashboard title and first tile row:**

> **World Cup newcomers were retained at about half the benchmark rate, even after adjusting for the calendar.** Relative week-8 retention: **0.27** for the World Cup cohort, against **0.53** (2024 election) and **0.54** (Q1 2026). **Wallets that entered through a World Cup market were retained worst: 4.2% active in week 8, against 7.7%** for wallets that entered in the same weeks through any other market.

### Data sources and relationships

| Data source | Used for | Relationship |
|---|---|---|
| `cohort_retention` | weekly retention curves, entry channel, splits | none needed |
| `cohort_calendar_baseline` | calendar-adjusted (relative) retention | none; it already carries the cohort's own retention |
| `cohort_retention_monthly` | the long view (30-day periods 0–23) | none |
| `cohort_category_transition` | cross-sell and the transition matrix | none |

Keep the four as separate data sources. Link the cohort selection across them with a **parameter**, not a relationship.

### Parameters and filters

| Control | Type | Values | Default |
|---|---|---|---|
| `[Cohort Type]` | parameter | event, month | event |
| Cohort | filter on `Cohort` (multi-value is safe here **only** as a Colour split with `ALL` excluded) | event: World Cup 2026, 2024 election, Q1 2026; month: 2022-11 … 2026-10 | all three event cohorts shown as three lines |
| Entry category | single-value filter | All + 7 categories | All |
| Segment at entry | single-value filter | All, retail, market_maker, high_frequency_taker | All |
| Scripted wallets | single-value filter | All, not scripted, scripted | All |
| Entry channel | single-value filter | All, via_event_market, other | All |

Every sheet filters `[Cohort Type] = [Cohort Type parameter]` with the calculated field `[Cohort Type] = [Cohort Type Parameter]` set to True. Aliases: `world_cup_2026` → "World Cup 2026", `us_election_2024` → "2024 US election", `benchmark_q1_2026` → "Q1 2026", `via_event_market` → "Via event market", `other` → "Other market, same window".

`Entry Channel` has values other than `ALL` only for the World Cup and election cohorts. `cohort_category_transition` has no `Scripted` or `Entry Channel` dimension, so those two dropdowns do not apply to its sheets; say so in the sheet subtitle.

### Calculated fields

```
// cohort_retention and cohort_retention_monthly
// Retention
SUM([Active Wallets]) / SUM([Observable Wallets])

// Cash traded per acquired wallet (in week n)
SUM([Cash Traded]) / SUM([Observable Wallets])

// Share entering via the event market (week 0 rows)
SUM([Entered Via Event Market Wallets]) / SUM([Cohort Wallets])

// Share of the cohort observable in this period (grey out below 100%)
SUM([Observable Wallets]) / SUM([Cohort Wallets])

// Fully observable?
SUM([Observable Wallets]) = SUM([Cohort Wallets])

// cohort_calendar_baseline (one row per cohort, segment and week, so AVG returns the stored value)
// Cohort retention
AVG([Cohort Retention])

// Baseline retention
AVG([Baseline Retention])

// Relative retention
AVG([Cohort Retention]) / AVG([Baseline Retention])

// cohort_category_transition
// Transition share
SUM([Wallets]) / SUM([Base Wallets])

// Is cross-sell summary row
[Traded Category] = "Any other category"

// Stayed in entry category
[Traded Category] = [Entry Category]
```

With the filters pinned as §0 requires there is one row per mark, so `SUM` in the ratios returns that row's value; the `SUM(...) / SUM(...)` form is used so that a ratio is never averaged.

### Sheets

| # | Sheet | Source | Chart | Fields | Pinned on this sheet |
|---|---|---|---|---|---|
| 2.1 | Lead tiles | `cohort_calendar_baseline`, `cohort_retention` | three numbers per cohort: week-8 retention, baseline week-8 retention, relative retention; plus the entry-channel pair for the World Cup | Relative retention; Retention | baseline: `Segment` = ALL, `Week N` = 8. retention: all dimensions ALL, `Week N` = 8; second tile with `Entry Channel` split |
| 2.2 | Retention by weeks since first fill | `cohort_retention` | line chart, weeks 0–8 | Columns `Week N`; Rows Retention; Colour `Cohort` | `Cohort` excludes ALL; `Entry Category`, `Segment At Entry`, `Scripted`, `Entry Channel` from the dropdowns |
| 2.3 | Relative retention (calendar-adjusted) | `cohort_calendar_baseline` | two panels sharing `Week N`: (a) cohort and baseline retention as solid and dashed lines per cohort; (b) Relative retention, one line per cohort, with a reference line at 1.0 | `Week N`, Cohort retention, Baseline retention, Relative retention; Colour `Cohort` | `Segment` single-value (ALL or retail), fed by the Segment-at-entry dropdown through a parameter; weeks 1–8 on panel (b) |
| 2.4 | Entry channel | `cohort_retention` | small multiples: one panel for World Cup, one for the election; lines for "Via event market" and "Other market, same window" | `Week N`, Retention; Colour `Entry Channel` | `Cohort` in (world_cup_2026, us_election_2024); `Entry Channel` excludes ALL. The Entry channel dropdown does not apply |
| 2.5 | Retention by entry category | `cohort_retention` | heat map: rows `Entry Category`, columns `Week N`, colour and label Retention; one cohort at a time | as listed | `Cohort` single-value (parameter-driven, default World Cup); `Entry Category` excludes ALL. The Entry category dropdown does not apply |
| 2.6 | Retention by segment at entry | `cohort_retention` | lines, weeks 0–8 | Colour `Segment At Entry` | `Cohort` single-value; `Segment At Entry` excludes ALL |
| 2.7 | Monthly acquisition cohorts | `cohort_retention` with `[Cohort Type]` = month | triangle heat map: rows `Cohort` (month), columns `Week N`, colour Retention; cells not fully observable are blank | `Cohort`, `Week N`, Retention, Fully observable? | `Cohort` excludes ALL; mark monthly cohorts with `Is Left Censored Cohort` = true by a row label "left-censored" |
| 2.8 | The long view | `cohort_retention_monthly` | lines, 30-day periods 0–23; lines stop where a cohort is no longer fully observable | `Month N`, Retention; Colour `Cohort`; filter Fully observable? = True | as 2.2 |
| 2.9 | Cross-sell summary | `cohort_category_transition` | three bars per cohort: "Any category", "Any other category", "None (did not trade in weeks 1–8)" | Transition share | `Entry Category` = ALL, `Segment At Entry` from the dropdown; `Traded Category` in the three summary values |
| 2.10 | Category transition matrix | `cohort_category_transition` | heat map: rows `Entry Category`, columns `Traded Category`, colour and label Transition share; outline the diagonal (stayed) | as listed; tooltip `Base Wallets`, `Wallets` | one `Cohort`; `Entry Category` excludes ALL; `Traded Category` excludes the three summary values. Subtitle: "Share of each entry category's wallets that traded the column category in weeks 1–8. Rows do not sum to 100%." |
| 2.11 | Cohort sizes | `cohort_retention` | table: cohort, wallets, share via event market, share scripted | `Cohort Wallets`, Share entering via the event market | `Week N` = 0 |

Annotations and captions on this dashboard:

- Under 2.2 and 2.3: "Week n is counted from each wallet's own first fill; week 0 is its first 7 days and is 100% by construction. Only wallets whose week lies wholly inside the data are counted."
- Under 2.3: "Baseline: wallets first seen at least 12 weeks before the window opened and active inside it, measured over the same calendar weeks. Relative retention below 1 means the cohort fades faster than established wallets did in those weeks."
- On 2.7 and 2.8: a wallet's weeks 0–8 cover the 63 days from its first fill, so wallets first seen from 25 Feb to 28 Apr 2026 have the 28 Apr 2026 cutover inside that span. Add the tooltip note "Weeks 0–8 include the 28 April 2026 exchange cutover" on the monthly cohorts of Feb, Mar and Apr 2026 and on the Q1 2026 cohort, and the "left-censored" label on cohorts before Mar 2023. The cohorts of Nov 2022 and Oct 2026 are partial months.
- On 2.8: "The World Cup cohort is fully observable for periods 0–1 only; the election cohort for periods 0–22."

### Headline numbers this dashboard must show

1. **Relative week-8 retention: World Cup 0.27, 2024 election 0.53, Q1 2026 0.54** (baseline week-8 retention 21.4%, 25.1%, 33.9%).
2. **Week-8 retention: World Cup 5.7%, 2024 election 13.3%, Q1 2026 18.4%** (week 1: 30.2%, 29.6%, 39.7%; week 4: 12.5%, 21.8%, 25.2%).
3. **Entry channel, World Cup: 4.2% via a World Cup market against 7.7% via other markets in the same window** (129,299 and 97,624 wallets). For the election cohort the order is reversed: 13.7% via an election market, 12.1% other.
4. **Cross-sell in weeks 1–8: 16.5% of the World Cup cohort traded outside its entry category, against 39.2% (election) and 40.4% (Q1 2026)**; 61.1% of the World Cup cohort did not trade again in weeks 1–8.
5. **Cohort sizes: World Cup 226,923 wallets (57.0% entered via a World Cup market), 2024 election 218,361 (73.7% via an election market), Q1 2026 713,066.**

Retail-only versions of numbers 1–3 differ by less than a point (week-8 relative retention 0.27 / 0.57 / 0.55), which is worth one line of text: the result is not driven by automated or scripted wallets.

---

## 3. Dashboard 3 — Listing scorecard

**Question it answers:** which listings bring in new wallets, which only serve existing ones, and which attract almost no one?

### Data sources and relationships

| Data source | Used for | Relationship |
|---|---|---|
| `listing_scorecard` | one row per listing (one-off event or recurring series) | to `series_month` on `Listing Key` = `Series Key` (one to many), used only by sheet 3.6 |
| `series_month` | a recurring series over time | — |

### Filters (single-value dropdowns; Tableau's "(All)" is safe here because there are no `ALL` rows)

| Filter | Field | Default |
|---|---|---|
| Listing type | `Listing Type` (event, series) | (All) |
| Category | `Category` | (All) |
| Status | calculated field `Status` (below) | Traded |
| World Cup listings only | `Is World Cup` | (All) |
| First traded | `First Fill Date` (range) | everything |
| Minimum traders | `Traders Band` | (All) |

### Calculated fields

```
// Status
IF [Never Traded] THEN "Never traded"
ELSEIF [Low Traction] THEN "Low traction (under 10 traders)"
ELSE "Traded" END

// Acquired share (recomputed so that groups of listings aggregate correctly)
SUM([Wallets Acquired]) / SUM([Traders])

// Existing share
1 - SUM([Wallets Acquired]) / SUM([Traders])

// Acquired wallets that went on to trade elsewhere
SUM([Acquired Wallets Trading Elsewhere]) / SUM([Wallets Acquired])

// Notional per trader (shares)
SUM([Notional Shares]) / SUM([Traders])

// Notional per day of active life
SUM([Notional Shares]) / SUM([Active Life Days])

// Quadrant
IF [Traders] >= 1000 AND [Acquired Share] >= 0.25 THEN "Acquirer"
ELSEIF [Traders] >= 1000 THEN "Serves existing wallets"
ELSEIF [Traders] >= 10 THEN "Niche"
ELSE "Low traction" END

// Listing label
IF [Listing Type] = "series" THEN [Listing Title] + " (series, " + STR([Events]) + " events)" ELSE [Listing Title] END
```

`Traders` and `Wallets Acquired` are per listing and a wallet can trade many listings, so **never show a sum of `Traders` across listings as a number of wallets**. Sums of `Wallets Acquired` are valid (each wallet is acquired by exactly one listing) but understate the total where counts under 10 are suppressed. Use `Traders Band` and `Acquired Band` wherever an exact count is null.

### Sheets

| # | Sheet | Chart | Fields | Notes |
|---|---|---|---|---|
| 3.1 | Headline tiles | 5 big numbers | `COUNT([listing_scorecard])` by Status; top acquirer; share of notional from series | — |
| 3.2 | Listings by status and category | stacked bars: number of listings and notional | Rows `Category`; Colour Status; `CNT(Listing Key)`, `SUM([Notional Shares])` | Status filter set to (All) on this sheet |
| 3.3 | Acquirers and servers | scatter, log axes | Columns `SUM([Traders])`; Rows Acquired share; Size `SUM([Notional Shares])`; Colour `Category`; Detail `Listing Key` | Filter `Traders` not null. Reference lines at 1,000 traders and 25% acquired share (the Quadrant thresholds). Tooltip: listing label, traders, wallets acquired, acquired share, went on to trade elsewhere, active life, notional |
| 3.4 | Top listings by wallets acquired | ranked bar table, top 25 | Listing label, `Wallets Acquired`, Acquired share, Acquired wallets that went on to trade elsewhere, `Active Life Days`, `Notional Shares` | Sort by `Wallets Acquired` descending |
| 3.5 | Low traction and never traded | bars by category and by month of first fill | `CNT(Listing Key)` by Status; for low traction also `SUM([Notional Shares])` | Never-traded listings have no fill date: show them by category only |
| 3.6 | A recurring series over time | lines by month for one selected series | `series_month`: `Month Start`, `Events`, `Traders`, `Wallets Acquired`, `Notional Shares` | Driven by selecting a series in 3.4; partial-month bands on Nov 2022 and Oct 2026; 28 Apr 2026 reference line |
| 3.7 | Listing detail | table for the selected listing | all scorecard fields incl. `Share Markets Within 1pct Of Source Volume` | — |

Annotations: 3.5 and 3.6 carry the §0 lines where they have a date axis. Under 3.3: "A recurring series (for example the 5- and 15-minute bitcoin up-or-down markets) is one listing. Trader counts under 10 are not shown."

### Headline numbers this dashboard must show

1. **277,996 listings: 59,677 never traded (21%) and 37,121 traded with fewer than 10 traders (13%).**
2. **Volume is concentrated: 165 listings make half of all notional volume.**
3. **Recurring series: 345 listings, 18% of notional volume**; the bitcoin up-or-down series alone acquired 322,296 wallets, the most of any listing.
4. **Best acquirer among one-off events: the 2024 presidential election winner market, 180,533 wallets acquired, 70% of its traders new to the platform.**
5. **The World Cup winner market had 281,682 traders but acquired only 39,486 (14%)**; 150,847 traded listings acquired no wallet at all.

---

## 4. Methodology page

One dashboard of tables and short text. No cube rules apply.

| # | Sheet | Source | Content |
|---|---|---|---|
| 4.1 | Metric definitions | `metric_definition` | Table sorted by `Sort Order`: `Metric`, `Definition`, `Source`, `Caveat`. Lead rows: notional shares is the platform headline; cash traded is "by wallets (both sides)" and not comparable with it |
| 4.2 | Bronze to silver, month by month | `recon_monthly` | Table and bars: `Trades Rows Bronze` vs `Trades Rows Silver`, `Wallet Fills Rows Bronze` vs `Wallet Fills Rows Silver`, `Maker Rows Equal Fills`, `Checks Passed` / `Checks Run`; highlight `Is Partial Month` |
| 4.3 | Source clock error | `recon_monthly` | Bar per month of `Max Source Clock Error Seconds` (log axis): shows why chain time is used |
| 4.4 | Fills against published market volume | `recon_volume` | 100% stacked bar per `Period`, coloured by `Status` (`Pct Of Period Markets`) |
| 4.5 | Taxonomy coverage | `taxonomy_coverage`, `taxonomy_entry_coverage` | Stacked bars by `Year` of `Pct Notional` by `Category Source`; table of entering wallets by cohort |
| 4.6 | Wallet segments | `segment_summary` | Lines by `Month Start`: `Pct Wallets`, `Pct Wallet Fills`, `Pct Cash Traded` by `Segment`; partial-month bands |
| 4.7 | Tests | `dq_summary` | Count of tests by `Layer` and `Passed`; expandable table of every test with `Expected` and `Actual` |
| 4.8 | Small-cell suppression | `suppression_log` | Table: `Table Name`, `Rows Kept`, `Coverage Measure`, `Coverage Pct`, `Values Nulled`, `Note` |
| 4.9 | Known issues and notes | `annotation` plus fixed text | The annotation table, and the text below |

Calculated fields:

```
// recon_monthly: bronze equals silver
[Trades Rows Bronze] = [Trades Rows Silver] AND [Wallet Fills Rows Bronze] = [Wallet Fills Rows Silver]

// recon_monthly: checks passed
SUM([Checks Passed]) / SUM([Checks Run])

// dq_summary: tests passed
SUM(INT([Passed])) / COUNT([Test])

// recon_volume: share of markets within 1%
SUM(IF [Status] = "within_1pct" THEN [Markets] END) / SUM([Markets])
```

Fixed text for 4.9 (from the decision log of `docs/PROJECT_BRIEF.md`):

- 28 Apr 2026: the exchange moved to V2. Active established wallets step down that day (74,041 to 47,814). The step is annotated, not adjusted.
- 29–30 Apr 2026: fills are short of published volume for markets first traded on those two days.
- Before the cutover, fills exceed the published volume for a minority of recurring crypto markets (not investigated).
- The source's own timestamps are off by up to 23 hours between Oct 2024 and Oct 2025; all times here come from the chain.
- Retail is not purely human: wallets with 1,000–9,999 fills a month that are active in under a quarter of hours stay retail.
- 207 taker rows (207 wallets, $6,954) have no matching fill; they count towards activity and cash traded only.
- Gamma tags were fetched for the 160,000 largest of 219,622 one-off events; the rest are categorised by series or slug keywords, and 0.02% of 2025–26 notional volume is uncategorised.

### Headline numbers this page must show

1. **1,266,885,846 fills and 2,026,673,731 wallet fills in silver, equal to the source in all 48 months.**
2. **136 tests, all passing** (27 per-month, 53 whole-layer silver, 56 gold).
3. **Fills reconcile with published market volume within 1% for 92.6% of markets before the V2 cutover and 96.6% after.**
4. **0.02% of 2025–26 notional volume is uncategorised** (0.08% in 2024).
5. **Small-cell suppression hides under 0.2% of any table's measure at the finest grain; totals are exact.**

---

## 5. Build order and checks

1. Connect the CSVs as separate data sources; set data types (§0); create the aliases.
2. Build dashboard 2 first: it carries the main finding. Check the lead tiles against the numbers in §2 before styling anything.
3. Build dashboards 1 and 3, then the methodology page.
4. For every sheet on a cube table, confirm the double-count guard is blank for every filter combination a user can reach, and that changing a dropdown never changes a total that should not move.
5. Check each dashboard's headline numbers against its list above. If one differs, the cause is almost always a filter left on Tableau's "(All)" or a split sheet that still includes the `ALL` row.
6. Before publishing: no sheet shows a wallet count from 1 to 9; the caption of §0 is on every dashboard; the data-source page lists no field named for a wallet id or address (the export refuses such columns, so this is a second look).
