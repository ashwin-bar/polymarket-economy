# Databricks notebook source
# Silver market dimension and test H (docs/SILVER_PLAN.md §4).
#
#   polymarket_silver.dim_event   one row per event in markets.parquet, with its category
#   polymarket_silver.dim_market  one row per market: source fields, fill totals, the
#                                 reconciliation against markets.volume, and the event's category
#
# Category is decided per event (Gamma tags are per event), in this order:
#   1. slug_series    the event's slug stem is a recurring series (20+ events): the series takes
#                     the most common category among its sampled events, so a series is never
#                     split across categories
#   2. gamma_tag      the event's own Gamma tags through config/tag_category_map.csv: the
#                     matched tag with the lowest category_priority decides the category, and
#                     within that category the lowest subcategory_priority decides the subcategory
#   3. slug_keyword   config/slug_category_rules.csv on the event slug (first matching rule)
#   4. uncategorised
#
# Inputs: polymarket_silver.market_month (fills per market and month), markets.parquet and the
# Gamma event extract, both in the bronze Volume. Per-market, per-event and per-series totals
# are worked out here as views, so no other staging table is needed.
#
# Test H is the coverage of that, measured on notional shares, fills and entering wallets.
# Nothing wallet-level leaves the notebook: wallet results are counts.
#
# Parameters: config_dir (workspace folder holding the two CSVs), taxonomy_version

# COMMAND ----------

import csv
import json
import time

dbutils.widgets.text("config_dir", "")  # noqa: F821 (passed by scripts/16_load_silver.py)
dbutils.widgets.text("taxonomy_version", "v1")  # noqa: F821
config_dir, version = dbutils.widgets.get("config_dir"), dbutils.widgets.get("taxonomy_version")  # noqa: F821
if not config_dir:
    raise ValueError("config_dir is required: the workspace folder holding the two taxonomy CSVs")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
MARKETS_PATH = "/Volumes/workspace/polymarket/bronze/markets/markets.parquet"
GAMMA_FILES = "/Volumes/workspace/polymarket/bronze/gamma_events/chunk-*.jsonl"
GAMMA_SCHEMA = ("id string, missing boolean, slug string, title string, category string, subcategory string, closed boolean, "
                "startDate string, endDate string, volume double, n_markets int, "
                "tags array<struct<id:string,slug:string,label:string>>, "
                "series array<struct<id:string,slug:string,title:string,recurrence:string>>, fetched_at string")
RECURRING_MIN_EVENTS = 20  # a slug stem shared by this many traded events is a recurring series
STEM = """regexp_replace(regexp_replace(regexp_replace(lower(event_slug),
          '(january|february|march|april|may|june|july|august|september|october|november|december)', 'MON'),
          '[0-9]+', '#'), '(#[-:]?)+(am|pm)?', '#')"""  # must stay identical to dbx_silver_market_stats.py
result, problems, timings = {"taxonomy_version": version}, [], {}
started = time.time()


def rows(sql: str) -> list[dict]:
    return [r.asDict() for r in spark.sql(sql).collect()]  # noqa: F821


def q(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


with open(f"{config_dir}/tag_category_map.csv", newline="", encoding="utf-8") as f:
    tag_map = [(r["tag_slug"], r["category"], int(r["category_priority"]), r["subcategory"] or None,
                int(r["subcategory_priority"] or 99)) for r in csv.DictReader(f)]
with open(f"{config_dir}/slug_category_rules.csv", newline="", encoding="utf-8") as f:
    slug_rules = sorted(csv.DictReader(f), key=lambda r: int(r["priority"]))
if len({t[0] for t in tag_map}) != len(tag_map):
    raise RuntimeError("tag_category_map.csv lists a tag more than once")
spark.createDataFrame(tag_map, "tag_slug string, category string, category_priority int, subcategory string, "  # noqa: F821
                               "subcategory_priority int").createOrReplaceTempView("tag_map")
result["config"] = {"mapped_tags": len(tag_map), "slug_rules": len(slug_rules)}

# COMMAND ----------

(spark.read.schema(GAMMA_SCHEMA).json(GAMMA_FILES).withColumnRenamed("id", "event_id")  # noqa: F821
 .createOrReplaceTempView("gamma_event"))
spark.sql(f"""
    SELECT market_id, sum(fills) AS fills, sum(notional_shares) AS notional_shares, sum(usd_amount) AS usd_amount,
           min(first_fill_ts) AS first_fill_ts, max(last_fill_ts) AS last_fill_ts
    FROM {SILVER}.market_month GROUP BY market_id""").createOrReplaceTempView("market_stats")  # noqa: F821

# Each fetched event's category from its own tags.
spark.sql(f"""
    SELECT g.event_id, t.slug AS tag_slug, m.category, m.category_priority, m.subcategory, m.subcategory_priority
    FROM (SELECT event_id, explode(tags) AS t FROM gamma_event WHERE NOT missing) g
    JOIN tag_map m ON m.tag_slug = t.slug""").createOrReplaceTempView("matched")  # noqa: F821
spark.sql("""
    WITH cat AS (
        SELECT event_id, min_by(category, struct(category_priority, tag_slug)) AS category FROM matched GROUP BY event_id)
    SELECT c.event_id, c.category,
           min_by(m.subcategory, struct(m.subcategory_priority, m.tag_slug)) FILTER (WHERE m.subcategory IS NOT NULL) AS subcategory
    FROM cat c JOIN matched m ON m.event_id = c.event_id AND m.category = c.category
    GROUP BY c.event_id, c.category""").createOrReplaceTempView("tag_cat")  # noqa: F821

keyword_category = "CASE " + " ".join(
    f"WHEN event_slug RLIKE {q(r['pattern'])} THEN {q(r['category'])}" for r in slug_rules) + " END"
keyword_subcategory = "CASE " + " ".join(
    f"WHEN event_slug RLIKE {q(r['pattern'])} THEN {q(r['subcategory']) if r['subcategory'] else 'NULL'}"
    for r in slug_rules) + " END"

spark.sql(f"""
    SELECT CAST(event_id AS STRING) AS event_id, min(event_slug) AS event_slug, min(event_title) AS event_title,
           count(*) AS markets_in_file, count(DISTINCT event_slug) AS slugs
    FROM parquet.`{MARKETS_PATH}` GROUP BY 1""").createOrReplaceTempView("ev_file")  # noqa: F821
spark.sql(f"""
    SELECT f.*, {STEM} AS stem, {keyword_category} AS keyword_category, {keyword_subcategory} AS keyword_subcategory
    FROM ev_file f""").createOrReplaceTempView("ev_all")  # noqa: F821
spark.sql(f"""
    SELECT CAST(m.event_id AS STRING) AS event_id, count(*) AS markets, sum(s.fills) AS fills,
           sum(s.notional_shares) AS notional_shares, min(s.first_fill_ts) AS first_fill_ts, max(s.last_fill_ts) AS last_fill_ts
    FROM market_stats s JOIN parquet.`{MARKETS_PATH}` m ON CAST(m.id AS STRING) = s.market_id
    GROUP BY 1""").createOrReplaceTempView("event_stats")  # noqa: F821
spark.sql(f"""
    SELECT a.stem, count(*) AS events, sum(e.notional_shares) AS notional_shares,
           count(*) >= {RECURRING_MIN_EVENTS} AS is_recurring
    FROM event_stats e JOIN ev_all a USING (event_id) GROUP BY a.stem""").createOrReplaceTempView("series_stem")  # noqa: F821
# A recurring series' category: the most common one among its events that have tags.
spark.sql(f"""
    WITH tagged AS (
        SELECT a.stem, c.category, c.subcategory
        FROM ev_all a JOIN series_stem s ON s.stem = a.stem AND s.is_recurring
        JOIN tag_cat c USING (event_id))
    SELECT stem, category, subcategory, count(*) AS n FROM tagged GROUP BY stem, category, subcategory""") \
    .createOrReplaceTempView("stem_votes")  # noqa: F821
spark.sql("""
    WITH cat AS (
        SELECT stem, category, sum(n) AS agreeing, sum(sum(n)) OVER (PARTITION BY stem) AS sampled,
               row_number() OVER (PARTITION BY stem ORDER BY sum(n) DESC, category) AS pick
        FROM stem_votes GROUP BY stem, category),
    sub AS (
        SELECT stem, category, subcategory,
               row_number() OVER (PARTITION BY stem, category ORDER BY n DESC, subcategory) AS pick
        FROM stem_votes WHERE subcategory IS NOT NULL)
    SELECT c.stem, c.category, s.subcategory, c.sampled, c.agreeing
    FROM cat c LEFT JOIN sub s ON s.stem = c.stem AND s.category = c.category AND s.pick = 1
    WHERE c.pick = 1""").createOrReplaceTempView("stem_cat")  # noqa: F821

t = time.time()
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.dim_event AS
    WITH base AS (
        SELECT a.event_id, a.event_slug, a.event_title, a.stem, a.markets_in_file,
               coalesce(ss.is_recurring, false) AS is_recurring_series,
               e.markets AS traded_markets, e.fills, e.notional_shares, e.first_fill_ts, e.last_fill_ts,
               g.event_id IS NOT NULL AND NOT g.missing AS in_gamma,
               CASE WHEN g.missing THEN NULL ELSE transform(g.tags, x -> x.slug) END AS gamma_tags,
               get(g.series, 0).slug AS gamma_series_slug, get(g.series, 0).recurrence AS gamma_series_recurrence,
               sc.category AS series_category, sc.subcategory AS series_subcategory,
               tc.category AS tag_category, tc.subcategory AS tag_subcategory,
               a.keyword_category, a.keyword_subcategory
        FROM ev_all a
        LEFT JOIN series_stem ss USING (stem)
        LEFT JOIN event_stats e USING (event_id)
        LEFT JOIN gamma_event g USING (event_id)
        LEFT JOIN stem_cat sc USING (stem)
        LEFT JOIN tag_cat tc USING (event_id))
    SELECT event_id, event_slug, event_title, markets_in_file, traded_markets, fills, notional_shares,
           first_fill_ts, last_fill_ts,
           CASE WHEN is_recurring_series AND series_category IS NOT NULL THEN series_category
                WHEN tag_category IS NOT NULL THEN tag_category
                WHEN keyword_category IS NOT NULL THEN keyword_category ELSE 'Uncategorised' END AS category,
           CASE WHEN is_recurring_series AND series_category IS NOT NULL THEN series_subcategory
                WHEN tag_category IS NOT NULL THEN tag_subcategory
                WHEN keyword_category IS NOT NULL THEN keyword_subcategory END AS subcategory,
           CASE WHEN is_recurring_series AND series_category IS NOT NULL THEN 'slug_series'
                WHEN tag_category IS NOT NULL THEN 'gamma_tag'
                WHEN keyword_category IS NOT NULL THEN 'slug_keyword' ELSE 'uncategorised' END AS category_source,
           in_gamma, gamma_tags, gamma_series_slug, gamma_series_recurrence,
           is_recurring_series, CASE WHEN is_recurring_series THEN stem END AS series_key,
           coalesce(arrays_overlap(gamma_tags, array('2026-fifa-world-cup', 'fifa-world-cup', 'world-cup',
                                                     'wc-tournament-futures', 'wc-player-futures', 'world-cup-awards')), false)
               OR event_slug RLIKE '(^|-)(fifa-)?world-cup(-|$)|^fifwc-' AS world_cup_candidate,
           coalesce(arrays_overlap(gamma_tags, array(
               '2024-presidential-election', '2024-election', '2024-elections', 'us-presidential-election-2024',
               '2024-us-presidential-election', 'presidential-election-2024', '2024-presidential-nomination',
               '2024-nomination', '2024-vp-nomination')), false)
               OR (coalesce(arrays_overlap(gamma_tags, array(
                       'us-presidential-election', 'us-elections', 'deprec-us-election', 'swing-states', 'senate-elections',
                       'house-races', 'state-presidential-election', 'margin-of-victory', 'popular-vote')), false)
                   AND last_fill_ts < TIMESTAMP '2025-02-01') AS is_us_election_2024,
           '{version}' AS taxonomy_version
    FROM base""")  # noqa: F821
timings["dim_event_s"] = round(time.time() - t, 1)

# COMMAND ----------

# World Cup 2026 flag. A candidate has a World Cup tag, a fifwc- match slug or "world-cup" in
# its slug. Those also catch other tournaments (the 2025 Club World Cup, the cricket T20, esports
# and chess world cups, the 2030 World Cup), so a candidate only counts if it still traded on or
# after 1 December 2025 (the draw was on 5 December 2025) and its slug names none of those.
# Friendlies (fif- slugs) are not World Cup markets.
t = time.time()
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.dim_market AS
    WITH e AS (
        SELECT *, world_cup_candidate AND last_fill_ts >= TIMESTAMP '2025-12-01'
                  AND NOT event_slug RLIKE 'club-world-cup|esports|icc-|t20|cricket|rugby|fide|chess|womens|2030' AS is_world_cup
        FROM {SILVER}.dim_event)
    SELECT CAST(m.id AS STRING) AS market_id, m.question, m.slug, e.event_id, e.event_slug, e.event_title,
           m.closed = 1 AS closed, m.neg_risk = 1 AS neg_risk, m.volume AS markets_volume,
           m.created_at AS source_created_at, m.end_date AS source_end_date,
           s.first_fill_ts, s.last_fill_ts, coalesce(s.fills, 0) AS fills,
           coalesce(s.notional_shares, 0) AS notional_shares, coalesce(s.usd_amount, 0) AS usd_amount,
           s.notional_shares / nullif(m.volume, 0) AS volume_ratio,
           CASE WHEN s.market_id IS NULL THEN 'no_fills'
                WHEN m.closed != 1 THEN 'not_closed'
                WHEN coalesce(m.volume, 0) <= 0 THEN 'no_source_volume'
                WHEN abs(s.notional_shares / m.volume - 1) <= 0.01 THEN 'within_1pct'
                WHEN s.notional_shares < m.volume THEN 'short' ELSE 'over' END AS volume_recon_status,
           e.category,
           CASE WHEN e.category = 'Sports' AND e.is_world_cup THEN 'Soccer: World Cup 2026'
                WHEN e.category = 'Crypto' AND e.is_recurring_series THEN 'Recurring price series'
                ELSE coalesce(e.subcategory, 'Other ' || lower(e.category)) END AS subcategory,
           e.category_source, e.gamma_tags, e.gamma_series_slug, e.gamma_series_recurrence,
           e.is_recurring_series, e.series_key, e.is_world_cup, e.is_us_election_2024, e.taxonomy_version
    FROM parquet.`{MARKETS_PATH}` m
    JOIN e ON e.event_id = CAST(m.event_id AS STRING)
    LEFT JOIN market_stats s ON s.market_id = CAST(m.id AS STRING)""")  # noqa: F821
timings["dim_market_s"] = round(time.time() - t, 1)
DIM = f"{SILVER}.dim_market"

# COMMAND ----------

# Data quality (SILVER_PLAN §6).
d = spark.sql(f"""
    SELECT count(*) AS n, count(DISTINCT market_id) AS distinct_ids, count_if(category IS NULL) AS null_category,
           count_if(subcategory IS NULL) AS null_subcategory, sum(fills) AS fills,
           round(sum(notional_shares), 0) AS notional_shares, count_if(fills > 0) AS traded_markets,
           (SELECT count(*) FROM market_stats s LEFT ANTI JOIN {DIM} d USING (market_id)) AS traded_markets_not_in_dim,
           (SELECT count(*) FROM {SILVER}.dim_event) AS events,
           (SELECT count(*) FROM {SILVER}.dim_event WHERE is_recurring_series) AS recurring_events
    FROM {DIM}""").first().asDict()  # noqa: F821
result["dim_market"] = d
if d["n"] != 4_049_900 or d["distinct_ids"] != d["n"]:
    problems.append(f"dim_market rows {d['n']}, distinct ids {d['distinct_ids']}, expected 4,049,900 unique")
if d["null_category"] or d["null_subcategory"] or d["traded_markets_not_in_dim"]:
    problems.append(f"null category {d['null_category']}, null subcategory {d['null_subcategory']}, "
                    f"traded markets not in dim {d['traded_markets_not_in_dim']}")
if d["fills"] != 1_266_885_846:
    problems.append(f"fills {d['fills']} != 1,266,885,846")

# Reconciliation with markets.volume by period, as in the V2 follow-up (scripts/dbx_v2_completeness.py):
# closed markets with source volume and fills; pre = last fill before 28 April 2026, post = first
# fill on or after 29 April 2026. That run gave 92.6% (pre) and 96.6% (post) of markets within 1%.
recon = rows(f"""
    SELECT CASE WHEN last_fill_ts < TIMESTAMP '2026-04-28' THEN 'pre'
                WHEN first_fill_ts >= TIMESTAMP '2026-04-29' THEN 'post' ELSE 'spans' END AS period,
           count(*) AS markets, round(sum(notional_shares) / sum(markets_volume), 4) AS ratio_of_totals,
           round(100.0 * count_if(volume_recon_status = 'within_1pct') / count(*), 2) AS pct_within_1pct,
           round(100.0 * count_if(volume_recon_status = 'short') / count(*), 2) AS pct_short,
           round(100.0 * count_if(volume_recon_status = 'over') / count(*), 2) AS pct_over
    FROM {DIM} WHERE volume_recon_status IN ('within_1pct', 'short', 'over') GROUP BY 1 ORDER BY 1""")
result["volume_recon_by_period"] = recon
got = {r["period"]: float(r["pct_within_1pct"]) for r in recon}
if abs(got.get("pre", 0) - 92.6) > 0.15 or abs(got.get("post", 0) - 96.6) > 0.15:
    problems.append(f"volume reconciliation does not reproduce the V2 follow-up (92.6 / 96.6): {got}")
result["volume_recon_status"] = rows(f"SELECT volume_recon_status, count(*) AS markets FROM {DIM} GROUP BY 1 ORDER BY 2 DESC")

# COMMAND ----------

# Test H: coverage. Volume is taken per market and month, so a year is the year the fills happened.
spark.sql(f"""
    SELECT year(mm.month_start) AS yr, mm.month_start, d.category, d.subcategory, d.category_source,
           d.is_recurring_series, d.is_world_cup, d.is_us_election_2024, mm.fills, mm.notional_shares, mm.market_id
    FROM {SILVER}.market_month mm JOIN {DIM} d USING (market_id)""").createOrReplaceTempView("vol")  # noqa: F821
result["coverage_by_year_source"] = rows("""
    SELECT yr, category_source, count(DISTINCT market_id) AS markets, sum(fills) AS fills,
           round(sum(notional_shares) / 1e6, 1) AS notional_m,
           round(100.0 * sum(notional_shares) / sum(sum(notional_shares)) OVER (PARTITION BY yr), 2) AS pct_notional,
           round(100.0 * sum(fills) / sum(sum(fills)) OVER (PARTITION BY yr), 2) AS pct_fills
    FROM vol GROUP BY yr, category_source ORDER BY yr, category_source""")
result["coverage_2025_26"] = rows("""
    SELECT category_source, count(DISTINCT market_id) AS markets,
           round(100.0 * count(DISTINCT market_id) / sum(count(DISTINCT market_id)) OVER (), 2) AS pct_markets,
           round(100.0 * sum(notional_shares) / sum(sum(notional_shares)) OVER (), 2) AS pct_notional,
           round(100.0 * sum(fills) / sum(sum(fills)) OVER (), 2) AS pct_fills
    FROM vol WHERE yr >= 2025 GROUP BY category_source ORDER BY category_source""")
result["category_by_year_pct_notional"] = rows("""
    SELECT yr, category, round(sum(notional_shares) / 1e6, 1) AS notional_m,
           round(100.0 * sum(notional_shares) / sum(sum(notional_shares)) OVER (PARTITION BY yr), 2) AS pct_notional,
           round(100.0 * sum(fills) / sum(sum(fills)) OVER (PARTITION BY yr), 2) AS pct_fills
    FROM vol GROUP BY yr, category ORDER BY yr, notional_m DESC""")
result["subcategory_2025_26"] = rows("""
    SELECT category, subcategory, count(DISTINCT market_id) AS markets, round(sum(notional_shares) / 1e6, 1) AS notional_m,
           round(100.0 * sum(notional_shares) / sum(sum(notional_shares)) OVER (), 2) AS pct_notional
    FROM vol WHERE yr >= 2025 GROUP BY category, subcategory ORDER BY category, notional_m DESC""")
result["top_uncategorised_events"] = rows(f"""
    SELECT event_id, event_slug, in_gamma, round(notional_shares / 1e6, 2) AS notional_m,
           CAST(first_fill_ts AS DATE) AS first_fill, array_join(slice(gamma_tags, 1, 8), ',') AS tags
    FROM {SILVER}.dim_event WHERE category = 'Uncategorised' AND notional_shares IS NOT NULL
    ORDER BY notional_shares DESC LIMIT 50""")
result["uncategorised_events"] = rows(f"""
    SELECT in_gamma, gamma_tags IS NOT NULL AND size(gamma_tags) > 0 AS has_tags, count(*) AS traded_events,
           round(sum(notional_shares) / 1e6, 1) AS notional_m
    FROM {SILVER}.dim_event WHERE category = 'Uncategorised' AND notional_shares IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2""")
result["top_unmapped_tags_on_uncategorised"] = rows(f"""
    SELECT tag, count(*) AS events, round(sum(notional_shares) / 1e6, 1) AS notional_m
    FROM (SELECT explode(gamma_tags) AS tag, notional_shares FROM {SILVER}.dim_event
          WHERE category = 'Uncategorised' AND notional_shares IS NOT NULL)
    GROUP BY tag ORDER BY sum(notional_shares) DESC LIMIT 60""")
result["series_with_split_samples"] = rows("""
    SELECT stem, category, sampled, agreeing FROM stem_cat WHERE agreeing < sampled ORDER BY sampled - agreeing DESC LIMIT 40""")
result["recurring_series_without_category"] = rows(f"""
    SELECT s.stem, s.events, round(s.notional_shares / 1e6, 2) AS notional_m
    FROM series_stem s LEFT ANTI JOIN stem_cat c USING (stem) WHERE s.is_recurring ORDER BY s.notional_shares DESC LIMIT 30""")

# Entering wallets: how many first traded in a market we cannot categorise (counts only).
spark.sql(f"""
    SELECT w.first_date, d.category, d.subcategory, d.category_source, d.is_world_cup, d.is_us_election_2024,
           CASE WHEN w.first_date BETWEEN DATE '2026-06-11' AND DATE '2026-07-19' THEN 'world_cup_2026'
                WHEN w.first_date BETWEEN DATE '2024-09-28' AND DATE '2024-11-05' THEN 'us_election_2024'
                WHEN w.first_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31' THEN 'benchmark_q1_2026' END AS cohort
    FROM {SILVER}.dim_wallet w JOIN {DIM} d ON d.market_id = w.first_market_id""").createOrReplaceTempView("entry")  # noqa: F821
result["entry_wallets_by_source"] = rows("""
    SELECT cohort_or_rest, category_source, count(*) AS wallets,
           round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY cohort_or_rest), 2) AS pct
    FROM (SELECT coalesce(cohort, 'all other wallets') AS cohort_or_rest, category_source FROM entry)
    GROUP BY cohort_or_rest, category_source ORDER BY cohort_or_rest, category_source""")
result["entry_wallets_total"] = rows("""
    SELECT count(*) AS wallets, round(100.0 * count_if(category = 'Uncategorised') / count(*), 2) AS pct_uncategorised FROM entry""")
result["entry_category_by_cohort"] = rows("""
    SELECT cohort, category, count(*) AS wallets, round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY cohort), 2) AS pct
    FROM entry WHERE cohort IS NOT NULL GROUP BY cohort, category ORDER BY cohort, wallets DESC""")
result["cohort_entered_via_flagged_market"] = rows("""
    SELECT cohort, count(*) AS wallets, count_if(is_world_cup) AS via_world_cup_market,
           round(100.0 * count_if(is_world_cup) / count(*), 2) AS pct_via_world_cup,
           count_if(is_us_election_2024) AS via_us_election_2024_market,
           round(100.0 * count_if(is_us_election_2024) / count(*), 2) AS pct_via_us_election_2024
    FROM entry WHERE cohort IS NOT NULL GROUP BY cohort ORDER BY cohort""")

# The two event flags: size, timing, and what they contain.
for flag in ("is_world_cup", "is_us_election_2024"):
    result[flag] = {
        "totals": rows(f"""
            SELECT count(DISTINCT event_id) AS events, count(*) AS markets, count_if(fills > 0) AS traded_markets,
                   round(sum(notional_shares) / 1e6, 1) AS notional_m FROM {DIM} WHERE {flag}"""),
        "by_month": rows(f"""
            SELECT date_format(month_start, 'yyyy-MM') AS month,
                   round(sum(CASE WHEN {flag} THEN notional_shares END) / 1e6, 1) AS notional_m,
                   round(100.0 * sum(CASE WHEN {flag} THEN notional_shares END) / sum(notional_shares), 2) AS pct_of_month
            FROM vol GROUP BY 1 HAVING sum(CASE WHEN {flag} THEN notional_shares END) > 0 ORDER BY 1"""),
        "by_category": rows(f"SELECT category, subcategory, count(*) AS markets FROM {DIM} WHERE {flag} GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10"),
        "top_series_or_events": rows(f"""
            SELECT coalesce(series_key, event_slug) AS series_or_event, count(DISTINCT event_id) AS events,
                   round(sum(notional_shares) / 1e6, 1) AS notional_m
            FROM {DIM} WHERE {flag} GROUP BY 1 ORDER BY sum(notional_shares) DESC LIMIT 25"""),
    }
result["world_cup_candidates_excluded"] = rows(f"""
    SELECT e.event_slug, round(e.notional_shares / 1e6, 2) AS notional_m, CAST(e.last_fill_ts AS DATE) AS last_fill
    FROM {SILVER}.dim_event e
    LEFT ANTI JOIN (SELECT DISTINCT event_id FROM {DIM} WHERE is_world_cup) d USING (event_id)
    WHERE e.world_cup_candidate AND e.notional_shares IS NOT NULL
    ORDER BY e.notional_shares DESC LIMIT 25""")

timings["total_s"] = round(time.time() - started, 1)
result.update(timings=timings, problems=problems)
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
