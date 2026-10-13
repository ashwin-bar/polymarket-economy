# Databricks notebook source
# Silver market dimension, step 1 (docs/SILVER_PLAN.md §4): what the fills say about each market,
# and what the taxonomy has to cover.
#
#   polymarket_silver.market_month  one row per market x UTC month, from polymarket_silver.trades
#                                   (kept: the market dimension and gold read it)
#   polymarket_silver.market_stats  one row per market with fills (whole-life totals)
#   polymarket_silver.event_stats   one row per event with fills, with its slug stem
#                                   (both staging for the Gamma fetch; dropped once the market
#                                   dimension is built, and recreated by re-running this notebook)
#
# Then a profile:
#   - do all traded markets exist in markets.parquet; how many events carry the notional volume
#   - recurring series: event slugs with numbers and month names blanked, ranked by notional
#   - a small Gamma pilot (one request by event id, one page by offset) to size the tag fetch
#
# Safe to re-run: the tables are replaced whole.

# COMMAND ----------

import gzip
import json
import time
import urllib.request

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821 (dbutils and spark are provided by Databricks)
SILVER = "workspace.polymarket_silver"
MARKETS_PATH = "/Volumes/workspace/polymarket/bronze/markets/markets.parquet"
EXPECTED_FILLS = 1_266_885_846
result, problems, timings = {}, [], {}

# COMMAND ----------

t = time.time()
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.market_month AS
    SELECT market_id, month_start, count(*) AS fills, sum(token_amount) AS notional_shares,
           sum(usd_amount) AS usd_amount, min(ts_utc) AS first_fill_ts, max(ts_utc) AS last_fill_ts
    FROM {SILVER}.trades GROUP BY market_id, month_start""")  # noqa: F821
timings["market_month_s"] = round(time.time() - t, 1)
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.market_stats AS
    SELECT market_id, sum(fills) AS fills, sum(notional_shares) AS notional_shares, sum(usd_amount) AS usd_amount,
           min(first_fill_ts) AS first_fill_ts, max(last_fill_ts) AS last_fill_ts, count(*) AS months_traded
    FROM {SILVER}.market_month GROUP BY market_id""")  # noqa: F821

markets = spark.read.parquet(MARKETS_PATH)  # noqa: F821
result["markets_schema"] = {f.name: f.dataType.simpleString() for f in markets.schema.fields}
markets.selectExpr("CAST(id AS STRING) AS market_id", "CAST(event_id AS STRING) AS event_id", "event_slug",
                   "event_title", "slug", "question").createOrReplaceTempView("m")

s = spark.sql(f"""
    SELECT count(*) AS traded_markets, sum(s.fills) AS fills, round(sum(s.notional_shares), 0) AS notional_shares,
           count_if(m.market_id IS NULL) AS traded_markets_not_in_markets_file,
           count_if(m.market_id IS NOT NULL AND m.event_id IS NULL) AS traded_markets_without_event_id,
           count_if(m.market_id IS NOT NULL AND m.event_slug IS NULL) AS traded_markets_without_event_slug,
           count(DISTINCT m.event_id) AS traded_events,
           (SELECT count(*) FROM {SILVER}.market_month) AS market_month_rows,
           (SELECT count(*) FROM m) AS markets_in_file, (SELECT count(DISTINCT market_id) FROM m) AS distinct_market_ids,
           (SELECT count(DISTINCT event_id) FROM m) AS events_in_file
    FROM {SILVER}.market_stats s LEFT JOIN m USING (market_id)""").first().asDict()  # noqa: F821
result["summary"] = s
if s["fills"] != EXPECTED_FILLS:
    problems.append(f"fills {s['fills']} != {EXPECTED_FILLS}")
if s["markets_in_file"] != s["distinct_market_ids"]:
    problems.append("market id is not unique in markets.parquet")

# COMMAND ----------

# Event-level notional, and how concentrated it is: how many events make 90 / 99 / 99.9% of it.
spark.sql(f"""
    SELECT m.event_id, any_value(m.event_slug) AS event_slug, any_value(m.event_title) AS event_title,
           count(*) AS markets, sum(s.fills) AS fills, sum(s.notional_shares) AS notional_shares,
           min(s.first_fill_ts) AS first_fill_ts,
           sum(CASE WHEN year(s.last_fill_ts) >= 2025 THEN s.notional_shares ELSE 0 END) AS notional_last_fill_2025_26
    FROM {SILVER}.market_stats s JOIN m USING (market_id) GROUP BY m.event_id""").createOrReplaceTempView("ev")  # noqa: F821
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.event_stats AS
    SELECT *, regexp_replace(regexp_replace(regexp_replace(lower(event_slug),
               '(january|february|march|april|may|june|july|august|september|october|november|december)', 'MON'),
               '[0-9]+', '#'), '(#[-:]?)+(am|pm)?', '#') AS stem,
           sum(notional_shares) OVER (ORDER BY notional_shares DESC, event_id) / sum(notional_shares) OVER () AS cum_share,
           row_number() OVER (ORDER BY notional_shares DESC, event_id) AS rnk
    FROM ev""")  # noqa: F821
spark.table(f"{SILVER}.event_stats").createOrReplaceTempView("ev2")  # noqa: F821
result["event_concentration"] = spark.sql("""
    SELECT count(*) AS events, min(CASE WHEN cum_share >= 0.50 THEN rnk END) AS events_for_50pct,
           min(CASE WHEN cum_share >= 0.90 THEN rnk END) AS events_for_90pct,
           min(CASE WHEN cum_share >= 0.99 THEN rnk END) AS events_for_99pct,
           min(CASE WHEN cum_share >= 0.999 THEN rnk END) AS events_for_99_9pct
    FROM ev2""").first().asDict()  # noqa: F821

stems = spark.sql("""
    SELECT stem, count(*) AS events, sum(markets) AS markets, round(sum(notional_shares) / 1e6, 2) AS notional_m,
           round(sum(notional_shares) / (SELECT sum(notional_shares) FROM ev2), 5) AS notional_share,
           min(event_slug) AS example_slug, CAST(min(first_fill_ts) AS DATE) AS first_seen
    FROM ev2 GROUP BY stem""")  # noqa: F821
stems.createOrReplaceTempView("stems")
result["stems_total"] = stems.count()
result["stems_with_20_plus_events"] = spark.sql("""
    SELECT count(*) AS stems, sum(events) AS events, sum(markets) AS markets, round(sum(notional_share), 4) AS notional_share
    FROM stems WHERE events >= 20""").first().asDict()  # noqa: F821
result["top_recurring_stems_by_notional"] = [r.asDict() for r in spark.sql(
    "SELECT * FROM stems WHERE events >= 20 ORDER BY notional_m DESC LIMIT 150").collect()]  # noqa: F821
result["top_single_events_by_notional"] = [r.asDict() for r in spark.sql("""
    SELECT e.event_id, e.event_slug, e.markets, round(e.notional_shares / 1e6, 2) AS notional_m
    FROM ev2 e JOIN stems s USING (stem) WHERE s.events < 20 ORDER BY e.notional_shares DESC LIMIT 40""").collect()]  # noqa: F821
result["non_recurring_events"] = spark.sql("""
    SELECT count(*) AS events, round(sum(e.notional_shares) / 1e6, 0) AS notional_m,
           count_if(e.notional_shares >= 1e4) AS events_10k_plus, count_if(e.notional_shares >= 1e5) AS events_100k_plus
    FROM ev2 e JOIN stems s USING (stem) WHERE s.events < 20""").first().asDict()  # noqa: F821

# COMMAND ----------

# Gamma pilot: is a request by several event ids honoured, how big is the reply (gzip on the
# wire and unpacked), and how big a page does the offset listing give?
top_ids = [r["event_id"] for r in spark.sql("SELECT event_id FROM ev2 ORDER BY rnk LIMIT 50").collect()]  # noqa: F821
updown = [r["event_id"] for r in spark.sql(
    "SELECT event_id FROM ev2 WHERE event_slug LIKE '%updown%' OR event_slug LIKE '%up-or-down%' ORDER BY rnk LIMIT 3").collect()]  # noqa: F821


def gamma(url: str) -> dict:
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "polymarket-economy-portfolio/1.0",
                                                   "Accept": "application/json", "Accept-Encoding": "gzip"})
        with urllib.request.urlopen(req, timeout=60) as r:
            wire = r.read()
            encoding = r.headers.get("Content-Encoding")
        raw = gzip.decompress(wire) if encoding == "gzip" else wire
        body = json.loads(raw)
        return {"items": len(body), "wire_bytes": len(wire), "unpacked_bytes": len(raw), "encoding": encoding,
                "seconds": round(time.time() - t0, 2), "body": body}
    except Exception as e:  # report what happened; the fetch design depends on it
        return {"error": f"{type(e).__name__}: {e}"[:400], "seconds": round(time.time() - t0, 2)}


def brief(e: dict) -> dict:
    return {"id": e.get("id"), "slug": e.get("slug"), "category": e.get("category"),
            "series": [{k: x.get(k) for k in ("id", "slug", "recurrence")} for x in e.get("series") or []],
            "tags": [x.get("slug") for x in e.get("tags") or []], "markets": len(e.get("markets") or [])}


pilot = {}
by_id = gamma("https://gamma-api.polymarket.com/events?limit=100&" + "&".join(f"id={i}" for i in top_ids + updown))
if "body" in by_id:
    body = by_id.pop("body")
    by_id.update(requested=len(top_ids + updown), returned_ids_match=len({e["id"] for e in body} & set(top_ids + updown)),
                 with_tags=sum(1 for e in body if e.get("tags")), sample=[brief(e) for e in body])
pilot["by_id"] = by_id
for limit in (500, 1000):
    page = gamma(f"https://gamma-api.polymarket.com/events?limit={limit}&offset=0&order=id&ascending=true")
    if "body" in page:
        body = page.pop("body")
        page.update(first_id=body[0].get("id") if body else None, last_id=body[-1].get("id") if body else None,
                    with_tags=sum(1 for e in body if e.get("tags")))
    pilot[f"page_limit_{limit}"] = page
result["gamma_pilot"] = pilot

result.update(timings=timings, problems=problems)
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
