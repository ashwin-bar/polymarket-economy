# Databricks notebook source
# Silver market dimension, step 2 (docs/SILVER_PLAN.md §4): fetch event tags from the Polymarket
# Gamma API for the events that carry the traded volume.
#
# Which events are fetched:
#   - every traded event whose slug stem (numbers and month names blanked) has fewer than
#     RECURRING_MIN_EVENTS events: these are one-off events and need their own tags
#   - for each recurring stem (e.g. btc-updown-#m-#, about 100,000 events), only its
#     SAMPLE_PER_STEM largest events: the series takes its category from that sample
#
# Events are requested by id, 100 per request, largest notional first, and kept slim (id, slug,
# title, tags, series, a few flags; not the embedded markets or descriptions) as JSON lines in
# /Volumes/workspace/polymarket/bronze/gamma_events/chunk-NNNN.jsonl. A chunk already in the
# manifest is skipped, so the notebook can be re-run. It stops before the bytes received over
# the wire (all runs together) would pass max_wire_mb.
#
# The extract lives only in the bronze Volume; nothing is written to silver. The summary at the
# end reads it back: coverage, and the tag vocabulary ranked by the notional of its events.
# Needs polymarket_silver.event_stats, a staging table that dbx_silver_market_stats.py writes
# and that is dropped once the market dimension is built.
#
# Parameters: limit_events (pilot: only the first N target events; empty = all), max_wire_mb

# COMMAND ----------

import gzip
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

dbutils.widgets.text("limit_events", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
dbutils.widgets.text("max_wire_mb", "950")  # noqa: F821
limit_events = int(dbutils.widgets.get("limit_events") or 0)  # noqa: F821
max_wire_bytes = int(dbutils.widgets.get("max_wire_mb")) * 1_000_000  # noqa: F821

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
SILVER = "workspace.polymarket_silver"
OUT_DIR = "/Volumes/workspace/polymarket/bronze/gamma_events"
MANIFEST = f"{OUT_DIR}/_manifest.json"
RECURRING_MIN_EVENTS, SAMPLE_PER_STEM = 20, 5
CHUNK, BATCH, THREADS = 5000, 100, 4
result, problems = {}, []

# COMMAND ----------

spark.sql(f"""
    SELECT stem, count(*) AS events, sum(markets) AS markets, sum(fills) AS fills,
           sum(notional_shares) AS notional_shares, min(event_slug) AS example_slug,
           min(first_fill_ts) AS first_fill_ts, count(*) >= {RECURRING_MIN_EVENTS} AS is_recurring
    FROM {SILVER}.event_stats GROUP BY stem""").createOrReplaceTempView("series_stem")  # noqa: F821
spark.sql(f"""
    SELECT e.event_id, e.rnk, e.notional_shares, s.is_recurring,
           row_number() OVER (PARTITION BY e.stem ORDER BY e.rnk) AS rank_in_stem
    FROM {SILVER}.event_stats e JOIN series_stem s USING (stem)""").createOrReplaceTempView("ranked")  # noqa: F821
targets = [r["event_id"] for r in spark.sql(f"""
    SELECT event_id FROM ranked WHERE NOT is_recurring OR rank_in_stem <= {SAMPLE_PER_STEM}
    ORDER BY rnk""").collect()]  # noqa: F821
result["targets"] = spark.sql(f"""
    SELECT count(*) AS traded_events, count_if(NOT is_recurring) AS one_off_events,
           count_if(is_recurring AND rank_in_stem <= {SAMPLE_PER_STEM}) AS series_sample_events,
           round(sum(CASE WHEN NOT is_recurring THEN notional_shares END) / sum(notional_shares), 4) AS one_off_notional_share
    FROM ranked""").first().asDict()  # noqa: F821
if limit_events:
    targets = targets[:limit_events]

# COMMAND ----------


def fetch(ids: list[str]) -> tuple[list[dict], int, int]:
    """One request for up to BATCH event ids. Returns slim records, wire bytes, unpacked bytes."""
    url = f"https://gamma-api.polymarket.com/events?limit={BATCH}&" + "&".join(f"id={i}" for i in ids)
    req = urllib.request.Request(url, headers={"User-Agent": "polymarket-economy-portfolio/1.0",
                                               "Accept": "application/json", "Accept-Encoding": "gzip"})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                wire = r.read()
                raw = gzip.decompress(wire) if r.headers.get("Content-Encoding") == "gzip" else wire
            body = json.loads(raw)
            break
        except Exception:
            if attempt == 5:
                raise
            time.sleep(2 ** attempt)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    slim = [{"id": str(e.get("id")), "missing": False, "slug": e.get("slug"), "title": e.get("title"),
             "category": e.get("category"), "subcategory": e.get("subcategory"), "closed": e.get("closed"),
             "startDate": e.get("startDate"), "endDate": e.get("endDate"), "volume": e.get("volume"),
             "n_markets": len(e.get("markets") or []),
             "tags": [{k: (str(x[k]) if x.get(k) is not None else None) for k in ("id", "slug", "label")}
                      for x in e.get("tags") or []],
             "series": [{k: (str(x[k]) if x.get(k) is not None else None) for k in ("id", "slug", "title", "recurrence")}
                        for x in e.get("series") or []],
             "fetched_at": now} for e in body]
    got = {s["id"] for s in slim}
    slim += [{"id": i, "missing": True, "fetched_at": now} for i in ids if i not in got]
    return slim, len(wire), len(raw)


os.makedirs(OUT_DIR, exist_ok=True)
manifest = json.load(open(MANIFEST)) if os.path.exists(MANIFEST) else {}
wire_total = sum(c["wire_bytes"] for c in manifest.values())
stopped_for_size = False
started = time.time()
for n, start in enumerate(range(0, len(targets), CHUNK)):
    name = f"chunk-{n:04d}"
    ids = targets[start:start + CHUNK]
    if name in manifest and manifest[name]["requested"] == len(ids):
        continue
    # A chunk is assumed no larger than the largest one so far (the first chunk holds the biggest events).
    worst = max([c["wire_bytes"] for c in manifest.values()], default=60_000_000)
    if wire_total + worst > max_wire_bytes:
        stopped_for_size = True
        break
    t = time.time()
    batches = [ids[i:i + BATCH] for i in range(0, len(ids), BATCH)]
    with ThreadPoolExecutor(THREADS) as pool:
        out = list(pool.map(fetch, batches))
    records = [rec for slim, _, _ in out for rec in slim]
    with open(f"{OUT_DIR}/{name}.jsonl", "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(rec) for rec in records) + "\n")
    manifest[name] = {"requested": len(ids), "returned": sum(1 for rec in records if not rec["missing"]),
                      "requests": len(batches), "wire_bytes": sum(w for _, w, _ in out),
                      "unpacked_bytes": sum(u for _, _, u in out), "seconds": round(time.time() - t, 1)}
    wire_total += manifest[name]["wire_bytes"]
    with open(MANIFEST, "w", encoding="utf-8") as f:  # files in a Volume cannot be appended to
        json.dump(manifest, f)
    print(name, manifest[name], f"total {wire_total / 1e6:.1f} MB")
result["fetch"] = {"target_events_this_run": len(targets), "chunks_done": len(manifest),
                   "requested": sum(c["requested"] for c in manifest.values()),
                   "returned": sum(c["returned"] for c in manifest.values()),
                   "requests": sum(c["requests"] for c in manifest.values()),
                   "wire_mb_all_runs": round(wire_total / 1e6, 1),
                   "unpacked_mb_all_runs": round(sum(c["unpacked_bytes"] for c in manifest.values()) / 1e6, 1),
                   "seconds_this_run": round(time.time() - started, 1), "stopped_for_size": stopped_for_size,
                   "first_chunks": {k: manifest[k] for k in sorted(manifest)[:3]}}

# COMMAND ----------

SCHEMA = ("id string, missing boolean, slug string, title string, category string, subcategory string, closed boolean, "
          "startDate string, endDate string, volume double, n_markets int, "
          "tags array<struct<id:string,slug:string,label:string>>, "
          "series array<struct<id:string,slug:string,title:string,recurrence:string>>, fetched_at string")
(spark.read.schema(SCHEMA).json(f"{OUT_DIR}/chunk-*.jsonl")  # noqa: F821
 .withColumnRenamed("id", "event_id").createOrReplaceTempView("gamma_event"))

g = spark.sql(f"""
    SELECT count(*) AS n, count(DISTINCT g.event_id) AS distinct_events, count_if(g.missing) AS missing_in_gamma,
           count_if(NOT g.missing AND size(g.tags) = 0) AS returned_without_tags,
           count_if(g.category IS NOT NULL) AS with_category_field,
           count_if(size(g.series) > 0) AS with_series,
           count_if(g.slug != e.event_slug) AS slug_differs_from_markets_file,
           round(sum(e.notional_shares) / (SELECT sum(notional_shares) FROM {SILVER}.event_stats), 4) AS notional_share_fetched,
           round(sum(CASE WHEN NOT g.missing AND size(g.tags) > 0 THEN e.notional_shares END)
                 / (SELECT sum(notional_shares) FROM {SILVER}.event_stats), 4) AS notional_share_with_own_tags,
           round(sum(CASE WHEN g.missing THEN e.notional_shares END) / 1e6, 2) AS missing_notional_m
    FROM gamma_event g LEFT JOIN {SILVER}.event_stats e USING (event_id)""").first().asDict()  # noqa: F821
result["gamma_event"] = g
if g["n"] != g["distinct_events"]:
    problems.append(f"gamma_event has duplicate events: {g['n']} rows, {g['distinct_events']} events")

result["series_recurrence"] = [r.asDict() for r in spark.sql(f"""
    SELECT s.recurrence, count(*) AS events FROM (SELECT explode(series) AS s FROM gamma_event)
    GROUP BY 1 ORDER BY 2 DESC""").collect()]  # noqa: F821
# The tag vocabulary: one-off events only, so a recurring series' sample does not understate it.
result["top_tags"] = [r.asDict() for r in spark.sql(f"""
    SELECT t.slug AS tag_slug, any_value(t.label) AS label, count(*) AS events,
           round(sum(e.notional_shares) / 1e6, 1) AS notional_m
    FROM (SELECT event_id, explode(tags) AS t FROM gamma_event) g
    JOIN {SILVER}.event_stats e USING (event_id)
    GROUP BY t.slug ORDER BY sum(e.notional_shares) DESC LIMIT 500""").collect()]  # noqa: F821
result["distinct_tags"] = spark.sql(f"""
    SELECT count(DISTINCT t.slug) AS n FROM (SELECT explode(tags) AS t FROM gamma_event)""").first()["n"]  # noqa: F821
result["top_events_without_tags"] = [r.asDict() for r in spark.sql(f"""
    SELECT e.event_id, e.event_slug, g.missing, round(e.notional_shares / 1e6, 2) AS notional_m
    FROM gamma_event g JOIN {SILVER}.event_stats e USING (event_id)
    WHERE g.missing OR size(g.tags) = 0 ORDER BY e.notional_shares DESC LIMIT 30""").collect()]  # noqa: F821

result["problems"] = problems
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
