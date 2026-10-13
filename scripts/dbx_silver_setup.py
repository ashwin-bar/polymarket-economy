# Databricks notebook source
# Silver step 1-2 (docs/SILVER_PLAN.md): schemas, small sources, and the two time tables.
#
#   - schemas workspace.polymarket_silver and workspace.polymarket_gold
#   - markets.parquet copied byte-for-byte from Hugging Face main into the bronze Volume,
#     checked against the SHA-256 that Hugging Face publishes (brief §4.1)
#   - polymarket_silver.block_anchor from lookup/block_anchors.csv in the Volume
#   - polymarket_silver.dim_date: one row per UTC day with its block range
#   - whether the Polymarket Gamma API is reachable from serverless (for the taxonomy)
#
# Safe to re-run: the markets file is skipped if present with the right checksum, and the
# tables are replaced whole.
#
# Parameter: month_first_blocks = "YYYY-MM:block,..." exact first block of each UTC month
# (from the laptop's lookup\month_bounds.csv, found by bisection on real block times).

# COMMAND ----------

import hashlib
import json
import os
import shutil
import tempfile
import time
import urllib.request

dbutils.widgets.text("month_first_blocks", "")  # noqa: F821 (dbutils and spark are provided by Databricks)
months = [(m, int(b)) for m, b in (x.split(":") for x in dbutils.widgets.get("month_first_blocks").split(","))]  # noqa: F821

VOLUME = "/Volumes/workspace/polymarket/bronze"
SILVER = "workspace.polymarket_silver"
MARKETS_URL = "https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data/resolve/main/markets.parquet"
MARKETS_SHA256 = "ce153f7ffa523911dc46d7a8cbcf324f4fd7a39e1bca0b6d396ee572595ad35f"
MARKETS_PATH = f"{VOLUME}/markets/markets.parquet"
FIRST_DAY, LAST_DAY = "2022-11-21", "2026-10-04"
result, problems = {}, []

spark.conf.set("spark.sql.session.timeZone", "UTC")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")  # noqa: F821
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.polymarket_gold")  # noqa: F821

# COMMAND ----------


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


t = time.time()
if os.path.exists(MARKETS_PATH) and sha256_of(MARKETS_PATH) == MARKETS_SHA256:
    result["markets"] = {"skipped": True}
else:
    local = os.path.join(tempfile.mkdtemp(), "markets.parquet")
    urllib.request.urlretrieve(MARKETS_URL, local)
    digest = sha256_of(local)
    if digest != MARKETS_SHA256:
        raise RuntimeError(f"markets.parquet SHA-256 is {digest}, expected {MARKETS_SHA256}")
    os.makedirs(os.path.dirname(MARKETS_PATH), exist_ok=True)
    shutil.copyfile(local, MARKETS_PATH)
    result["markets"] = {"skipped": False}
markets_rows = spark.read.parquet(MARKETS_PATH).count()  # noqa: F821
result["markets"].update(bytes=os.path.getsize(MARKETS_PATH), sha256_ok=True, rows=markets_rows,
                         seconds=round(time.time() - t, 1))
if markets_rows != 4_049_900:
    problems.append(f"markets rows {markets_rows} != 4,049,900")

# COMMAND ----------

# Block anchors: one row per 1,000 blocks, with the next anchor's time alongside so a fill's
# time is one equi-join and a linear interpolation.
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.block_anchor AS
    SELECT CAST(block_number AS BIGINT) AS anchor_block, CAST(timestamp AS BIGINT) AS ts_unix,
           lead(CAST(timestamp AS BIGINT)) OVER (ORDER BY CAST(block_number AS BIGINT)) AS next_ts_unix
    FROM read_files('{VOLUME}/lookup/block_anchors.csv', format => 'csv', header => true)""")  # noqa: F821
a = spark.sql(f"""
    SELECT count(*) AS n, min(anchor_block) AS first_anchor, max(anchor_block) AS last_anchor,
           count_if(anchor_block % 1000 != 0) AS off_grid,
           count_if(next_ts_unix <= ts_unix) AS not_increasing,
           count_if(next_ts_unix IS NULL) AS without_next
    FROM {SILVER}.block_anchor""").first().asDict()  # noqa: F821
result["block_anchor"] = a
if a["n"] != 59_074 or a["off_grid"] or a["not_increasing"] or a["without_next"] != 1 \
        or (a["last_anchor"] - a["first_anchor"]) // 1000 + 1 != a["n"]:
    problems.append(f"block_anchor checks failed: {a}")

# COMMAND ----------

# One row per UTC day. A day's first block is the first block whose interpolated time is at or
# after midnight; month starts use the exact blocks passed in, so month boundaries agree with
# the bronze partitions.
spark.createDataFrame(months, "month string, first_block long").createOrReplaceTempView("month_bounds")  # noqa: F821
spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER}.dim_date AS
    WITH days AS (
        SELECT explode(sequence(DATE '{FIRST_DAY}', DATE '{LAST_DAY}')) AS date_utc),
    est AS (
        SELECT d.date_utc,
               a.anchor_block + CAST(ceil((unix_timestamp(d.date_utc) - a.ts_unix) * 1000.0
                                          / (a.next_ts_unix - a.ts_unix)) AS BIGINT) AS est_block
        FROM days d LEFT JOIN {SILVER}.block_anchor a
          ON unix_timestamp(d.date_utc) >= a.ts_unix AND unix_timestamp(d.date_utc) < a.next_ts_unix),
    blocks AS (
        SELECT e.date_utc,
               CASE WHEN e.date_utc = DATE '{FIRST_DAY}' OR day(e.date_utc) = 1 THEN m.first_block
                    ELSE e.est_block END AS first_block,
               e.est_block, m.first_block AS exact_block
        FROM est e LEFT JOIN month_bounds m ON m.month = date_format(e.date_utc, 'yyyy-MM'))
    SELECT date_utc,
           CAST(date_trunc('week', date_utc) AS DATE) AS week_start,
           CAST(date_trunc('month', date_utc) AS DATE) AS month_start,
           first_block,
           lead(first_block) OVER (ORDER BY date_utc) AS end_block,
           date_format(date_utc, 'yyyy-MM') IN ('2022-11', '2026-10') AS is_partial_month,
           date_utc >= DATE '2026-04-28' AS is_v2,
           date_utc IN (DATE '2026-04-29', DATE '2026-04-30') AS is_known_gap,
           CASE WHEN date_utc BETWEEN DATE '2026-06-11' AND DATE '2026-07-19' THEN 'world_cup_2026'
                WHEN date_utc BETWEEN DATE '2024-09-28' AND DATE '2024-11-05' THEN 'us_election_2024'
                WHEN date_utc BETWEEN DATE '2026-01-01' AND DATE '2026-03-31' THEN 'benchmark_q1_2026'
           END AS event_window,
           CASE WHEN day(date_utc) = 1 THEN est_block - exact_block END AS month_start_estimate_minus_exact
    FROM blocks""")  # noqa: F821
d = spark.sql(f"""
    SELECT count(*) AS days, count_if(first_block IS NULL) AS no_first_block,
           count_if(end_block <= first_block) AS not_increasing, count_if(end_block IS NULL) AS open_ended,
           max(abs(month_start_estimate_minus_exact)) AS worst_month_start_estimate,
           count_if(event_window = 'world_cup_2026') AS world_cup_days,
           count_if(event_window = 'us_election_2024') AS election_days,
           min(first_block) AS first_block, max(first_block) AS last_day_first_block
    FROM {SILVER}.dim_date""").first().asDict()  # noqa: F821
result["dim_date"] = d
if d["days"] != 1414 or d["no_first_block"] or d["not_increasing"] or d["open_ended"] != 1 \
        or d["world_cup_days"] != 39 or d["election_days"] != 39:
    problems.append(f"dim_date checks failed: {d}")

# COMMAND ----------

# Is the Gamma API reachable from serverless? Report exactly what happens, and the shape of
# what comes back, so the taxonomy is built on the real tag vocabulary.
gamma = {}
for name, url in (("events", "https://gamma-api.polymarket.com/events?limit=2&closed=true&order=volume&ascending=false"),
                  ("tags", "https://gamma-api.polymarket.com/tags?limit=5")):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "polymarket-economy-portfolio/1.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            body = json.loads(r.read())
        gamma[name] = {"status": r.status, "items": len(body), "keys": sorted(body[0].keys()) if body else []}
        if name == "events" and body:
            gamma[name]["sample"] = [{k: e.get(k) for k in ("id", "slug", "category", "subcategory", "volume")}
                                     | {"tags": [{k: tg.get(k) for k in ("id", "label", "slug")} for tg in e.get("tags") or []]}
                                     for e in body]
        if name == "tags":
            gamma[name]["sample"] = body
    except Exception as e:  # report, do not fail: the fallback is to fetch on the laptop
        gamma[name] = {"error": f"{type(e).__name__}: {e}"[:500]}
result["gamma"] = gamma

result["problems"] = problems
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED: " + json.dumps(problems, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
