# Databricks notebook source
# Bronze ingestion, direct path (brief decision 20): extract one true UTC month of one table
# straight from Hugging Face into the Unity Catalog Volume.
#
#   <volume>/<table>/year=YYYY/month=MM/part-NN.parquet     all source columns, each under 3 GB
#   <volume>/_manifest.jsonl                                one line per completed month
#
# DuckDB (pip-installed in the session) reads hf://...@main/<table>.parquet filtered by block
# range into one file on the session's local disk. If that file is over target_gb it is split
# into equal-row parts. Parts are copied into the Volume, then checked against a separate
# remote row count.
#
# Parameters (job base_parameters / notebook widgets):
#   table, year, month       which bronze month
#   first_block, end_block   block range of the true UTC month, end exclusive ('' = open-ended),
#                            from the laptop's lookup\month_bounds.csv
#   target_gb                planning size per part file (limit is 3 GB)
#   spark_check              'true' to also read the month back with Spark
#   adopt_existing           'true': if the month folder has files but is not in the manifest,
#                            verify those files and register them instead of extracting again
#   clear_incomplete         'true': remove such a folder and extract again
#
# Resumable: a month already in the manifest is skipped.

# COMMAND ----------

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

DEFAULTS = {"table": "trades", "year": "", "month": "", "first_block": "", "end_block": "",
            "target_gb": "2.5", "spark_check": "true", "adopt_existing": "false",
            "clear_incomplete": "false"}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)  # noqa: F821 (dbutils and spark are provided by Databricks)
p = {name: dbutils.widgets.get(name) for name in DEFAULTS}  # noqa: F821
table, year, month = p["table"], int(p["year"]), int(p["month"])
first_block = int(p["first_block"])
end_block = int(p["end_block"]) if p["end_block"] else None
target_bytes = int(float(p["target_gb"]) * 1e9)
LIMIT_BYTES = 3_000_000_000

REPO = "SII-WANGZJ/Polymarket_data"
VOLUME = "/Volumes/workspace/polymarket/bronze"
MANIFEST = f"{VOLUME}/_manifest.jsonl"
source = f"hf://datasets/{REPO}@main/{table}.parquet"
month_dir = f"{VOLUME}/{table}/year={year:04d}/month={month:02d}"
where = f"block_number >= {first_block}" + (f" AND block_number < {end_block}" if end_block else "")


def read_manifest() -> list[dict]:
    if not os.path.exists(MANIFEST):
        return []
    with open(MANIFEST, encoding="utf-8") as f:
        return [json.loads(line) for line in f.read().splitlines() if line]


def add_to_manifest(entry: dict) -> None:
    """Volumes do not support appending to a file (OSError 29), so rewrite the whole manifest."""
    lines = [json.dumps(e, default=str) for e in read_manifest() + [entry]]
    tmp = os.path.join(tempfile.mkdtemp(), "_manifest.jsonl")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    shutil.copyfile(tmp, MANIFEST)


if any((e["table"], e["year"], e["month"]) == (table, year, month) for e in read_manifest()):
    dbutils.notebook.exit(json.dumps({"skipped": True, "reason": "month is in the manifest"}))  # noqa: F821

adopt = False
if os.path.exists(month_dir) and os.listdir(month_dir):
    if p["adopt_existing"] == "true":
        adopt = True
    elif p["clear_incomplete"] == "true":
        shutil.rmtree(month_dir)
    else:
        raise RuntimeError(f"{month_dir} has files but is not in the manifest. "
                           "Re-run with adopt_existing=true or clear_incomplete=true.")

# COMMAND ----------

timings = {}
t = time.time()
subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "duckdb"], check=True)
import duckdb  # noqa: E402

timings["pip_install_s"] = round(time.time() - t, 1)

local_dir = tempfile.mkdtemp()
con = duckdb.connect()
con.execute("SET preserve_insertion_order = false")
con.execute(f"SET temp_directory = '{local_dir}/duckdb_tmp'")
con.execute("CALL enable_logging('HTTP')")

t = time.time()
remote_count = con.execute(f"SELECT count(*) FROM read_parquet('{source}') WHERE {where}").fetchone()[0]
timings["remote_count_s"] = round(time.time() - t, 1)
if remote_count == 0:
    raise RuntimeError("No rows in that block range.")

# COMMAND ----------

extras = {}
if adopt:
    parts = [{"name": f, "bytes": os.path.getsize(f"{month_dir}/{f}")} for f in sorted(os.listdir(month_dir))]
else:
    disk_free_start = shutil.disk_usage(local_dir).free
    write_options = "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1048576)"
    whole = os.path.join(local_dir, "month.parquet")
    t = time.time()
    con.execute(f"COPY (SELECT * FROM read_parquet('{source}') WHERE {where}) TO '{whole}' {write_options}")
    timings["extract_s"] = round(time.time() - t, 1)
    whole_bytes = os.path.getsize(whole)
    http_requests, http_bytes = con.execute("""
        SELECT count(*),
               coalesce(sum(split_part(replace(request.headers['Range'], 'bytes=', ''), '-', 2)::BIGINT
                          - split_part(replace(request.headers['Range'], 'bytes=', ''), '-', 1)::BIGINT + 1), 0)
        FROM duckdb_logs_parsed('HTTP') WHERE request.type = 'GET'""").fetchone()

    # Split into equal-row parts if needed, copying each part into the Volume as it is produced
    # so local disk never holds more than the whole month plus one part.
    os.makedirs(month_dir, exist_ok=True)
    n_parts = max(1, -(-whole_bytes // target_bytes))
    total_rows = con.execute(f"SELECT count(*) FROM read_parquet('{whole}')").fetchone()[0]
    parts, split_s, copy_s = [], 0.0, 0.0
    for k in range(n_parts):
        name = f"part-{k:02d}.parquet"
        if n_parts == 1:
            local_part = whole
        else:
            local_part = os.path.join(local_dir, name)
            lo, hi = total_rows * k // n_parts, total_rows * (k + 1) // n_parts
            t = time.time()
            con.execute(f"""
                COPY (SELECT * EXCLUDE (file_row_number)
                      FROM read_parquet('{whole}', file_row_number = true)
                      WHERE file_row_number >= {lo} AND file_row_number < {hi})
                TO '{local_part}' {write_options}""")
            split_s += time.time() - t
        t = time.time()
        # Plain file copy through the /Volumes mount (dbutils.fs.cp cannot read local disk on serverless).
        shutil.copyfile(local_part, f"{month_dir}/{name}")
        copy_s += time.time() - t
        parts.append({"name": name, "bytes": os.path.getsize(local_part)})
        if n_parts > 1:
            os.remove(local_part)
    timings["split_s"], timings["copy_to_volume_s"] = round(split_s, 1), round(copy_s, 1)
    extras = {
        "http_get_requests": http_requests, "http_bytes_received": int(http_bytes),
        "local_disk_free_gb_start": round(disk_free_start / 1e9, 1),
        "local_disk_free_gb_at_end_of_split": round(shutil.disk_usage(local_dir).free / 1e9, 1),
    }
shutil.rmtree(local_dir, ignore_errors=True)

# COMMAND ----------

# Verify what is in the Volume.
t = time.time()
vol_rows, vol_min, vol_max = con.execute(f"""
    SELECT count(*), min(block_number)::BIGINT, max(block_number)::BIGINT
    FROM read_parquet('{month_dir}/part-*.parquet', hive_partitioning = false)""").fetchone()
timings["duckdb_verify_s"] = round(time.time() - t, 1)
volume_files = sorted((f, os.path.getsize(f"{month_dir}/{f}")) for f in os.listdir(month_dir))

spark_rows = None
if p["spark_check"] == "true":
    t = time.time()
    spark_rows = spark.read.parquet(month_dir).count()  # noqa: F821
    timings["spark_count_s"] = round(time.time() - t, 1)

problems = []
if vol_rows != remote_count:
    problems.append(f"Volume rows {vol_rows} != remote count {remote_count}")
if spark_rows is not None and spark_rows != remote_count:
    problems.append(f"Spark rows {spark_rows} != remote count {remote_count}")
if vol_min < first_block or (end_block and vol_max >= end_block):
    problems.append(f"blocks {vol_min}..{vol_max} outside the month's range")
if volume_files != [(x["name"], x["bytes"]) for x in parts]:
    problems.append(f"files in the Volume {volume_files} do not match the parts written {parts}")
if [x["name"] for x in parts] != [f"part-{k:02d}.parquet" for k in range(len(parts))]:
    problems.append(f"unexpected file names: {[x['name'] for x in parts]}")
if max(x["bytes"] for x in parts) > LIMIT_BYTES:
    problems.append("a part is over the 3 GB limit")

result = {
    "table": table, "year": year, "month": month, "cut": "chain_utc_month",
    "first_block": first_block, "end_block": end_block,
    "block_min": vol_min, "block_max": vol_max,
    "rows": vol_rows, "remote_count": remote_count, "spark_rows": spark_rows,
    "files": len(parts), "bytes": sum(x["bytes"] for x in parts), "parts": parts,
    "adopted_existing_files": adopt, **extras,
    "timings": timings,
    "cpu_count": os.cpu_count(),
    "ram_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9, 1),
    "duckdb_version": duckdb.__version__,
    "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "problems": problems,
}
print(json.dumps(result, indent=2, default=str))
if problems:
    raise RuntimeError("FAILED, month folder left in place and not added to the manifest: "
                       + json.dumps(problems))

add_to_manifest({k: v for k, v in result.items() if k != "problems"})
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
