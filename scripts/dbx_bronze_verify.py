# Databricks notebook source
# Batch check with Spark: rows per month in the bronze Volume against the manifest.
# One Spark pass per table (footers only), instead of a Spark read in every monthly run.

# COMMAND ----------

import json
import os

VOLUME = "/Volumes/workspace/polymarket/bronze"
with open(f"{VOLUME}/_manifest.jsonl", encoding="utf-8") as f:
    manifest = [json.loads(line) for line in f.read().splitlines() if line]

result = {"tables": {}, "mismatches": []}
for table in ("trades", "users"):
    expected = {(e["year"], e["month"]): e["rows"] for e in manifest if e["table"] == table}
    if not os.path.exists(f"{VOLUME}/{table}"):
        continue
    counts = spark.read.parquet(f"{VOLUME}/{table}").groupBy("year", "month").count().collect()  # noqa: F821
    actual = {(int(r["year"]), int(r["month"])): r["count"] for r in counts}
    for key in sorted(set(expected) | set(actual)):
        if expected.get(key) != actual.get(key):
            result["mismatches"].append({"table": table, "year": key[0], "month": key[1],
                                         "manifest_rows": expected.get(key), "spark_rows": actual.get(key)})
    result["tables"][table] = {
        "months_in_manifest": len(expected), "months_in_volume": len(actual),
        "manifest_rows": sum(expected.values()), "spark_rows": sum(actual.values()),
        "manifest_bytes": sum(e["bytes"] for e in manifest if e["table"] == table),
        "files": sum(e["files"] for e in manifest if e["table"] == table),
        "first_month": min(actual) if actual else None, "last_month": max(actual) if actual else None,
    }
result["ok"] = not result["mismatches"]
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821 (provided by Databricks)
