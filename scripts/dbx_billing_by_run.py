# Databricks notebook source
# Cost of each job run, from system.billing.usage (which lags by some hours).
# Parameter: days = how many days back to look.

# COMMAND ----------

import json

dbutils.widgets.text("days", "3")  # noqa: F821 (dbutils and spark are provided by Databricks)
days = int(dbutils.widgets.get("days"))  # noqa: F821

by_run = spark.sql(f"""
    SELECT usage_metadata.job_run_id AS job_run_id,
           any_value(usage_metadata.job_name) AS job_name,
           sku_name, usage_unit,
           min(usage_start_time) AS first_usage, max(usage_end_time) AS last_usage,
           round(sum(usage_quantity), 5) AS quantity
    FROM system.billing.usage
    WHERE usage_date >= current_date() - {days} AND usage_metadata.job_run_id IS NOT NULL
    GROUP BY ALL ORDER BY first_usage""").collect()  # noqa: F821
by_sku = spark.sql(f"""
    SELECT usage_date, sku_name, usage_unit, round(sum(usage_quantity), 5) AS quantity,
           max(usage_end_time) AS latest_usage_recorded
    FROM system.billing.usage WHERE usage_date >= current_date() - {days}
    GROUP BY ALL ORDER BY usage_date, sku_name""").collect()  # noqa: F821

result = {"by_run": [r.asDict() for r in by_run], "by_sku": [r.asDict() for r in by_sku],
          "now_utc": str(spark.sql("SELECT current_timestamp()").first()[0])}  # noqa: F821
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))  # noqa: F821
