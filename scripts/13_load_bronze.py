"""Load bronze in Databricks by direct extraction, one month at a time, oldest first.

    python scripts/13_load_bronze.py 2022-11 2025-12
    python scripts/13_load_bronze.py 2026-01 2026-01 --tables trades

For each month and table it submits scripts/dbx_bronze_month.py (already imported into the
workspace) as a one-time serverless job through the Databricks CLI, waits, and checks the
result. No data passes through the laptop. Runs are strictly sequential because the notebook
rewrites the Volume manifest whole.

Stops at the first run that fails or whose count check fails (this is also what a compute
quota cut-off looks like).

Resumable: months already in the Volume manifest are skipped without submitting a job. A month
folder that has files but no manifest entry is verified and registered, not extracted again.

Every run is logged to D:\\polymarket-economy-data\\eda\\dbx_bronze_runs.jsonl with its job run
id, so DBU cost can be attached later from system.billing.usage.
"""
import argparse
import csv
import json
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_ROOT = Path(r"D:\polymarket-economy-data")
MONTH_BOUNDS = DATA_ROOT / "lookup" / "month_bounds.csv"
RUN_LOG = DATA_ROOT / "eda" / "dbx_bronze_runs.jsonl"
NOTEBOOK = "/Users/ashwinbarman.work@gmail.com/polymarket-economy/bronze_month"
MANIFEST = "dbfs:/Volumes/workspace/polymarket/bronze/_manifest.jsonl"
POLL_SECONDS = 10
FINISHED = {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}
CLI = shutil.which("databricks")


def cli(*args: str, check: bool = True) -> str:
    out = subprocess.run([CLI, *args], capture_output=True, text=True, encoding="utf-8")
    if check and out.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args[:3])} failed: {out.stderr.strip()[:500]}")
    return out.stdout


def months_between(first: str, last: str) -> list[str]:
    y, m = map(int, first.split("-"))
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        y, m = y + m // 12, m % 12 + 1
    return out


def next_month(label: str) -> str:
    y, m = map(int, label.split("-"))
    return f"{y + m // 12:04d}-{m % 12 + 1:02d}"


def manifest_months() -> set[tuple[str, str]]:
    text = cli("fs", "cat", MANIFEST, check=False)
    done = set()
    for line in text.splitlines():
        if line.startswith("{"):
            e = json.loads(line)
            done.add((e["table"], f"{e['year']:04d}-{e['month']:02d}"))
    return done


def run_month(table: str, label: str, first_block: int, end_block: int | None) -> dict:
    year, month = label.split("-")
    payload = {
        "run_name": f"polymarket bronze: {table} {label}",
        "tasks": [{
            "task_key": "bronze_month",
            "notebook_task": {
                "notebook_path": NOTEBOOK, "source": "WORKSPACE",
                "base_parameters": {
                    "table": table, "year": year, "month": str(int(month)),
                    "first_block": str(first_block),
                    "end_block": "" if end_block is None else str(end_block),
                    # Count checks are the remote count against the files in the Volume.
                    # Spark is left out per month to save quota; 13b verifies with Spark per batch.
                    "spark_check": "false",
                    "adopt_existing": "true",
                },
            },
        }],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f)
    run_id = json.loads(cli("jobs", "submit", "--json", f"@{f.name}", "--no-wait", "-o", "json"))["run_id"]
    Path(f.name).unlink()

    while True:
        time.sleep(POLL_SECONDS)
        try:
            run = json.loads(cli("jobs", "get-run", str(run_id), "-o", "json"))
        except (RuntimeError, json.JSONDecodeError):
            continue  # transient API error; the job itself keeps running
        if run["state"]["life_cycle_state"] in FINISHED:
            break

    task = run["tasks"][0]
    record = {
        "table": table, "month": label, "job_run_id": run_id, "task_run_id": task["run_id"],
        "state": run["state"]["life_cycle_state"], "result_state": run["state"].get("result_state"),
        "state_message": run["state"].get("state_message", ""),
        "execution_ms": task.get("execution_duration"),
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    output = json.loads(cli("jobs", "get-run-output", str(task["run_id"]), "-o", "json"))
    if output.get("notebook_output", {}).get("result"):
        record["result"] = json.loads(output["notebook_output"]["result"])
    if output.get("error"):
        record["error"] = output["error"][:1000]
    with RUN_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("first_month")
    parser.add_argument("last_month")
    parser.add_argument("--tables", nargs="+", default=["trades", "users"], choices=["trades", "users"])
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    if not MONTH_BOUNDS.exists():
        raise SystemExit(f"{MONTH_BOUNDS} not found. Is D: mounted?")
    if CLI is None:
        raise SystemExit("databricks CLI not found on PATH.")
    with MONTH_BOUNDS.open(newline="", encoding="utf-8") as f:
        first = {r["month"]: int(r["first_block"]) for r in csv.DictReader(f)}

    done = manifest_months()
    todo = [(label, table) for label in months_between(args.first_month, args.last_month)
            for table in args.tables if (table, label) not in done]
    print(f"{len(todo)} month-tables to load, {len(done)} already in the Volume manifest", flush=True)

    started = time.time()
    for label, table in todo:
        if label not in first:
            raise SystemExit(f"{label} is not in {MONTH_BOUNDS}.")
        rec = run_month(table, label, first[label], first.get(next_month(label)))
        res = rec.get("result", {})
        ok = (rec["result_state"] == "SUCCESS" and not res.get("problems")
              and res.get("rows") == res.get("remote_count") and res.get("rows") is not None)
        if not ok:
            print(f"STOPPED at {table} {label}: {rec['state']}/{rec['result_state']} "
                  f"{rec['state_message']} {rec.get('error', '')} {res.get('problems', '')}", flush=True)
            raise SystemExit(1)
        how = "registered" if res.get("adopted_existing_files") else "extracted"
        print(f"{label} {table}: {how}, {res['rows']:,} rows, {res['files']} file(s), "
              f"{res['bytes'] / 1e9:.3f} GB, {rec['execution_ms'] / 1000:.0f} s", flush=True)
    print(f"Batch finished: {len(todo)} runs in {(time.time() - started) / 60:.1f} minutes", flush=True)


if __name__ == "__main__":
    main()
