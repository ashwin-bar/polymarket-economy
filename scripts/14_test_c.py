"""Test C: run scripts/dbx_test_c_wallet_identity.py in Databricks over the bronze users table.

    python scripts/14_test_c.py

Works out the first block of every UTC day in the window from the chain-time lookup
(lookup\\block_anchors.csv, linear interpolation between anchors; month starts use the exact
blocks in lookup\\month_bounds.csv), submits the notebook as a one-time serverless job with those
bounds as a parameter, waits, and saves the daily aggregates to
D:\\polymarket-economy-data\\eda\\C_wallet_identity_daily.csv. No data passes through the laptop
except that one-row-per-day result.
"""
import bisect
import csv
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

DATA_ROOT = Path(r"D:\polymarket-economy-data")
ANCHORS = DATA_ROOT / "lookup" / "block_anchors.csv"
MONTH_BOUNDS = DATA_ROOT / "lookup" / "month_bounds.csv"
OUT_CSV = DATA_ROOT / "eda" / "C_wallet_identity_daily.csv"
OUT_JSON = DATA_ROOT / "eda" / "C_wallet_identity_run.json"
START, LAST = date(2026, 1, 1), date(2026, 10, 5)  # LAST is the first day after the snapshot ends
OPEN_END = 10**12
POLL_SECONDS = 15
FINISHED = {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}
CLI = shutil.which("databricks")


def workspace_dir() -> str:
    """Workspace folder the notebooks are imported into, e.g. /Users/<you>/polymarket-economy.
    Read from the POLYMARKET_WORKSPACE_DIR environment variable so no user path is in the repo."""
    path = os.environ.get("POLYMARKET_WORKSPACE_DIR", "").rstrip("/")
    if not path.startswith("/"):
        raise SystemExit("Set POLYMARKET_WORKSPACE_DIR to the workspace folder holding the notebooks, "
                         "e.g. /Users/<you>/polymarket-economy")
    return path


def cli(*args: str, tries: int = 1) -> str:
    for attempt in range(1, tries + 1):
        out = subprocess.run([CLI, *args], capture_output=True, text=True, encoding="utf-8")
        if out.returncode == 0:
            return out.stdout
        if attempt < tries:
            time.sleep(5 * attempt)
    raise RuntimeError(f"databricks {' '.join(args[:3])} failed: {out.stderr.strip()[:500]}")


def day_first_blocks() -> list[int]:
    with ANCHORS.open(newline="", encoding="utf-8") as f:
        anchors = [(int(r["block_number"]), int(r["timestamp"])) for r in csv.DictReader(f)]
    times = [t for _, t in anchors]
    with MONTH_BOUNDS.open(newline="", encoding="utf-8") as f:
        exact = {r["month"]: int(r["first_block"]) for r in csv.DictReader(f)}

    blocks, worst = [], 0
    day = START
    while day <= LAST:
        midnight = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
        i = bisect.bisect_left(times, midnight)
        (b0, t0), (b1, t1) = anchors[i - 1], anchors[i]
        estimate = b0 + math.ceil((midnight - t0) * (b1 - b0) / (t1 - t0))  # first block at or after midnight
        if day.day == 1:
            worst = max(worst, abs(estimate - exact[f"{day:%Y-%m}"]))
            estimate = exact[f"{day:%Y-%m}"]
        blocks.append(estimate)
        day += timedelta(days=1)
    print(f"{len(blocks)} day bounds, {START} to {LAST}; interpolated month starts are within "
          f"{worst} blocks of the exact ones", flush=True)
    return blocks + [OPEN_END]


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    if not ANCHORS.exists():
        raise SystemExit(f"{ANCHORS} not found. Is D: mounted?")
    if CLI is None:
        raise SystemExit("databricks CLI not found on PATH.")

    payload = {
        "run_name": "polymarket test C: wallet identity",
        "idempotency_token": f"pm-test-c-{int(time.time())}",
        "tasks": [{
            "task_key": "test_c",
            "notebook_task": {
                "notebook_path": f"{workspace_dir()}/test_c_wallet_identity", "source": "WORKSPACE",
                "base_parameters": {
                    "start_date": START.isoformat(),
                    "day_first_blocks": ",".join(map(str, day_first_blocks())),
                },
            },
        }],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f)
    run_id = json.loads(cli("jobs", "submit", "--json", f"@{f.name}", "--no-wait", "-o", "json",
                            tries=6))["run_id"]
    Path(f.name).unlink()
    print(f"job run {run_id} submitted", flush=True)

    while True:
        time.sleep(POLL_SECONDS)
        try:
            run = json.loads(cli("jobs", "get-run", str(run_id), "-o", "json"))
        except (RuntimeError, json.JSONDecodeError):
            continue  # transient API error; the job itself keeps running
        if run["state"]["life_cycle_state"] in FINISHED:
            break

    task = run["tasks"][0]
    output = json.loads(cli("jobs", "get-run-output", str(task["run_id"]), "-o", "json", tries=6))
    print(f"{run['state']['life_cycle_state']}/{run['state'].get('result_state')} in "
          f"{(task.get('execution_duration') or 0) / 1000:.0f} s", flush=True)
    if run["state"].get("result_state") != "SUCCESS":
        raise SystemExit(f"FAILED: {run['state'].get('state_message', '')} {output.get('error', '')[:2000]}")

    result = json.loads(output["notebook_output"]["result"])
    daily = result.pop("daily")
    result.update(job_run_id=run_id, task_run_id=task["run_id"], execution_ms=task.get("execution_duration"))
    OUT_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")
    columns = ["date"] + [c for c in daily[0] if c != "date"]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(daily)
    print(json.dumps(result, indent=2))
    print(f"{len(daily)} daily rows written to {OUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
