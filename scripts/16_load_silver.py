"""Build the silver layer in Databricks (docs/SILVER_PLAN.md), one step at a time.

    python scripts/16_load_silver.py setup                      schemas, markets.parquet, block anchors, dim_date
    python scripts/16_load_silver.py wallet --pilot 2022,2023   dim_wallet stage 1 on those years only, nothing written
    python scripts/16_load_silver.py wallet                     dim_wallet stage 1 (ids, first and last fill, flags)
    python scripts/16_load_silver.py facts 2024-12 2024-12      trades and wallet_fills, one job per month and table
    python scripts/16_load_silver.py facts 2022-11 2026-10 --tables trades --expected-s 600
    python scripts/16_load_silver.py market-stats               per-market and per-event totals, slug profile, Gamma pilot
    python scripts/16_load_silver.py gamma --pilot 10000        Gamma event tags for the 10,000 largest target events
    python scripts/16_load_silver.py gamma                      Gamma event tags for every one-off event and a sample per series
    python scripts/16_load_silver.py dim-market [--version v1]  dim_event and dim_market with categories, and test H

The fact load is sequential, oldest month first, and stops at the first failed run or failed
data quality check. Months already in polymarket_silver.load_log are skipped by the notebook.

Each step submits a notebook (already imported into the workspace) as a one-time serverless
job, waits, and saves the notebook's result to
D:\\polymarket-economy-data\\eda\\silver_<step>.json. Every run, including failed, timed-out
and cancelled ones, is appended to eda\\dbx_silver_runs.jsonl with its job run id so DBU cost
can be attached later.

Two guards against a stalled job (one ran 51 minutes in a single task on 13 Oct 2026):
  - each job is submitted with a timeout of 3x its expected run time, at least 20 minutes;
  - while it runs, its queries are watched in the Databricks query history; if rows read, tasks
    completed and bytes written have all stood still for 10 minutes, the run is cancelled.
"""
import csv
import importlib.util
import json
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("test_c", HERE / "14_test_c.py")
test_c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(test_c)  # reuses its Databricks CLI wrapper

WORKSPACE_DIR = "/Users/ashwinbarman.work@gmail.com/polymarket-economy"
EDA = test_c.DATA_ROOT / "eda"
RUN_LOG = EDA / "dbx_silver_runs.jsonl"
MIN_TIMEOUT_S = 1200
STALL_S = 600


def month_first_blocks() -> str:
    with test_c.MONTH_BOUNDS.open(newline="", encoding="utf-8") as f:
        return ",".join(f"{r['month']}:{r['first_block']}" for r in csv.DictReader(f))


def progress(task_run_id: int) -> tuple | None:
    """What the run's Spark queries have done so far, from query history. None if unreadable."""
    try:
        history = json.loads(test_c.cli("api", "get",
                                        "/api/2.0/sql/history/queries?include_metrics=true&max_results=100"))
    except (RuntimeError, json.JSONDecodeError):
        return None
    mine = [q for q in history.get("res", [])
            if str((q.get("query_source", {}).get("job_info") or {}).get("job_task_run_id")) == str(task_run_id)]
    metrics = [q.get("metrics") or {} for q in mine]
    return (len(mine),
            sum(m.get("rows_read_count") or 0 for m in metrics),
            sum((m.get("task_progress") or {}).get("completed_task_count") or 0 for m in metrics),
            sum(m.get("write_remote_bytes") or 0 for m in metrics))


def run_notebook(notebook: str, params: dict, label: str, expected_s: int, watch: bool = True) -> dict:
    """Submit one notebook run, watch it, log it, and return its result (or stop on failure).

    watch=False is for a run that spends long stretches outside Spark (an API fetch), where the
    query history stands still by design; the timeout is then the only guard.
    """
    timeout_s = max(MIN_TIMEOUT_S, 3 * expected_s)
    payload = {
        "run_name": f"polymarket silver: {label}",
        "idempotency_token": f"pm-silver-{label.replace(' ', '-')}-{int(time.time())}",
        "timeout_seconds": timeout_s,
        "tasks": [{"task_key": notebook,
                   "notebook_task": {"notebook_path": f"{WORKSPACE_DIR}/{notebook}", "source": "WORKSPACE",
                                     "base_parameters": params}}],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f)
    run_id = json.loads(test_c.cli("jobs", "submit", "--json", f"@{f.name}", "--no-wait", "-o", "json",
                                   tries=6))["run_id"]
    Path(f.name).unlink()
    print(f"{label}: job run {run_id} submitted, timeout {timeout_s / 60:.0f} min", flush=True)

    note, last_progress, last_change = "", None, time.time()
    while True:
        time.sleep(test_c.POLL_SECONDS)
        try:
            run = json.loads(test_c.cli("jobs", "get-run", str(run_id), "-o", "json"))
        except (RuntimeError, json.JSONDecodeError):
            continue  # transient API error; the job itself keeps running
        if run["state"]["life_cycle_state"] in test_c.FINISHED:
            break
        if not watch:
            continue
        now = progress(run["tasks"][0]["run_id"])
        if now is not None and now != last_progress:
            last_progress, last_change = now, time.time()
        elif time.time() - last_change >= STALL_S and not note:
            note = (f"Cancelled by the driver: no progress for {STALL_S // 60} min "
                    f"(queries, rows read, tasks completed, bytes written = {last_progress})")
            print(f"{label}: {note}", flush=True)
            test_c.cli("jobs", "cancel-run", str(run_id), tries=6)

    task = run["tasks"][0]
    output = json.loads(test_c.cli("jobs", "get-run-output", str(task["run_id"]), "-o", "json", tries=6))
    record = {"label": label, "notebook": notebook, "job_run_id": run_id, "task_run_id": task["run_id"],
              "result_state": run["state"].get("result_state"), "execution_ms": task.get("execution_duration"),
              "timeout_s": timeout_s, "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if note:
        record["note"] = note
    with RUN_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(f"{label}: {run['state']['life_cycle_state']}/{record['result_state']} in "
          f"{(record['execution_ms'] or 0) / 1000:.0f} s", flush=True)
    if record["result_state"] != "SUCCESS":
        raise SystemExit(f"FAILED: {run['state'].get('state_message', '')} {note} {output.get('error', '')[:3000]} "
                         f"{output.get('error_trace', '')[-3000:]}")
    result = json.loads(output["notebook_output"]["result"])
    result.update(job_run_id=run_id, task_run_id=task["run_id"], execution_ms=record["execution_ms"])
    return result


def save(result: dict, name: str) -> None:
    out = EDA / f"{name}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"saved to {out}", flush=True)


def option(args: list[str], flag: str) -> str | None:
    """Pop `flag value` from args."""
    if flag not in args:
        return None
    i = args.index(flag)
    value = args[i + 1]
    del args[i:i + 2]
    return value


def setup() -> None:
    save(run_notebook("silver_setup", {"month_first_blocks": month_first_blocks()}, "setup", expected_s=120),
         "silver_setup")


def wallet() -> None:
    pilot = option(sys.argv, "--pilot")
    if pilot:
        save(run_notebook("silver_dim_wallet", {"pilot_years": pilot}, f"dim_wallet pilot {pilot}", expected_s=120),
             "silver_dim_wallet_pilot")
    else:
        save(run_notebook("silver_dim_wallet", {"pilot_years": ""}, "dim_wallet stage 1", expected_s=400),
             "silver_dim_wallet")


def facts() -> None:
    args = sys.argv[2:]
    expected_s = int(option(args, "--expected-s") or 400)
    tables = ["trades", "wallet_fills"]
    if "--tables" in args:
        i = args.index("--tables")
        tables, args = args[i + 1:], args[:i]
    if len(args) != 2 or not set(tables) <= {"trades", "wallet_fills"}:
        raise SystemExit(__doc__)
    first, last = args
    y, m = map(int, first.split("-"))
    out = EDA / "silver_fact_runs.jsonl"
    started = time.time()
    while f"{y:04d}-{m:02d}" <= last:
        for table in tables:  # trades first, so wallet_fills can check its maker rows against it
            label = f"{table} {y:04d}-{m:02d}"
            r = run_notebook("silver_fact_month", {"table": table, "year": str(y), "month": str(m)}, label,
                             expected_s=expected_s)
            with out.open("a", encoding="utf-8") as f:
                f.write(json.dumps(r, default=str) + "\n")
            if r.get("skipped"):
                print(f"{label}: already loaded", flush=True)
                continue
            t = r["timings"]
            print(f"{label}: {r['rows']:,} rows; write {t['write_s']:.0f} s, bronze checks {t['bronze_checks_s']:.0f} s, "
                  f"silver checks {t['silver_checks_s']:.0f} s; table now {r['table_bytes_all_months'] / 1e9:.2f} GB", flush=True)
        y, m = y + m // 12, m % 12 + 1
    print(f"Finished in {(time.time() - started) / 60:.1f} minutes", flush=True)


def market_stats() -> None:
    save(run_notebook("silver_market_stats", {}, "market stats", expected_s=400), "silver_market_stats")


def gamma() -> None:
    pilot = option(sys.argv, "--pilot")
    if pilot:
        save(run_notebook("silver_gamma_events", {"limit_events": pilot}, f"gamma events pilot {pilot}", expected_s=200,
                          watch=False),
             "silver_gamma_events_pilot")
    else:
        save(run_notebook("silver_gamma_events", {"limit_events": ""}, "gamma events", expected_s=1500, watch=False),
             "silver_gamma_events")


def dim_market() -> None:
    # The two taxonomy files in config\ are copied next to the notebooks so the job reads the
    # same versions that are in the repo.
    test_c.cli("workspace", "mkdirs", f"{WORKSPACE_DIR}/config")
    for name in ("tag_category_map.csv", "slug_category_rules.csv"):
        test_c.cli("workspace", "import", f"{WORKSPACE_DIR}/config/{name}", "--file", str(HERE.parent / "config" / name),
                   "--format", "AUTO", "--overwrite")
    version = option(sys.argv, "--version") or "v1"
    save(run_notebook("silver_dim_market", {"taxonomy_version": version}, f"dim_market {version}", expected_s=400),
         "silver_dim_market")


STEPS = {"setup": setup, "wallet": wallet, "facts": facts, "market-stats": market_stats, "gamma": gamma,
         "dim-market": dim_market}


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    if len(sys.argv) < 2 or sys.argv[1] not in STEPS:
        raise SystemExit(__doc__)
    if not test_c.MONTH_BOUNDS.exists():
        raise SystemExit(f"{test_c.MONTH_BOUNDS} not found. Is D: mounted?")
    STEPS[sys.argv[1]]()


if __name__ == "__main__":
    main()
