"""Follow-ups to test C, run in Databricks over bronze (brief §9, concerns 1 and 16).

    python scripts/15_test_c_followups.py v2        V2 completeness: trades against markets.volume
    python scripts/15_test_c_followups.py wallets   27 April veterans; wallets first seen after 4 May

Each submits a notebook (already imported into the workspace) as a one-time serverless job with
block bounds from the chain-time lookup as parameters, waits, and saves the aggregate result to
D:\\polymarket-economy-data\\eda\\C_followup_<name>.json. Only aggregates come back.
"""
import csv
import importlib.util
import json
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("test_c", HERE / "14_test_c.py")
test_c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(test_c)  # reuses its Databricks CLI wrapper and day bounds

WORKSPACE_DIR = "/Users/ashwinbarman.work@gmail.com/polymarket-economy"
OUT_DIR = test_c.DATA_ROOT / "eda"


def parameters(name: str) -> tuple[str, dict]:
    days = test_c.day_first_blocks()
    if name == "wallets":
        return "wallet_followups", {"start_date": test_c.START.isoformat(),
                                    "day_first_blocks": ",".join(map(str, days))}
    with test_c.MONTH_BOUNDS.open(newline="", encoding="utf-8") as f:
        months = [(r["month"], r["first_block"]) for r in csv.DictReader(f)]

    def first_block_of(iso: str) -> int:
        return days[(test_c.date.fromisoformat(iso) - test_c.START).days]

    return "v2_completeness", {
        "month_first_blocks": ",".join(f"{m}:{b}" for m, b in months),
        "pre_end_block": str(first_block_of("2026-04-28")),
        "post_start_block": str(first_block_of("2026-04-29")),
    }


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    if len(sys.argv) != 2 or sys.argv[1] not in ("v2", "wallets"):
        raise SystemExit(__doc__)
    if not test_c.ANCHORS.exists():
        raise SystemExit(f"{test_c.ANCHORS} not found. Is D: mounted?")
    name = sys.argv[1]
    notebook, params = parameters(name)

    payload = {
        "run_name": f"polymarket test C follow-up: {notebook}",
        "idempotency_token": f"pm-followup-{name}-{int(time.time())}",
        "tasks": [{"task_key": notebook,
                   "notebook_task": {"notebook_path": f"{WORKSPACE_DIR}/{notebook}", "source": "WORKSPACE",
                                     "base_parameters": params}}],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f)
    run_id = json.loads(test_c.cli("jobs", "submit", "--json", f"@{f.name}", "--no-wait", "-o", "json",
                                   tries=6))["run_id"]
    Path(f.name).unlink()
    print(f"job run {run_id} submitted", flush=True)

    while True:
        time.sleep(test_c.POLL_SECONDS)
        try:
            run = json.loads(test_c.cli("jobs", "get-run", str(run_id), "-o", "json"))
        except (RuntimeError, json.JSONDecodeError):
            continue  # transient API error; the job itself keeps running
        if run["state"]["life_cycle_state"] in test_c.FINISHED:
            break

    task = run["tasks"][0]
    output = json.loads(test_c.cli("jobs", "get-run-output", str(task["run_id"]), "-o", "json", tries=6))
    print(f"{run['state']['life_cycle_state']}/{run['state'].get('result_state')} in "
          f"{(task.get('execution_duration') or 0) / 1000:.0f} s", flush=True)
    if run["state"].get("result_state") != "SUCCESS":
        raise SystemExit(f"FAILED: {run['state'].get('state_message', '')} {output.get('error', '')[:3000]} "
                         f"{output.get('error_trace', '')[-3000:]}")

    result = json.loads(output["notebook_output"]["result"])
    result.update(job_run_id=run_id, task_run_id=task["run_id"], execution_ms=task.get("execution_duration"))
    out = OUT_DIR / f"C_followup_{name}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"saved to {out}", flush=True)


if __name__ == "__main__":
    main()
