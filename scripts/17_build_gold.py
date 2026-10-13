"""Build the gold layer (docs/GOLD_PLAN.md) by running the SQL files in sql\\gold on the SQL warehouse.

    python scripts/17_build_gold.py run 00_wallet_week_category
    python scripts/17_build_gold.py run 00_wallet_week_category --set target=workspace.polymarket_silver.wallet_week_category_pilot --set "month_filter=month_start = DATE '2024-12-01'"
    python scripts/17_build_gold.py run 10_dim_week 11_annotation 12_weekly_kpi
    python scripts/17_build_gold.py check 90_check_weekly_kpi      prints the rows a checking query returns

A file holds one or more statements, separated by a line containing only ";". A header line
"-- default name = value" declares a placeholder {{name}}; --set overrides it (used for pilots).
Each statement is sent through the Statement Execution API and polled until it finishes. Every
statement is logged to D:\\polymarket-economy-data\\eda\\gold_runs.jsonl with its statement id
and duration, so its cost can be attached later. After a file, the row count of each table it
created is printed and logged.
"""
import importlib.util
import json
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("test_c", HERE / "14_test_c.py")
test_c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(test_c)  # reuses its Databricks CLI wrapper

SQL_DIR = HERE.parent / "sql" / "gold"
WAREHOUSE_ID = "748ff4e9a4a1ce9b"
RUN_LOG = test_c.DATA_ROOT / "eda" / "gold_runs.jsonl"
POLL_SECONDS = 5
TIMEOUT_S = 3600


def api(method: str, path: str, payload: dict | None = None) -> dict:
    args = ["api", method, path]
    if payload is not None:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump(payload, f)
        args += ["--json", f"@{f.name}"]
    try:
        return json.loads(test_c.cli(*args, tries=4))
    finally:
        if payload is not None:
            Path(f.name).unlink()


def execute(statement: str) -> dict:
    """Run one statement to completion; return the API's final response."""
    r = api("post", "/api/2.0/sql/statements",
            {"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "30s", "on_wait_timeout": "CONTINUE"})
    started = time.time()
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - started > TIMEOUT_S:
            api("post", f"/api/2.0/sql/statements/{r['statement_id']}/cancel")
            raise SystemExit(f"Cancelled after {TIMEOUT_S} s: statement {r['statement_id']}")
        time.sleep(POLL_SECONDS)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    return r


def rows_of(r: dict) -> tuple[list[str], list[list]]:
    """Column names and all rows of a finished statement (following result chunks)."""
    columns = [c["name"] for c in r["manifest"]["schema"].get("columns", [])]
    result, rows = r.get("result", {}), []
    while True:
        rows += result.get("data_array", [])
        if result.get("next_chunk_internal_link") is None:
            return columns, rows
        result = api("get", result["next_chunk_internal_link"])


def load(name: str, overrides: dict) -> list[str]:
    text = (SQL_DIR / f"{name.removesuffix('.sql')}.sql").read_text(encoding="utf-8")
    params = dict(re.findall(r"^-- default (\w+) = (.*)$", text, flags=re.M)) | overrides
    for key, value in params.items():
        text = text.replace("{{" + key + "}}", value.strip())
    if "{{" in text:
        raise SystemExit(f"{name}: unresolved placeholder {re.findall(r'{{.*?}}', text)}")
    statements = [s.strip() for s in re.split(r"^;\s*$", text, flags=re.M)]
    return [s for s in statements if re.sub(r"--.*", "", s).strip()]


def log(record: dict) -> None:
    record["logged_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with RUN_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def run(names: list[str], overrides: dict) -> None:
    for name in names:
        tables = []
        for n, statement in enumerate(load(name, overrides), 1):
            t = time.time()
            r = execute(statement)
            state, seconds = r["status"]["state"], round(time.time() - t, 1)
            log({"file": name, "statement": n, "statement_id": r["statement_id"], "state": state, "seconds": seconds,
                 "overrides": overrides})
            print(f"{name} [{n}]: {state} in {seconds:.0f} s", flush=True)
            if state != "SUCCEEDED":
                raise SystemExit(f"FAILED: {r['status'].get('error', {}).get('message', '')[:3000]}")
            tables += re.findall(r"CREATE OR REPLACE TABLE\s+([\w.]+)", statement, flags=re.I)
        for table in tables:
            _, rows = rows_of(execute(f"SELECT count(*) FROM {table}"))
            log({"file": name, "table": table, "rows": int(rows[0][0])})
            print(f"{name}: {table} has {int(rows[0][0]):,} rows", flush=True)


def check(names: list[str], overrides: dict) -> None:
    for name in names:
        out = []
        for statement in load(name, overrides):
            r = execute(statement)
            if r["status"]["state"] != "SUCCEEDED":
                raise SystemExit(f"FAILED: {r['status'].get('error', {}).get('message', '')[:3000]}")
            columns, rows = rows_of(r)
            print(" | ".join(columns))
            for row in rows:
                print(" | ".join("" if v is None else str(v) for v in row))
            out.append({"columns": columns, "rows": rows})
        path = test_c.DATA_ROOT / "eda" / f"gold_{name.removesuffix('.sql')}.json"
        path.write_text(json.dumps(out, indent=1), encoding="utf-8")
        print(f"saved to {path}", flush=True)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    args = sys.argv[1:]
    overrides = {}
    while "--set" in args:
        i = args.index("--set")
        key, value = args[i + 1].split("=", 1)
        overrides[key] = value
        del args[i:i + 2]
    if len(args) < 2 or args[0] not in ("run", "check"):
        raise SystemExit(__doc__)
    if not test_c.MONTH_BOUNDS.exists():
        raise SystemExit(f"{test_c.MONTH_BOUNDS} not found. Is D: mounted?")
    (run if args[0] == "run" else check)(args[1:], overrides)


if __name__ == "__main__":
    main()
