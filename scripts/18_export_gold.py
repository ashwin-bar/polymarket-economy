"""Export every gold table to CSV for Tableau Public Edition (docs/GOLD_PLAN.md §8).

    python scripts/18_export_gold.py                 all tables in workspace.polymarket_gold
    python scripts/18_export_gold.py weekly_kpi      only the tables named

Each table is read through the SQL warehouse and written to
D:\\polymarket-economy-data\\gold_export\\<table>.csv (UTF-8, header row, ISO dates, empty cell
for null, numbers as plain decimals with no exponent and no rounding). _export_manifest.json records table, columns, rows, bytes, SHA-256 and time.

A table is refused, and nothing is written for it, if it has a column that could identify a
wallet, or more than 15,000,000 rows (the Tableau Public limit). After writing, the rows in the
file must equal the table's row count in Databricks. The files stay on D:, never in the repo.
"""
import csv
import hashlib
import http.client
import importlib.util
import json
import sys
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("build_gold", HERE / "17_build_gold.py")
gold = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gold)  # reuses its Statement Execution API helpers

SCHEMA = "workspace.polymarket_gold"
OUT_DIR = gold.test_c.DATA_ROOT / "gold_export"
MANIFEST = OUT_DIR / "_export_manifest.json"
MAX_ROWS = 15_000_000
FORBIDDEN = {"wallet_id", "address", "maker_wallet_id", "taker_wallet_id", "first_fill_ts", "first_block",
             "first_log_index", "transaction_hash"}


def finished(statement: str, **extra) -> dict:
    """Run a statement to completion with extra request fields (disposition, format)."""
    r = gold.api("post", "/api/2.0/sql/statements",
                 {"warehouse_id": gold.WAREHOUSE_ID, "statement": statement, "wait_timeout": "30s",
                  "on_wait_timeout": "CONTINUE", **extra})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        gold.time.sleep(gold.POLL_SECONDS)
        r = gold.api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"FAILED: {statement[:80]}: {r['status'].get('error', {}).get('message', '')[:1000]}")
    return r


def download(url: str) -> bytes:
    """Fetch a pre-signed result link (no token is sent). A connection that drops part-way is
    resumed from the bytes already received with a Range request, up to 40 times."""
    body, total, failures = b"", None, 0
    while True:
        request = urllib.request.Request(url, headers={"Range": f"bytes={len(body)}-"} if body else {})
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                if body and response.status != 206:
                    body = b""  # the server ignored the range: start again
                if not body:
                    total = int(response.headers["Content-Length"])
                while block := response.read(1 << 20):
                    body += block
        except http.client.IncompleteRead as e:
            body += e.partial
        except (OSError, http.client.HTTPException):
            pass
        # A dropped connection can also end the read quietly, so only the byte count decides.
        if total is not None and len(body) == total:
            return body
        if total is not None and len(body) > total:
            raise SystemExit(f"Download returned {len(body):,} bytes, expected {total:,}")
        failures += 1
        if failures >= 40:
            raise SystemExit(f"Download kept failing after {len(body):,} bytes")
        gold.time.sleep(min(2 * failures, 20))


def chunks(r: dict):
    """Yield each result chunk (a list of rows) of a statement run with external links."""
    result = r.get("result", {})
    while True:
        for link in result.get("external_links", []):
            yield json.loads(download(link["external_link"]))
        nxt = [link.get("next_chunk_internal_link") for link in result.get("external_links", [])]
        nxt = [n for n in nxt if n]
        if not nxt:
            return
        result = gold.api("get", nxt[-1])


def plain(value: str | None) -> str:
    """A double as a plain decimal string. Databricks sends the shortest text that round-trips
    to the same double, sometimes in scientific notation ("3.37E7"); Decimal re-writes those
    digits without an exponent, so nothing is rounded and the value read back is identical."""
    if value is None:
        return ""
    if value in ("NaN", "Infinity", "-Infinity"):
        raise SystemExit(f"Non-finite number in export: {value}")
    return format(Decimal(value), "f")


def export(table: str) -> dict:
    _, count_rows = gold.rows_of(gold.execute(f"SELECT count(*) FROM {SCHEMA}.{table}"))
    expected = int(count_rows[0][0])
    r = finished(f"SELECT * FROM {SCHEMA}.{table}", disposition="EXTERNAL_LINKS", format="JSON_ARRAY")
    columns = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    floating = [c["type_name"] in ("DOUBLE", "FLOAT") for c in r["manifest"]["schema"]["columns"]]
    bad = [c for c in columns if c in FORBIDDEN or "address" in c]
    if bad:
        raise SystemExit(f"{table}: refused, column(s) {bad} could identify a wallet")
    if expected > MAX_ROWS:
        raise SystemExit(f"{table}: refused, {expected:,} rows is over the Tableau Public limit of {MAX_ROWS:,}")

    path, tmp, written = OUT_DIR / f"{table}.csv", OUT_DIR / f"{table}.csv.tmp", 0
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for rows in chunks(r):
            writer.writerows([plain(v) if is_float else ("" if v is None else v) for v, is_float in zip(row, floating)]
                             for row in rows)
            written += len(rows)
    if written != expected:
        tmp.unlink()
        raise SystemExit(f"{table}: {written:,} rows written but the table has {expected:,}")
    tmp.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"table": table, "file": path.name, "columns": columns, "rows": written, "bytes": path.stat().st_size,
            "sha256": digest, "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    if not gold.test_c.MONTH_BOUNDS.exists():
        raise SystemExit(f"{gold.test_c.MONTH_BOUNDS} not found. Is D: mounted?")
    OUT_DIR.mkdir(exist_ok=True)
    _, listed = gold.rows_of(gold.execute(f"SHOW TABLES IN {SCHEMA}"))
    tables = sorted(row[1] for row in listed)
    wanted = sys.argv[1:] or tables
    unknown = sorted(set(wanted) - set(tables))
    if unknown:
        raise SystemExit(f"Not in {SCHEMA}: {unknown}")
    manifest = {e["table"]: e for e in json.loads(MANIFEST.read_text(encoding="utf-8"))} if MANIFEST.exists() else {}
    for table in wanted:
        manifest[table] = export(table)
        print(f"{table}: {manifest[table]['rows']:,} rows, {manifest[table]['bytes']:,} bytes", flush=True)
        MANIFEST.write_text(json.dumps([manifest[t] for t in sorted(manifest)], indent=1), encoding="utf-8")
    total = [manifest[t] for t in sorted(manifest)]
    print(f"{len(total)} tables, {sum(e['rows'] for e in total):,} rows, {sum(e['bytes'] for e in total):,} bytes in {OUT_DIR}")


if __name__ == "__main__":
    main()
