"""Write one true UTC month of one table to bronze.

    python scripts/04_bronze_month.py trades 2024 12

Reads hf://datasets/SII-WANGZJ/Polymarket_data@main/<table>.parquet, all columns, and writes
    <data-root>\\bronze\\<table>\\year=YYYY\\month=MM\\part-NN.parquet
A month larger than --target-gb is split into equal-row part files.

The month is cut by BLOCK RANGE, not by the dataset's `timestamp` column, which is wrong by
hours in places (brief section 9, concern 11). The first block of each true UTC month comes
from lookup\\month_bounds.csv, built from real Polygon block times by 06_block_times.py.

Resumable: a month already in bronze\\_manifest.jsonl is skipped. The month is written to a
staging folder and only renamed into place after its row count matches a separate remote
count. A staging folder left by an interrupted run is removed and the month is started again.
"""
import argparse
import csv
import json
import shutil
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO = "SII-WANGZJ/Polymarket_data"
DEFAULT_DATA_ROOT = Path(r"D:\polymarket-economy-data")
MONTH_BOUNDS = DEFAULT_DATA_ROOT / "lookup" / "month_bounds.csv"
LIMIT_BYTES = 3_000_000_000


def block_range(year: int, month: int) -> tuple[int, int | None]:
    """First block of the month and first block of the next month (None = open-ended)."""
    if not MONTH_BOUNDS.exists():
        raise SystemExit(f"{MONTH_BOUNDS} not found. Run 06_block_times.py (anchors, then months) first.")
    with MONTH_BOUNDS.open(newline="", encoding="utf-8") as f:
        first = {r["month"]: int(r["first_block"]) for r in csv.DictReader(f)}
    this, nxt = f"{year:04d}-{month:02d}", f"{year + month // 12:04d}-{month % 12 + 1:02d}"
    if this not in first:
        raise SystemExit(f"{this} is not in {MONTH_BOUNDS}.")
    return first[this], first.get(nxt)


def source_commit() -> str:
    with urllib.request.urlopen(f"https://huggingface.co/api/datasets/{REPO}/revision/main") as resp:
        return json.load(resp)["sha"]


def save_http_log(con, data_root: Path, table: str, year: int, month: int, count_requests: int) -> dict:
    """Save DuckDB's HTTP request log for this month and return bytes received per phase."""
    out = data_root / "eda" / f"http_{table}_{year:04d}-{month:02d}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            WITH r AS (
                SELECT "timestamp" AS logged_at, request.type AS method, response.status AS status,
                       split_part(replace(request.headers['Range'], 'bytes=', ''), '-', 1)::BIGINT AS range_start,
                       split_part(replace(request.headers['Range'], 'bytes=', ''), '-', 2)::BIGINT AS range_end,
                       request.duration_ms AS duration_ms
                FROM duckdb_logs_parsed('HTTP'))
            SELECT CASE WHEN row_number() OVER (ORDER BY logged_at) <= {count_requests}
                        THEN 'count' ELSE 'copy' END AS phase,
                   method, status, range_start, range_end,
                   -- every data request is a byte-range GET answered with 206 for exactly that range
                   coalesce(range_end - range_start + 1, 0) AS bytes_received,
                   duration_ms
            FROM r
        ) TO '{out.as_posix()}' (FORMAT csv, HEADER)""")
    rows = con.execute(f"""
        SELECT phase, count(*), coalesce(sum(bytes_received), 0),
               count(*) FILTER (status NOT IN ('OK_200', 'PartialContent_206'))
        FROM read_csv('{out.as_posix()}') GROUP BY 1""").fetchall()
    stats = {phase: {"requests": n, "bytes": int(b), "non_ok": bad} for phase, n, b, bad in rows}
    for phase in ("count", "copy"):
        s = stats.get(phase, {"requests": 0, "bytes": 0, "non_ok": 0})
        print(f"HTTP, {phase} pass: {s['requests']:,} requests, {s['bytes'] / 1e9:.3f} GB received, "
              f"{s['non_ok']} non-OK responses")
    print(f"HTTP log saved to {out}")
    return stats


def manifest_has(manifest: Path, table: str, year: int, month: int) -> bool:
    if not manifest.exists():
        return False
    for line in manifest.read_text(encoding="utf-8").splitlines():
        entry = json.loads(line)
        if (entry["table"], entry["year"], entry["month"]) == (table, year, month):
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("table", choices=["trades", "users"])
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    parser.add_argument("--target-gb", type=float, default=2.5)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--source", default=None, help="override the source (for local tests)")
    args = parser.parse_args()
    table, year, month = args.table, args.year, args.month
    sys.stdout.reconfigure(encoding="utf-8")

    if not args.data_root.exists():
        raise SystemExit(f"{args.data_root} not found. Is D: mounted?")
    bronze = args.data_root / "bronze"
    manifest = bronze / "_manifest.jsonl"
    final_dir = bronze / table / f"year={year:04d}" / f"month={month:02d}"
    staging = final_dir.with_name(final_dir.name + ".tmp")

    if manifest_has(manifest, table, year, month):
        print(f"{table} {year:04d}-{month:02d} is in the manifest, skipping.")
        return
    if final_dir.exists():
        raise SystemExit(f"{final_dir} exists but is not in the manifest. Inspect it before re-running.")

    first_block, end_block = block_range(year, month)
    where = f"block_number >= {first_block}" + (f" AND block_number < {end_block}" if end_block else "")
    source = args.source or f"hf://datasets/{REPO}@main/{table}.parquet"
    target_bytes = int(args.target_gb * 1e9)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET preserve_insertion_order = false")  # lower memory; order is not needed in bronze

    print(f"{table} {year:04d}-{month:02d}: blocks {first_block:,} to "
          f"{f'{end_block - 1:,}' if end_block else 'end of file'}")
    print(f"Source: {source}")
    con.execute("CALL enable_logging('HTTP')")  # DuckDB's own record of every HTTP request it makes

    # Independent row count: a separate remote pass over block_number only.
    expected_rows = con.execute(f"SELECT count(*) FROM read_parquet('{source}') WHERE {where}").fetchone()[0]
    print(f"Remote count: {expected_rows:,} rows")
    count_requests = con.execute("SELECT count(*) FROM duckdb_logs_parsed('HTTP')").fetchone()[0]
    if expected_rows == 0:
        raise SystemExit("No rows in that block range.")

    if staging.exists():
        print(f"Removing staging folder from an interrupted run: {staging}")
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    # Step 1: one remote pass into a single local file (no size limit applies locally).
    # DuckDB's FILE_SIZE_BYTES option was tried and rejected: it measures uncompressed
    # bytes (a 0.15 GB setting gave 0.05 GB files) and leaves an empty trailing file.
    started = time.time()
    whole = staging / "month.parquet"
    write_options = "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1048576)"
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{source}') WHERE {where})
        TO '{whole.as_posix()}' {write_options}""")
    http = save_http_log(con, args.data_root, table, year, month, count_requests)

    # Step 2: if it is over the target, split it locally into equal row ranges.
    n_parts = max(1, -(-whole.stat().st_size // target_bytes))
    if n_parts == 1:
        parts = [whole.rename(staging / "part-00.parquet")]
    else:
        total = con.execute(f"SELECT count(*) FROM read_parquet('{whole.as_posix()}')").fetchone()[0]
        parts = []
        for k in range(n_parts):
            first, last = total * k // n_parts, total * (k + 1) // n_parts
            part = staging / f"part-{k:02d}.parquet"
            con.execute(f"""
                COPY (SELECT * EXCLUDE (file_row_number)
                      FROM read_parquet('{whole.as_posix()}', file_row_number = true)
                      WHERE file_row_number >= {first} AND file_row_number < {last})
                TO '{part.as_posix()}' {write_options}""")
            parts.append(part)
        whole.unlink()
    seconds = time.time() - started

    rows, block_min, block_max = con.execute(f"""
        SELECT count(*), min(block_number), max(block_number)
        FROM read_parquet('{staging.as_posix()}/part-*.parquet')""").fetchone()
    sizes = [p.stat().st_size for p in parts]
    problems = []
    if rows != expected_rows:
        problems.append(f"row count {rows:,} != remote count {expected_rows:,}")
    if block_min < first_block or (end_block and block_max >= end_block):
        problems.append(f"blocks {block_min}..{block_max} fall outside the month's range")
    if max(sizes) > LIMIT_BYTES:
        problems.append(f"largest part is {max(sizes):,} bytes, over the 3 GB limit")
    if problems:
        raise SystemExit("FAILED, staging folder kept at " + str(staging) + ": " + "; ".join(problems))

    staging.rename(final_dir)
    entry = {
        "table": table, "year": year, "month": month, "cut": "chain_utc_month",
        "first_block": first_block, "end_block": end_block,
        "block_min": block_min, "block_max": block_max,
        "rows": rows, "files": len(parts), "bytes": sum(sizes),
        "http_bytes_received": sum(s["bytes"] for s in http.values()),
        "source_commit": source_commit(),
        "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

    print(f"Wrote {len(parts)} file(s) to {final_dir} in {seconds:,.0f} s")
    for p, size in zip(parts, sizes):
        print(f"  {p.name}: {size:,} bytes")
    print(f"Rows: {rows:,} (matches remote count)")
    print(f"Blocks in file: {block_min:,} to {block_max:,}")


if __name__ == "__main__":
    main()
