"""Write one month of one table to bronze.

    python scripts/04_bronze_month.py trades 2024 12

Reads hf://datasets/SII-WANGZJ/Polymarket_data@main/<table>.parquet, filtered to one UTC
calendar month (row-group skipping, test A), all columns, and writes
    <data-root>\\bronze\\<table>\\year=YYYY\\month=MM\\part-NN.parquet
A month larger than --target-gb is split into equal-row part files.

Resumable: a month already in bronze\\_manifest.jsonl is skipped. The month is written to a
staging folder and only renamed into place after its row count matches the source footer.
A staging folder left by an interrupted run is removed and the month is started again.
"""
import argparse
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
SIZES_DIR = DEFAULT_DATA_ROOT / "eda"  # A_column_sizes_<table>.parquet from 03_column_sizing.py
LIMIT_BYTES = 3_000_000_000


def month_bounds(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + month // 12, month % 12 + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def expected_from_footer(con, table: str, year: int, month: int) -> tuple[int, int]:
    sizes = SIZES_DIR / f"A_column_sizes_{table}.parquet"
    if not sizes.exists():
        raise SystemExit(f"{sizes} not found. Run 03_column_sizing.py first.")
    rows, source_bytes = con.execute(f"""
        SELECT coalesce(sum(row_group_num_rows) FILTER (column_name = 'timestamp'), 0),
               coalesce(sum(total_compressed_size), 0)
        FROM read_parquet('{sizes.as_posix()}') WHERE month = '{year:04d}-{month:02d}'""").fetchone()
    return int(rows), int(source_bytes)


def source_commit() -> str:
    with urllib.request.urlopen(f"https://huggingface.co/api/datasets/{REPO}/revision/main") as resp:
        return json.load(resp)["sha"]


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

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET preserve_insertion_order = false")  # lower memory; order is not needed in bronze

    expected_rows, source_bytes = expected_from_footer(con, table, year, month)
    if expected_rows == 0:
        raise SystemExit(f"No rows for {table} {year:04d}-{month:02d} in the source footer.")
    source = args.source or f"hf://datasets/{REPO}@main/{table}.parquet"
    lo, hi = month_bounds(year, month)
    target_bytes = int(args.target_gb * 1e9)
    print(f"{table} {year:04d}-{month:02d}: expecting {expected_rows:,} rows, "
          f"{source_bytes / 1e9:.3f} GB compressed in the source")
    print(f"Source: {source}")

    if staging.exists():
        print(f"Removing staging folder from an interrupted run: {staging}")
        shutil.rmtree(staging)
    staging.parent.mkdir(parents=True, exist_ok=True)

    staging.mkdir()

    # Step 1: one remote pass into a single local file (no size limit applies locally).
    # DuckDB's FILE_SIZE_BYTES option was tried and rejected: it measures uncompressed
    # bytes (a 0.15 GB setting gave 0.05 GB files) and leaves an empty trailing file.
    started = time.time()
    whole = staging / "month.parquet"
    write_options = "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1048576)"
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{source}')
              WHERE "timestamp" >= {lo} AND "timestamp" < {hi})
        TO '{whole.as_posix()}' {write_options}""")

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

    rows, ts_min, ts_max = con.execute(f"""
        SELECT count(*), min("timestamp"), max("timestamp")
        FROM read_parquet('{staging.as_posix()}/part-*.parquet')""").fetchone()
    sizes = [p.stat().st_size for p in parts]
    problems = []
    if rows != expected_rows:
        problems.append(f"row count {rows:,} != expected {expected_rows:,}")
    if not (lo <= ts_min and ts_max < hi):
        problems.append(f"timestamps {ts_min}..{ts_max} fall outside the month")
    if max(sizes) > LIMIT_BYTES:
        problems.append(f"largest part is {max(sizes):,} bytes, over the 3 GB limit")
    if problems:
        raise SystemExit("FAILED, staging folder kept at " + str(staging) + ": " + "; ".join(problems))

    staging.rename(final_dir)
    entry = {
        "table": table, "year": year, "month": month,
        "rows": rows, "files": len(parts), "bytes": sum(sizes),
        "source_bytes": source_bytes, "source_commit": source_commit(),
        "ts_min": ts_min, "ts_max": ts_max,
        "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

    print(f"Wrote {len(parts)} file(s) to {final_dir} in {seconds:,.0f} s")
    for p, size in zip(parts, sizes):
        print(f"  {p.name}: {size:,} bytes")
    print(f"Rows: {rows:,} (match: True)")
    print(f"Timestamps (UTC): {datetime.fromtimestamp(ts_min, timezone.utc)} to "
          f"{datetime.fromtimestamp(ts_max, timezone.utc)}")
    print(f"Output / source compressed size: {sum(sizes) / source_bytes:.3f}")


if __name__ == "__main__":
    main()
