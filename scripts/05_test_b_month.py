"""Test B on one bronze month: duplicates and block gaps; optionally the time-based checks.

    python scripts/05_test_b_month.py trades 2024 12
    python scripts/05_test_b_month.py trades 2024 12 --with-time

Local only: reads the month's bronze part files, downloads nothing.
Saves the report to D:\\polymarket-economy-data\\eda\\B_<table>_<YYYY-MM>[_time].txt

The default run uses no timestamps. The dataset's `timestamp` column is wrong by up to
16 hours in places (brief section 9, concern 11), so the --with-time sections describe the
dataset's own clock and must not be read as true UTC days.
"""
import argparse
import sys
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")

TIME_INDEPENDENT = {
    "Schema": "DESCRIBE SELECT * FROM t",
    "Rows, nulls and ranges": """
        SELECT count(*) AS n_rows,
               count(*) - count(transaction_hash) AS null_tx_hash,
               count(*) - count(log_index) AS null_log_index,
               count(*) - count(block_number) AS null_block,
               count(*) - count(usd_amount) AS null_usd,
               count(*) FILTER (usd_amount = 0) AS usd_zero,
               count(*) FILTER (usd_amount < 0) AS usd_negative,
               min(block_number) AS min_block, max(block_number) AS max_block,
               round(sum(usd_amount) / 1e9, 4) AS usd_bn
        FROM t""",
    "Uniqueness of (transaction_hash, log_index)": """
        WITH k AS (SELECT transaction_hash, log_index, count(*) AS n FROM t GROUP BY ALL)
        SELECT sum(n) AS n_rows, count(*) AS distinct_keys,
               count(*) FILTER (n > 1) AS keys_with_duplicates,
               coalesce(sum(n - 1) FILTER (n > 1), 0) AS surplus_rows,
               max(n) AS max_rows_per_key
        FROM k""",
    "Blocks": """
        WITH b AS (SELECT block_number, count(*) AS fills FROM t GROUP BY 1)
        SELECT count(*) AS blocks_with_fills,
               max(block_number) - min(block_number) + 1 AS blocks_in_range,
               round(100.0 * count(*) / (max(block_number) - min(block_number) + 1), 2) AS pct_blocks_with_fills,
               round(avg(fills), 2) AS avg_fills_per_block, max(fills) AS max_fills_per_block
        FROM b""",
    "Block-gap scan: runs of consecutive blocks with no fills": """
        WITH b AS (SELECT DISTINCT block_number FROM t),
             g AS (SELECT block_number - lag(block_number) OVER (ORDER BY block_number) - 1 AS missing FROM b)
        SELECT max(missing) AS longest_run,
               count(*) FILTER (missing >= 10) AS runs_of_10_plus,
               count(*) FILTER (missing >= 100) AS runs_of_100_plus,
               count(*) FILTER (missing >= 1000) AS runs_of_1000_plus,
               sum(missing) AS total_blocks_without_fills
        FROM g""",
    "Ten longest runs of blocks with no fills": """
        WITH b AS (SELECT DISTINCT block_number FROM t),
             g AS (SELECT lag(block_number) OVER (ORDER BY block_number) AS last_block_before,
                          block_number AS first_block_after FROM b)
        SELECT last_block_before, first_block_after,
               first_block_after - last_block_before - 1 AS blocks_without_fills
        FROM g WHERE last_block_before IS NOT NULL
        ORDER BY blocks_without_fills DESC, last_block_before LIMIT 10""",
}

TRADES_ONLY = {
    "Fills and volume by exchange contract": """
        SELECT contract, count(*) AS fills, round(sum(usd_amount) / 1e6, 2) AS usd_m
        FROM t GROUP BY 1 ORDER BY fills DESC""",
}

TIME_DEPENDENT = {
    "Dataset clock: gaps between consecutive blocks that have fills (seconds)": """
        WITH b AS (SELECT block_number, min("timestamp") AS ts, count(DISTINCT "timestamp") AS n_ts
                   FROM t GROUP BY 1),
             g AS (SELECT n_ts, ts - lag(ts) OVER (ORDER BY block_number) AS gap_s FROM b)
        SELECT round(quantile_cont(gap_s, 0.5), 1) AS p50, round(quantile_cont(gap_s, 0.99), 1) AS p99,
               max(gap_s) AS max,
               count(*) FILTER (gap_s > 3600) AS gaps_over_1_hour,
               count(*) FILTER (gap_s < 0) AS timestamp_goes_backwards,
               count(*) FILTER (n_ts > 1) AS blocks_with_more_than_one_timestamp
        FROM g""",
    "Dataset clock: pace by 100,000-block chunk (exactly 2 s per block = estimated)": """
        WITH b AS (SELECT block_number, min("timestamp") AS ts, count(*) AS fills FROM t GROUP BY 1),
             d AS (SELECT *, ts - lag(ts) OVER (ORDER BY block_number) AS step_s,
                          block_number - lag(block_number) OVER (ORDER BY block_number) AS step_blocks
                   FROM b)
        SELECT block_number // 100000 * 100000 AS chunk_start,
               count(*) AS blocks_with_fills, sum(fills) AS fills,
               to_timestamp(min(ts))::VARCHAR AS first_utc, to_timestamp(max(ts))::VARCHAR AS last_utc,
               round((max(ts) - min(ts)) / (max(block_number) - min(block_number)), 4) AS seconds_per_block,
               round(100.0 * count(*) FILTER (step_blocks = 1 AND step_s = 2)
                     / nullif(count(*) FILTER (step_blocks = 1), 0), 1) AS pct_adjacent_steps_exactly_2s
        FROM d GROUP BY 1 ORDER BY 1""",
    "Dataset clock: daily fills and volume": """
        SELECT to_timestamp("timestamp")::DATE AS day, count(*) AS fills,
               round(sum(usd_amount) / 1e6, 2) AS usd_m,
               count(DISTINCT block_number) AS blocks_with_fills,
               count(DISTINCT strftime(to_timestamp("timestamp"), '%H')) AS hours_with_fills
        FROM t GROUP BY 1 ORDER BY 1""",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("table", choices=["trades", "users"])
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    parser.add_argument("--with-time", action="store_true",
                        help="run only the sections that use the dataset's timestamp column")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    month_dir = DATA_ROOT / "bronze" / args.table / f"year={args.year:04d}" / f"month={args.month:02d}"
    if not month_dir.exists():
        raise SystemExit(f"{month_dir} not found. Is D: mounted? Run 04_bronze_month.py first.")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    # The source stores these as unsigned integers; cast so differences can go negative.
    con.execute(f"""
        CREATE VIEW t AS
        SELECT * REPLACE ("timestamp"::BIGINT AS "timestamp", block_number::BIGINT AS block_number)
        FROM read_parquet('{month_dir.as_posix()}/part-*.parquet', hive_partitioning = false)""")

    if args.with_time:
        reports, suffix = TIME_DEPENDENT, "_time"
    else:
        reports = TIME_INDEPENDENT | (TRADES_ONLY if args.table == "trades" else {})
        suffix = ""
    lines = [f"Test B: {args.table} {args.year:04d}-{args.month:02d}"
             + (" (dataset clock, not true UTC)" if args.with_time else " (time-independent checks)")]
    for title, sql in reports.items():
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
    report = "\n".join(lines)

    out = DATA_ROOT / "eda" / f"B_{args.table}_{args.year:04d}-{args.month:02d}{suffix}.txt"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
