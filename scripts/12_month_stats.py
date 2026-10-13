"""Content statistics for one bronze month on the laptop, as JSON.

    python scripts/12_month_stats.py trades 2024 12

The same figures as scripts/dbx_month_stats.py computes in Databricks, so the two copies of a
month can be compared: row count, distinct keys, USD and token sums, block range, and exact
integer checksums that do not depend on row order or floating-point summation order.
"""
import argparse
import json
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("table", choices=["trades", "users"])
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    args = parser.parse_args()
    month_dir = DATA_ROOT / "bronze" / args.table / f"year={args.year:04d}" / f"month={args.month:02d}"
    if not month_dir.exists():
        raise SystemExit(f"{month_dir} not found. Is D: mounted?")

    rel = duckdb.sql(f"""
        SELECT count(*) AS n_rows,
               count(DISTINCT (transaction_hash, log_index)) AS distinct_keys,
               count(DISTINCT transaction_hash) AS distinct_transactions,
               sum(usd_amount) AS sum_usd_amount,
               sum(token_amount) AS sum_token_amount,
               min(block_number)::BIGINT AS min_block,
               max(block_number)::BIGINT AS max_block,
               sum(block_number::HUGEINT) AS sum_block_number,
               sum(log_index::HUGEINT) AS sum_log_index,
               sum(round(usd_amount * 1e6)::HUGEINT) AS sum_usd_micro,
               sum(round(token_amount * 1e6)::HUGEINT) AS sum_token_micro
        FROM read_parquet('{month_dir.as_posix()}/part-*.parquet', hive_partitioning = false)""")
    row = dict(zip(rel.columns, rel.fetchone()))
    result = {k: (repr(float(v)) if k in ("sum_usd_amount", "sum_token_amount") else int(v))
              for k, v in row.items()}
    print(json.dumps(result))


if __name__ == "__main__":
    main()
