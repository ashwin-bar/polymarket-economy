"""Attribute the HTTP byte ranges of one bronze extraction to source row groups.

    python scripts/07_http_attribution.py users 2024 12

Answers: did DuckDB fetch only the row groups the month needs, did it fetch anything twice,
and how much of the transfer is row groups that straddle the true month boundary?

Inputs (all under D:\\polymarket-economy-data):
    eda\\http_<table>_<YYYY-MM>.csv        HTTP log written by 04_bronze_month.py
    eda\\A_column_sizes_<table>.parquet    dataset month of each row group (03_column_sizing.py)
    lookup\\month_bounds.csv               first block of each true UTC month (06_block_times.py)
    eda\\A_rowgroup_offsets_<table>.parquet  byte span and block range of each source row group;
                                            read from the remote footer (a few MB) if not on disk
"""
import argparse
import csv
import sys
from pathlib import Path

import duckdb

REPO = "SII-WANGZJ/Polymarket_data"
DATA_ROOT = Path(r"D:\polymarket-economy-data")
EDA = DATA_ROOT / "eda"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("table", choices=["trades", "users"])
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    table = args.table
    label = f"{args.year:04d}-{args.month:02d}"
    nxt = f"{args.year + args.month // 12:04d}-{args.month % 12 + 1:02d}"

    log = EDA / f"http_{table}_{label}.csv"
    sizes = EDA / f"A_column_sizes_{table}.parquet"
    offsets = EDA / f"A_rowgroup_offsets_{table}.parquet"
    bounds = DATA_ROOT / "lookup" / "month_bounds.csv"
    for needed in (log, sizes, bounds):
        if not needed.exists():
            raise SystemExit(f"{needed} not found. Is D: mounted?")
    with bounds.open(newline="", encoding="utf-8") as f:
        first = {r["month"]: int(r["first_block"]) for r in csv.DictReader(f)}
    first_block, end_block = first[label], first.get(nxt, 2**62)

    con = duckdb.connect()
    if not offsets.exists():
        print(f"Reading the {table} footer remotely for row-group byte offsets")
        con.execute(f"""
            COPY (
                SELECT row_group_id, any_value(row_group_num_rows) AS n_rows,
                       min(coalesce(dictionary_page_offset, data_page_offset)) AS byte_start,
                       min(coalesce(dictionary_page_offset, data_page_offset))
                           + sum(total_compressed_size) AS byte_end,
                       min(CASE WHEN path_in_schema = 'block_number'
                                THEN coalesce(stats_min_value, stats_min)::BIGINT END) AS block_min,
                       max(CASE WHEN path_in_schema = 'block_number'
                                THEN coalesce(stats_max_value, stats_max)::BIGINT END) AS block_max
                FROM parquet_metadata('hf://datasets/{REPO}@main/{table}.parquet')
                GROUP BY 1
            ) TO '{offsets.as_posix()}' (FORMAT parquet)""")

    con.execute(f"""
        CREATE VIEW rg AS
        SELECT o.*, s.month AS dataset_month, o.byte_end - o.byte_start AS rg_bytes,
               o.block_max >= {first_block} AND o.block_min < {end_block} AS needed,
               o.block_min >= {first_block} AND o.block_max < {end_block} AS fully_inside
        FROM read_parquet('{offsets.as_posix()}') o
        JOIN (SELECT DISTINCT row_group_id, month FROM read_parquet('{sizes.as_posix()}')) s
          USING (row_group_id)""")
    con.execute(f"""
        CREATE VIEW req AS
        SELECT * FROM read_csv('{log.as_posix()}') WHERE method = 'GET' AND phase = 'copy'""")
    con.execute("""
        CREATE VIEW hit AS
        SELECT q.range_start, q.bytes_received, g.row_group_id, g.dataset_month, g.needed,
               g.fully_inside, g.rg_bytes
        FROM req q LEFT JOIN rg g ON q.range_start >= g.byte_start AND q.range_start < g.byte_end""")

    reports = {
        f"Source row groups whose block range overlaps {label} (footer statistics)": """
            SELECT dataset_month, count(*) AS row_groups,
                   count(*) FILTER (fully_inside) AS fully_inside,
                   count(*) FILTER (NOT fully_inside) AS straddling_the_boundary,
                   sum(n_rows) AS n_rows, round(sum(rg_bytes) / 1e9, 3) AS gb
            FROM rg WHERE needed GROUP BY 1 ORDER BY 1""",
        "Extraction requests (DuckDB HTTP log, copy pass)": """
            SELECT count(*) AS requests, round(sum(bytes_received) / 1e9, 3) AS gb_received,
                   count(*) - count(DISTINCT range_start) AS repeated_range_starts
            FROM req""",
        "Bytes received, by the row group each range falls in": """
            WITH per_rg AS (
                SELECT row_group_id, any_value(dataset_month) AS dataset_month,
                       any_value(needed) AS needed, any_value(fully_inside) AS fully_inside,
                       any_value(rg_bytes) AS rg_bytes, count(*) AS requests,
                       sum(bytes_received) AS received
                FROM hit GROUP BY 1)
            SELECT coalesce(dataset_month, 'footer / not in a row group') AS dataset_month,
                   CASE WHEN row_group_id IS NULL THEN ''
                        WHEN NOT needed THEN 'not needed'
                        WHEN fully_inside THEN 'fully inside the month'
                        ELSE 'straddles the month boundary' END AS row_group_kind,
                   count(row_group_id) AS row_groups, sum(requests) AS requests,
                   round(sum(received) / 1e9, 3) AS gb_received,
                   round(sum(rg_bytes) / 1e9, 3) AS gb_of_those_row_groups
            FROM per_rg GROUP BY 1, 2 ORDER BY 1, 2""",
    }
    lines = [f"HTTP attribution: {table} {label}, blocks {first_block} to {end_block - 1}"]
    for title, sql in reports.items():
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
    report = "\n".join(lines)
    out = EDA / f"http_{table}_{label}_attribution.txt"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
