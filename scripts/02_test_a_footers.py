"""Test A: read Parquet footers remotely for trades and users (no data pages).

Step 1 measures each footer with an 8-byte HTTP Range request.
Step 2 reads row-group statistics for `timestamp` and `block_number` through DuckDB
and saves them to D:\\polymarket-economy-data\\eda\\A_rowgroup_stats_<table>.parquet.
Step 3 reports whether the row groups are ordered by time.
Resumable: a table whose statistics file is already on disk is not re-read.
"""
import struct
import sys
import urllib.request
from pathlib import Path

import duckdb

REPO = "SII-WANGZJ/Polymarket_data"
TABLES = ["trades", "users"]
STAT_COLUMNS = ("timestamp", "block_number")
MAX_FOOTER_BYTES = 1_000_000_000  # project rule: ask before any remote read over 1 GB
OUT_DIR = Path(r"D:\polymarket-economy-data") / "eda"


def footer_bytes(table: str) -> int:
    """A Parquet file ends with a 4-byte little-endian footer length, then b'PAR1'."""
    url = f"https://huggingface.co/datasets/{REPO}/resolve/main/{table}.parquet"
    req = urllib.request.Request(url, headers={"Range": "bytes=-8"})
    with urllib.request.urlopen(req) as resp:
        tail = resp.read()
    if tail[4:] != b"PAR1":
        raise SystemExit(f"{table}: unexpected file tail {tail!r}")
    return struct.unpack("<I", tail[:4])[0]


def fetch_stats(con: duckdb.DuckDBPyConnection, table: str, out: Path) -> None:
    cols = ", ".join(f"'{c}'" for c in STAT_COLUMNS)
    tmp = out.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    con.execute(f"""
        COPY (
            SELECT row_group_id, row_group_num_rows, row_group_bytes,
                   path_in_schema AS column_name, type,
                   -- pyarrow writes the current min_value / max_value fields; the
                   -- deprecated min / max fields are empty for these columns
                   coalesce(stats_min_value, stats_min) AS stats_min,
                   coalesce(stats_max_value, stats_max) AS stats_max,
                   stats_null_count, total_compressed_size, total_uncompressed_size
            FROM parquet_metadata('hf://datasets/{REPO}@main/{table}.parquet')
            WHERE path_in_schema IN ({cols})
        ) TO '{tmp.as_posix()}' (FORMAT parquet)""")
    tmp.replace(out)


REPORTS = {
    "Row groups and rows": """
        SELECT count(*) AS row_groups, sum(row_group_num_rows) AS total_rows,
               min(row_group_num_rows) AS min_rows, max(row_group_num_rows) AS max_rows,
               count(*) FILTER (stats_min IS NULL OR stats_max IS NULL) AS groups_without_stats
        FROM s WHERE column_name = 'timestamp'""",
    "Range per column": """
        SELECT column_name, any_value(type) AS type,
               min(stats_min::BIGINT) AS overall_min, max(stats_max::BIGINT) AS overall_max,
               CASE WHEN column_name = 'timestamp'
                    THEN to_timestamp(min(stats_min::BIGINT))::VARCHAR END AS min_utc,
               CASE WHEN column_name = 'timestamp'
                    THEN to_timestamp(max(stats_max::BIGINT))::VARCHAR END AS max_utc
        FROM s GROUP BY 1 ORDER BY 1""",
    "Ordering across row groups (in file order)": """
        WITH o AS (
            SELECT column_name, row_group_id, stats_min::BIGINT AS lo, stats_max::BIGINT AS hi,
                   max(stats_max::BIGINT) OVER (PARTITION BY column_name ORDER BY row_group_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
            FROM s)
        SELECT column_name,
               count(*) AS row_groups,
               count(*) FILTER (lo < prev_max) AS groups_starting_before_earlier_max,
               round(100.0 * count(*) FILTER (lo < prev_max) / count(*), 2) AS pct_out_of_order
        FROM o GROUP BY 1 ORDER BY 1""",
    "Width of each row group's timestamp range (days)": """
        SELECT round(quantile_cont((stats_max::BIGINT - stats_min::BIGINT) / 86400.0, 0.5), 3) AS p50,
               round(quantile_cont((stats_max::BIGINT - stats_min::BIGINT) / 86400.0, 0.9), 3) AS p90,
               round(quantile_cont((stats_max::BIGINT - stats_min::BIGINT) / 86400.0, 0.99), 3) AS p99,
               round(max((stats_max::BIGINT - stats_min::BIGINT) / 86400.0), 3) AS max
        FROM s WHERE column_name = 'timestamp'""",
    "Monthly filter cost: row groups whose timestamp range overlaps each month": """
        WITH g AS (SELECT row_group_id, row_group_num_rows AS n,
                          to_timestamp(stats_min::BIGINT) AS lo, to_timestamp(stats_max::BIGINT) AS hi
                   FROM s WHERE column_name = 'timestamp'),
             months AS (SELECT unnest(generate_series(
                            date_trunc('month', (SELECT min(lo) FROM g)),
                            date_trunc('month', (SELECT max(hi) FROM g)),
                            INTERVAL 1 MONTH)) AS m)
        SELECT strftime(m, '%Y-%m') AS month,
               count(*) AS groups_to_read,
               round(100.0 * sum(n) / (SELECT sum(n) FROM g), 2) AS pct_of_file_rows,
               count(*) FILTER (lo >= m AND hi < m + INTERVAL 1 MONTH) AS groups_fully_inside
        FROM months JOIN g ON g.lo < m + INTERVAL 1 MONTH AND g.hi >= m
        GROUP BY m ORDER BY m""",
    "Widest 10 row groups by timestamp range": """
        SELECT row_group_id, row_group_num_rows,
               to_timestamp(stats_min::BIGINT)::VARCHAR AS min_utc,
               to_timestamp(stats_max::BIGINT)::VARCHAR AS max_utc,
               round((stats_max::BIGINT - stats_min::BIGINT) / 86400.0, 1) AS days
        FROM s WHERE column_name = 'timestamp' ORDER BY days DESC LIMIT 10""",
}


def main() -> None:
    if not OUT_DIR.parent.exists():
        raise SystemExit(f"{OUT_DIR.parent} not found. Is D: mounted?")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sys.stdout.reconfigure(encoding="utf-8")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    for table in ["markets"] + TABLES:
        print(f"Footer size, {table}: {footer_bytes(table):,} bytes")

    for table in TABLES:
        out = OUT_DIR / f"A_rowgroup_stats_{table}.parquet"
        if out.exists():
            print(f"\n{table}: statistics already on disk, skipping remote read: {out}")
        else:
            size = footer_bytes(table)
            if size > MAX_FOOTER_BYTES:
                raise SystemExit(f"{table}: footer is {size:,} bytes, over the 1 GB limit. Stopping.")
            print(f"\n{table}: reading footer remotely")
            fetch_stats(con, table, out)

        con.execute(f"CREATE OR REPLACE VIEW s AS SELECT * FROM read_parquet('{out.as_posix()}')")
        lines = []
        for title, sql in REPORTS.items():
            rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
            lines.append(f"\n=== {table}: {title} ===")
            lines.append(" | ".join(rel.columns))
            lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
        report = "\n".join(lines)
        (OUT_DIR / f"A_rowgroup_stats_{table}_report.txt").write_text(report, encoding="utf-8")
        print(report)


if __name__ == "__main__":
    main()
