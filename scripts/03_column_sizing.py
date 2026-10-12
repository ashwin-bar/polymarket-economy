"""Bronze sizing: compressed bytes per column per month, from the remote Parquet footers.

Reads footers only (a few MB per table), no data pages. Saves per-row-group, per-column
sizes to D:\\polymarket-economy-data\\eda\\A_column_sizes_<table>.parquet, then reports
how many part files each month needs for the proposed column list.
Resumable: a table whose sizes file is already on disk is not re-read.

Sizes are the SOURCE file's compressed bytes. Our bronze files are re-written by DuckDB,
so actual sizes will differ; the plan uses a target below the 3 GB limit to leave headroom.
"""
import sys
from pathlib import Path

import duckdb

REPO = "SII-WANGZJ/Polymarket_data"
OUT_DIR = Path(r"D:\polymarket-economy-data") / "eda"
LIMIT_BYTES = 3_000_000_000   # bronze file limit (brief section 5)
TARGET_BYTES = 2_500_000_000  # planning target per part file, leaves headroom

# Proposed bronze columns. Everything not listed can be re-derived from markets.parquet
# by joining on market_id, or is not used by any metric in brief section 7.
KEEP = {
    "trades": [
        "timestamp", "block_number", "transaction_hash", "log_index", "contract",
        "market_id", "maker", "taker", "price", "usd_amount", "token_amount",
        "maker_direction", "taker_direction", "nonusdc_side",
    ],
    "users": [
        "timestamp", "block_number", "transaction_hash", "log_index", "address",
        "role", "direction", "usd_amount", "token_amount", "price", "market_id",
        "nonusdc_side",
    ],
}


def fetch_sizes(con: duckdb.DuckDBPyConnection, table: str, out: Path) -> None:
    tmp = out.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    con.execute(f"""
        COPY (
            WITH meta AS (
                SELECT row_group_id, row_group_num_rows, column_id,
                       path_in_schema AS column_name, type, compression,
                       total_compressed_size, total_uncompressed_size,
                       coalesce(stats_min_value, stats_min) AS stats_min
                FROM parquet_metadata('hf://datasets/{REPO}@main/{table}.parquet')
            )
            SELECT m.* EXCLUDE (stats_min),
                   -- test A: every row group lies inside one UTC calendar month
                   strftime(to_timestamp(t.stats_min::BIGINT), '%Y-%m') AS month
            FROM meta m
            JOIN (SELECT row_group_id, stats_min FROM meta WHERE column_name = 'timestamp') t
              USING (row_group_id)
        ) TO '{tmp.as_posix()}' (FORMAT parquet)""")
    tmp.replace(out)


def reports(table: str) -> dict[str, str]:
    keep = ", ".join(f"'{c}'" for c in KEEP[table])
    return {
        "Columns: compressed size over the whole file": f"""
            SELECT column_name, any_value(type) AS type, any_value(compression) AS compression,
                   round(sum(total_compressed_size) / 1e9, 2) AS compressed_gb,
                   round(100.0 * sum(total_compressed_size) / sum(sum(total_compressed_size)) OVER (), 1) AS pct_of_file,
                   round(sum(total_compressed_size) / sum(row_group_num_rows), 1) AS bytes_per_row,
                   CASE WHEN column_name IN ({keep}) THEN 'keep' ELSE 'drop' END AS proposed
            FROM s GROUP BY column_name ORDER BY min(column_id)""",
        "Totals: all columns vs proposed columns": f"""
            SELECT round(sum(total_compressed_size) / 1e9, 2) AS all_columns_gb,
                   round(sum(total_compressed_size) FILTER (column_name IN ({keep})) / 1e9, 2) AS proposed_gb,
                   count(DISTINCT column_name) AS all_columns,
                   count(DISTINCT column_name) FILTER (column_name IN ({keep})) AS proposed_columns
            FROM s""",
        "Proposed columns not found in the file (must be empty)": f"""
            SELECT k AS missing FROM (SELECT unnest([{keep}]) AS k)
            WHERE k NOT IN (SELECT column_name FROM s)""",
        f"Part files per month at a {TARGET_BYTES / 1e9:.1f} GB target (months needing more than one file, plus totals)": f"""
            WITH mth AS (
                SELECT month,
                       sum(row_group_num_rows) FILTER (column_name = 'timestamp') AS n_rows,
                       count(DISTINCT row_group_id) AS row_groups,
                       sum(total_compressed_size) FILTER (column_name IN ({keep})) AS proposed_bytes,
                       sum(total_compressed_size) AS all_bytes
                FROM s GROUP BY month),
            plan AS (
                SELECT *, greatest(1, ceil(proposed_bytes / {TARGET_BYTES}))::INT AS files_proposed,
                          greatest(1, ceil(all_bytes / {TARGET_BYTES}))::INT AS files_all_columns
                FROM mth)
            SELECT month, n_rows AS rows, row_groups,
                   round(proposed_bytes / 1e9, 2) AS proposed_gb, files_proposed,
                   round(proposed_bytes / files_proposed / 1e9, 2) AS gb_per_file,
                   round(all_bytes / 1e9, 2) AS all_columns_gb, files_all_columns
            FROM plan WHERE files_proposed > 1 OR files_all_columns > 1
            UNION ALL
            SELECT 'months with 1 file: ' || count(*) FILTER (files_proposed = 1), NULL, NULL,
                   round(max(proposed_bytes) FILTER (files_proposed = 1) / 1e9, 2), NULL, NULL, NULL, NULL
            FROM plan
            UNION ALL
            SELECT 'TOTAL, ' || count(*) || ' months', sum(n_rows), sum(row_groups),
                   round(sum(proposed_bytes) / 1e9, 2), sum(files_proposed), NULL,
                   round(sum(all_bytes) / 1e9, 2), sum(files_all_columns)
            FROM plan
            ORDER BY 1""",
    }


def main() -> None:
    if not OUT_DIR.parent.exists():
        raise SystemExit(f"{OUT_DIR.parent} not found. Is D: mounted?")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sys.stdout.reconfigure(encoding="utf-8")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    for table in KEEP:
        out = OUT_DIR / f"A_column_sizes_{table}.parquet"
        if out.exists():
            print(f"\n{table}: sizes already on disk, skipping remote read: {out}")
        else:
            print(f"\n{table}: reading footer remotely")
            fetch_sizes(con, table, out)

        con.execute(f"CREATE OR REPLACE VIEW s AS SELECT * FROM read_parquet('{out.as_posix()}')")
        lines = []
        for title, sql in reports(table).items():
            rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
            lines.append(f"\n=== {table}: {title} ===")
            lines.append(" | ".join(rel.columns))
            lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
        report = "\n".join(lines)
        (OUT_DIR / f"A_column_sizes_{table}_report.txt").write_text(report, encoding="utf-8")
        print(report)


if __name__ == "__main__":
    main()
