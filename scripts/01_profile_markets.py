"""Test 0, step 2: profile markets.parquet (local file, no download).

Prints each section and saves the full text to
D:\\polymarket-economy-data\\eda\\00_markets_profile.txt
"""
import sys
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")
SOURCE = DATA_ROOT / "raw" / "markets.parquet"
OUT = DATA_ROOT / "eda" / "00_markets_profile.txt"

SECTIONS = {
    "Parquet layout": """
        SELECT count(DISTINCT row_group_id) AS row_groups,
               max(row_group_num_rows) AS max_rows_per_group,
               any_value(compression) AS compression
        FROM parquet_metadata('{src}')""",
    "Schema": "DESCRIBE SELECT * FROM m",
    "Rows and key uniqueness": """
        SELECT count(*) AS n_rows,
               count(DISTINCT id) AS distinct_id,
               count(DISTINCT condition_id) AS distinct_condition_id,
               count(DISTINCT event_id) AS distinct_event_id,
               count(DISTINCT slug) AS distinct_slug
        FROM m""",
    "Null rates and distinct counts": "SUMMARIZE SELECT * FROM m",
    "Sample rows": "SELECT * FROM m USING SAMPLE 3 ROWS (reservoir, 42)",
    "Empty strings (no column has NULLs)": """
        SELECT count(*) FILTER (condition_id = '') AS condition_id,
               count(*) FILTER (token1 = '') AS token1,
               count(*) FILTER (token2 = '') AS token2,
               count(*) FILTER (event_id = '') AS event_id,
               count(*) FILTER (event_slug = '') AS event_slug,
               count(*) FILTER (event_title = '') AS event_title,
               count(*) FILTER (outcome_prices = '[]') AS outcome_prices_empty_list
        FROM m""",
    "Duplicate non-empty condition_id": """
        SELECT count(*) AS duplicated_condition_ids, coalesce(sum(n), 0) AS rows_involved
        FROM (SELECT condition_id, count(*) AS n FROM m
              WHERE condition_id <> '' GROUP BY 1 HAVING count(*) > 1)""",
    "Markets by creation year (UTC); volume is lifetime volume of those markets": """
        SELECT year(created_at) AS created_year,
               count(*) AS markets,
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_markets,
               round(sum(volume) / 1e9, 3) AS volume_usd_bn,
               round(100.0 * sum(volume) / sum(sum(volume)) OVER (), 2) AS pct_volume
        FROM m GROUP BY 1 ORDER BY 1""",
    "Markets by creation month, 2026 (UTC)": """
        SELECT strftime(created_at, '%Y-%m') AS created_month,
               count(*) AS markets,
               round(sum(volume) / 1e9, 3) AS volume_usd_bn
        FROM m WHERE year(created_at) = 2026 GROUP BY 1 ORDER BY 1""",
    "end_date checks": """
        SELECT count(*) FILTER (end_date < created_at) AS end_before_created,
               count(*) FILTER (end_date < TIMESTAMPTZ '2020-01-01') AS end_before_2020,
               count(*) FILTER (end_date > TIMESTAMPTZ '2026-10-04 23:59:59') AS end_after_snapshot,
               count(*) FILTER (closed = 0) AS open_markets
        FROM m""",
    "Volume totals and quantiles (USD)": """
        SELECT round(sum(volume) / 1e9, 3) AS total_usd_bn,
               round(quantile_cont(volume, 0.5), 2) AS p50,
               round(quantile_cont(volume, 0.9), 2) AS p90,
               round(quantile_cont(volume, 0.99), 2) AS p99,
               round(quantile_cont(volume, 0.999), 2) AS p999,
               round(max(volume), 2) AS max
        FROM m""",
    "Volume buckets": """
        SELECT CASE WHEN volume = 0 THEN '0: zero'
                    WHEN volume < 100 THEN '1: under 100'
                    WHEN volume < 1e3 THEN '2: 100 to 1k'
                    WHEN volume < 1e4 THEN '3: 1k to 10k'
                    WHEN volume < 1e5 THEN '4: 10k to 100k'
                    WHEN volume < 1e6 THEN '5: 100k to 1M'
                    WHEN volume < 1e7 THEN '6: 1M to 10M'
                    ELSE '7: 10M and over' END AS bucket,
               count(*) AS markets,
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_markets,
               round(100.0 * sum(volume) / sum(sum(volume)) OVER (), 2) AS pct_volume
        FROM m GROUP BY 1 ORDER BY 1""",
    "Volume concentration across markets": """
        WITH r AS (SELECT volume, row_number() OVER (ORDER BY volume DESC) AS rn,
                          count(*) OVER () AS n, sum(volume) OVER () AS total FROM m)
        SELECT round(100.0 * sum(volume) FILTER (rn <= 100) / any_value(total), 2) AS pct_top_100,
               round(100.0 * sum(volume) FILTER (rn <= 1000) / any_value(total), 2) AS pct_top_1000,
               round(100.0 * sum(volume) FILTER (rn <= n / 100) / any_value(total), 2) AS pct_top_1pct
        FROM r""",
    "Markets per event": """
        WITH e AS (SELECT event_id, count(*) AS n, sum(volume) AS v
                   FROM m WHERE event_id <> '' GROUP BY 1)
        SELECT CASE WHEN n = 1 THEN '1: 1 market'
                    WHEN n <= 5 THEN '2: 2 to 5'
                    WHEN n <= 20 THEN '3: 6 to 20'
                    WHEN n <= 100 THEN '4: 21 to 100'
                    ELSE '5: over 100' END AS markets_in_event,
               count(*) AS events,
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_events,
               sum(n) AS markets,
               round(100.0 * sum(v) / sum(sum(v)) OVER (), 2) AS pct_volume,
               max(n) AS largest_event
        FROM e GROUP BY 1 ORDER BY 1""",
    "Top 20 event_slug patterns by market count (digit runs replaced with #)": """
        SELECT regexp_replace(event_slug, '[0-9]+', '#', 'g') AS pattern,
               count(*) AS markets,
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_markets,
               round(100.0 * sum(volume) / sum(sum(volume)) OVER (), 2) AS pct_volume
        FROM m GROUP BY 1 ORDER BY markets DESC LIMIT 20""",
    "Top 20 event_slug patterns by volume": """
        SELECT regexp_replace(event_slug, '[0-9]+', '#', 'g') AS pattern,
               count(*) AS markets,
               round(sum(volume) / 1e9, 3) AS volume_usd_bn,
               round(100.0 * sum(volume) / sum(sum(volume)) OVER (), 2) AS pct_volume
        FROM m GROUP BY 1 ORDER BY sum(volume) DESC LIMIT 20""",
    "Share of markets and volume in 'updown' slugs": """
        SELECT round(100.0 * count(*) FILTER (event_slug LIKE '%updown%' OR event_slug LIKE '%up-or-down%')
                     / count(*), 2) AS pct_markets,
               round(100.0 * sum(volume) FILTER (event_slug LIKE '%updown%' OR event_slug LIKE '%up-or-down%')
                     / sum(volume), 2) AS pct_volume
        FROM m""",
}


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(f"{SOURCE} not found. Is D: mounted? Run 00_fetch_markets.py first.")
    OUT.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE VIEW m AS SELECT * FROM read_parquet('{SOURCE.as_posix()}')")

    lines = []
    for title, sql in SECTIONS.items():
        # Cast to text in SQL: returning TIMESTAMPTZ values to Python would need pytz.
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql.format(src=SOURCE.as_posix())})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        for row in rel.fetchall():
            lines.append(" | ".join("" if v is None else str(v)[:80] for v in row))

    report = "\n".join(lines)
    OUT.write_text(report, encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    print(report)
    print(f"\nSaved to {OUT}")


if __name__ == "__main__":
    main()
