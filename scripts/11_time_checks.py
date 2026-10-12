"""Chain-time checks on one bronze month: dataset clock error, and the 24/7 activity criterion.

    python scripts/11_time_checks.py 2024 12

Local only. True UTC time comes from the block -> time lookup (lookup\\block_anchors.csv,
real Polygon block times every 1,000 blocks, linear interpolation), joined on block_number.
Aggregates only: no wallet addresses are printed or saved.
Saves the report to D:\\polymarket-economy-data\\eda\\time_checks_<YYYY-MM>.txt
"""
import argparse
import sys
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")
ANCHORS = DATA_ROOT / "lookup" / "block_anchors.csv"

REPORTS = {
    "Dataset clock against chain time (trades; error = dataset timestamp - chain time, seconds)": """
        SELECT count(*) AS fills,
               round(quantile_cont(err, 0.01)) AS p01, round(quantile_cont(err, 0.25)) AS p25,
               round(median(err)) AS p50, round(quantile_cont(err, 0.75)) AS p75,
               round(quantile_cont(err, 0.99)) AS p99, round(min(err)) AS min, round(max(err)) AS max,
               round(100.0 * count(*) FILTER (abs(err) > 300) / count(*), 2) AS pct_off_by_over_5_min,
               round(100.0 * count(*) FILTER (abs(err) > 3600) / count(*), 2) AS pct_off_by_over_1_hour,
               round(100.0 * count(*) FILTER (to_timestamp(ts)::DATE <> to_timestamp(chain_ts)::DATE) / count(*), 2)
                   AS pct_on_wrong_utc_day
        FROM (SELECT *, ts - chain_ts AS err FROM tc)""",
    "Dataset clock by 100,000-block chunk (trades)": """
        SELECT block_number // 100000 * 100000 AS chunk_start, count(*) AS fills,
               to_timestamp(min(chain_ts))::VARCHAR AS chain_first_utc,
               round(min(ts - chain_ts)) AS min_error_s, round(median(ts - chain_ts)) AS median_error_s,
               round(max(ts - chain_ts)) AS max_error_s
        FROM tc GROUP BY 1 ORDER BY 1""",
    "True daily activity (chain time, UTC days)": """
        WITH d AS (SELECT to_timestamp(chain_ts)::DATE AS day, count(*) AS fills,
                          sum(token_amount) AS shares FROM tc GROUP BY 1),
             w AS (SELECT to_timestamp(chain_ts)::DATE AS day, count(DISTINCT address) AS wallets,
                          sum(usd_amount) AS cash FROM uc GROUP BY 1)
        SELECT d.day, d.fills, round(d.shares / 1e6, 2) AS notional_shares_m,
               round(w.cash / 1e6, 2) AS cash_traded_usd_m, w.wallets AS active_wallets
        FROM d JOIN w USING (day) ORDER BY 1""",
    "24/7 criterion: activity spread by wallet group (chain time)": """
        WITH per AS (
            SELECT address, count(*) AS fills, count(*) FILTER (role = 'maker') AS maker_fills,
                   count(DISTINCT chain_ts::BIGINT // 3600) AS active_hours,
                   count(DISTINCT (chain_ts::BIGINT % 86400) // 3600) AS hours_of_day,
                   count(DISTINCT chain_ts::BIGINT // 86400) AS active_days
            FROM uc GROUP BY 1),
        r AS (SELECT *, row_number() OVER (ORDER BY fills DESC, address) AS by_fills FROM per),
        span AS (SELECT count(DISTINCT chain_ts::BIGINT // 3600) AS hours_in_period,
                        count(DISTINCT chain_ts::BIGINT // 86400) AS days_in_period FROM uc)
        SELECT CASE WHEN by_fills <= 100 THEN '1: top 100 by fills'
                    WHEN by_fills <= 1000 THEN '2: next 900'
                    ELSE '3: all other wallets' END AS wallet_group,
               count(*) AS wallets,
               any_value(hours_in_period) AS hours_in_period,
               round(median(active_hours)) AS median_active_hours,
               round(100.0 * median(active_hours) / any_value(hours_in_period), 1) AS median_pct_of_hours_active,
               round(median(hours_of_day)) AS median_hours_of_day_used,
               round(100.0 * count(*) FILTER (hours_of_day = 24) / count(*), 1) AS pct_wallets_using_all_24_hours,
               round(100.0 * count(*) FILTER (active_hours >= 0.9 * hours_in_period) / count(*), 1)
                   AS pct_wallets_active_90pct_of_hours,
               round(median(active_days)) AS median_active_days,
               any_value(days_in_period) AS days_in_period
        FROM r, span GROUP BY 1 ORDER BY 1""",
    "24/7 criterion: wallets by share of hours active": """
        WITH per AS (
            SELECT address, count(*) AS fills, count(*) FILTER (role = 'maker') AS maker_fills,
                   sum(usd_amount) AS cash, count(DISTINCT chain_ts::BIGINT // 3600) AS active_hours
            FROM uc GROUP BY 1),
        span AS (SELECT count(DISTINCT chain_ts::BIGINT // 3600) AS hours_in_period FROM uc)
        SELECT CASE WHEN active_hours >= 0.9 * hours_in_period THEN '1: 90%+ of hours'
                    WHEN active_hours >= 0.5 * hours_in_period THEN '2: 50-90%'
                    WHEN active_hours >= 0.1 * hours_in_period THEN '3: 10-50%'
                    ELSE '4: under 10%' END AS hours_active,
               count(*) AS wallets,
               round(100.0 * sum(fills) / sum(sum(fills)) OVER (), 2) AS pct_of_fills,
               round(100.0 * sum(cash) / sum(sum(cash)) OVER (), 2) AS pct_of_cash_traded,
               round(100.0 * sum(maker_fills) / sum(fills), 1) AS pct_fills_as_maker,
               round(median(fills)) AS median_fills
        FROM per, span GROUP BY 1 ORDER BY 1""",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    label = f"{args.year:04d}-{args.month:02d}"
    if not ANCHORS.exists():
        raise SystemExit(f"{ANCHORS} not found. Is D: mounted? Run 06_block_times.py first.")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"""
        CREATE TABLE anchors AS
        SELECT block_number AS a0, "timestamp" AS t0,
               lead("timestamp") OVER (ORDER BY block_number) AS t1
        FROM read_csv('{ANCHORS.as_posix()}')""")
    for view, table in (("tc", "trades"), ("uc", "users")):
        month_dir = DATA_ROOT / "bronze" / table / f"year={args.year:04d}" / f"month={args.month:02d}"
        if not month_dir.exists():
            raise SystemExit(f"{month_dir} not found. Run 04_bronze_month.py first.")
        # chain_ts: real time of the anchor below, plus a linear share of the gap to the next anchor
        con.execute(f"""
            CREATE VIEW {view} AS
            SELECT s.* EXCLUDE ("timestamp", block_number),
                   s.block_number::BIGINT AS block_number, s."timestamp"::BIGINT AS ts,
                   a.t0 + (s.block_number::BIGINT - a.a0) * (coalesce(a.t1, a.t0) - a.t0) / 1000.0 AS chain_ts
            FROM read_parquet('{month_dir.as_posix()}/part-*.parquet', hive_partitioning = false) s
            JOIN anchors a ON a.a0 = s.block_number::BIGINT // 1000 * 1000""")

    lines = [f"Chain-time checks: {label}"]
    for title, sql in REPORTS.items():
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
    report = "\n".join(lines)
    out = DATA_ROOT / "eda" / f"time_checks_{label}.txt"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
