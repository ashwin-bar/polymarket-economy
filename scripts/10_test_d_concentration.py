"""Test D on one bronze month: how concentrated are fills and cash among wallets?

    python scripts/10_test_d_concentration.py 2024 12

Local only. Aggregates only: no wallet addresses are printed or saved.
Saves the report to D:\\polymarket-economy-data\\eda\\D_concentration_<YYYY-MM>.txt

Measures (brief section 7):
    fills        rows in users for the wallet (each of its orders' fills, maker or taker)
    cash traded  the wallet's USD across buys and sells
    cash staked  the wallet's USD on BUY fills only
"""
import argparse
import sys
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")
TOP_N = [1, 10, 100, 1000]

REPORTS = {
    "Month totals": """
        SELECT (SELECT count(*) FROM w) AS wallets,
               (SELECT sum(fills) FROM w) AS fills,
               round((SELECT sum(cash_traded) FROM w) / 1e6, 3) AS cash_traded_usd_m,
               round((SELECT sum(cash_staked) FROM w) / 1e6, 3) AS cash_staked_usd_m,
               round((SELECT sum(token_amount) FROM t) / 1e6, 3) AS notional_volume_shares_m""",
    "Per-wallet distribution": """
        SELECT round(quantile_cont(fills, 0.5), 1) AS fills_p50, round(quantile_cont(fills, 0.9), 1) AS fills_p90,
               round(quantile_cont(fills, 0.99), 1) AS fills_p99, max(fills) AS fills_max,
               round(100.0 * count(*) FILTER (fills = 1) / count(*), 2) AS pct_wallets_with_1_fill,
               round(quantile_cont(cash_traded, 0.5), 2) AS cash_p50, round(quantile_cont(cash_traded, 0.9), 2) AS cash_p90,
               round(quantile_cont(cash_traded, 0.99), 2) AS cash_p99, round(max(cash_traded), 2) AS cash_max
        FROM w""",
    "Profile of the busiest wallets by fills, against everyone else": """
        SELECT CASE WHEN by_fills <= 100 THEN '1: top 100 by fills'
                    WHEN by_fills <= 1000 THEN '2: next 900'
                    ELSE '3: all other wallets' END AS wallet_group,
               count(*) AS wallets, sum(fills) AS fills,
               round(100.0 * sum(maker_fills) / sum(fills), 1) AS pct_fills_as_maker,
               round(sum(cash_traded) / sum(fills), 2) AS cash_per_fill,
               round(median(markets)) AS median_markets_traded,
               round(median(fills)) AS median_fills,
               round(100.0 * sum(cash_staked) / sum(cash_traded), 1) AS pct_cash_on_buys
        FROM r GROUP BY 1 ORDER BY 1""",
}


def concentration_sql() -> str:
    rows = []
    for n in TOP_N:
        rows.append(f"""
            SELECT 'top {n:,}' AS wallets_ranked, {n} AS n_wallets,
                   round(100.0 * sum(fills) FILTER (by_fills <= {n}) / sum(fills), 2) AS pct_of_fills,
                   round(100.0 * sum(cash_traded) FILTER (by_cash <= {n}) / sum(cash_traded), 2) AS pct_of_cash_traded,
                   round(100.0 * sum(cash_staked) FILTER (by_staked <= {n}) / sum(cash_staked), 2) AS pct_of_cash_staked,
                   round(100.0 * sum(cash_traded) FILTER (by_fills <= {n}) / sum(cash_traded), 2)
                       AS pct_of_cash_traded_held_by_top_fillers
            FROM r""")
    rows.append("""
        SELECT 'top 1% of wallets', max(by_fills) // 100,
               round(100.0 * sum(fills) FILTER (by_fills <= total // 100) / sum(fills), 2),
               round(100.0 * sum(cash_traded) FILTER (by_cash <= total // 100) / sum(cash_traded), 2),
               round(100.0 * sum(cash_staked) FILTER (by_staked <= total // 100) / sum(cash_staked), 2),
               round(100.0 * sum(cash_traded) FILTER (by_fills <= total // 100) / sum(cash_traded), 2)
        FROM r""")
    return " UNION ALL ".join(rows) + " ORDER BY n_wallets"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    label = f"{args.year:04d}-{args.month:02d}"

    con = duckdb.connect()
    for view, table in (("t", "trades"), ("u", "users")):
        month_dir = DATA_ROOT / "bronze" / table / f"year={args.year:04d}" / f"month={args.month:02d}"
        if not month_dir.exists():
            raise SystemExit(f"{month_dir} not found. Is D: mounted? Run 04_bronze_month.py first.")
        con.execute(f"""CREATE VIEW {view} AS SELECT * FROM
            read_parquet('{month_dir.as_posix()}/part-*.parquet', hive_partitioning = false)""")
    con.execute("""
        CREATE TABLE w AS
        SELECT address, count(*) AS fills, count(*) FILTER (role = 'maker') AS maker_fills,
               sum(usd_amount) AS cash_traded,
               coalesce(sum(usd_amount) FILTER (direction = 'BUY'), 0) AS cash_staked,
               count(DISTINCT market_id) AS markets
        FROM u GROUP BY 1""")
    con.execute("""
        CREATE TABLE r AS
        SELECT * EXCLUDE (address),
               row_number() OVER (ORDER BY fills DESC, address) AS by_fills,
               row_number() OVER (ORDER BY cash_traded DESC, address) AS by_cash,
               row_number() OVER (ORDER BY cash_staked DESC, address) AS by_staked,
               count(*) OVER () AS total
        FROM w""")

    reports = dict(REPORTS)
    reports = {
        "Month totals": reports["Month totals"],
        "Concentration (each column ranks wallets by its own measure, except the last)": concentration_sql(),
        "Per-wallet distribution": reports["Per-wallet distribution"],
        "Profile of the busiest wallets by fills, against everyone else":
            reports["Profile of the busiest wallets by fills, against everyone else"],
    }
    lines = [f"Test D: wallet concentration, {label}"]
    for title, sql in reports.items():
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
    report = "\n".join(lines)
    out = DATA_ROOT / "eda" / f"D_concentration_{label}.txt"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
