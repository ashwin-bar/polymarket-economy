"""Which measure of traded USD does markets.volume follow? One bronze month, local only.

    python scripts/09_volume_definition.py 2024 12

Compares several candidate measures per market with markets.volume. Only markets created
inside the month are used, so every trade before the month's end is in the bronze files.
A market that also traded after the month will show a ratio below 1 under the right
definition; a market that finished inside the month will show a ratio of exactly 1.
The right definition is therefore the one with a spike of markets at ratio 1.

created_at is not fully reliable (brief section 9, concern 14); it is used here only to pick
the comparison set, and the spike test does not depend on it being exact.

Saves the report to D:\\polymarket-economy-data\\eda\\volume_definition_<YYYY-MM>.txt
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")

CANDIDATES = {
    "maker_usd": "Sum of trades.usd_amount (maker leg of each fill)",
    "taker_usd": "Sum of users.usd_amount where role = taker (taker leg)",
    "both_legs_usd": "maker_usd + taker_usd",
    "shares": "Sum of trades.token_amount (shares matched)",
    "taker_shares": "Sum of users.token_amount where role = taker",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    label = f"{args.year:04d}-{args.month:02d}"
    start = datetime(args.year, args.month, 1, tzinfo=timezone.utc)
    end = datetime(args.year + args.month // 12, args.month % 12 + 1, 1, tzinfo=timezone.utc)

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for view, table in (("t", "trades"), ("u", "users")):
        month_dir = DATA_ROOT / "bronze" / table / f"year={args.year:04d}" / f"month={args.month:02d}"
        if not month_dir.exists():
            raise SystemExit(f"{month_dir} not found. Is D: mounted? Run 04_bronze_month.py first.")
        con.execute(f"""CREATE VIEW {view} AS SELECT * FROM
            read_parquet('{month_dir.as_posix()}/part-*.parquet', hive_partitioning = false)""")
    markets = DATA_ROOT / "raw" / "markets.parquet"
    con.execute(f"""
        CREATE TABLE per_market AS
        WITH mk AS (SELECT market_id, sum(usd_amount) AS maker_usd, sum(token_amount) AS shares,
                           count(*) AS fills FROM t GROUP BY 1),
             tk AS (SELECT market_id, sum(usd_amount) AS taker_usd, sum(token_amount) AS taker_shares
                    FROM u WHERE role = 'taker' GROUP BY 1)
        SELECT m.id AS market_id, m.volume, m.closed, m.neg_risk, mk.fills,
               mk.maker_usd, tk.taker_usd, mk.maker_usd + tk.taker_usd AS both_legs_usd,
               mk.shares, tk.taker_shares
        FROM read_parquet('{markets.as_posix()}') m
        JOIN mk ON mk.market_id = m.id
        JOIN tk ON tk.market_id = m.id
        WHERE m.created_at >= TIMESTAMPTZ '{start.isoformat()}'
          AND m.created_at < TIMESTAMPTZ '{end.isoformat()}'
          AND m.volume > 0""")

    reports = {
        "Comparison set: markets created in the month that have fills in it": """
            SELECT count(*) AS markets, count(*) FILTER (closed = 1) AS closed,
                   count(*) FILTER (neg_risk = 1) AS neg_risk,
                   round(sum(volume) / 1e6, 3) AS markets_volume_usd_m,
                   round(sum(maker_usd) / 1e6, 3) AS maker_usd_m,
                   round(sum(taker_usd) / 1e6, 3) AS taker_usd_m,
                   round(sum(shares) / 1e6, 3) AS shares_m
            FROM per_market""",
    }
    union = " UNION ALL ".join(
        f"SELECT '{name}' AS candidate, {name} / volume AS ratio, volume, neg_risk FROM per_market"
        for name in CANDIDATES)
    reports["Ratio of each candidate to markets.volume, per market"] = f"""
        SELECT candidate, count(*) AS markets,
               count(*) FILTER (abs(ratio - 1) <= 0.001) AS within_0_1_pct,
               round(100.0 * count(*) FILTER (abs(ratio - 1) <= 0.001) / count(*), 1) AS pct_within_0_1_pct,
               count(*) FILTER (abs(ratio - 1) <= 0.01) AS within_1_pct,
               count(*) FILTER (ratio > 1.01) AS above_1,
               round(quantile_cont(ratio, 0.25), 4) AS p25, round(median(ratio), 4) AS p50,
               round(quantile_cont(ratio, 0.75), 4) AS p75
        FROM ({union}) GROUP BY 1 ORDER BY within_0_1_pct DESC"""
    reports["Same, split by neg-risk flag"] = f"""
        SELECT candidate, neg_risk, count(*) AS markets,
               round(100.0 * count(*) FILTER (abs(ratio - 1) <= 0.001) / count(*), 1) AS pct_within_0_1_pct,
               round(100.0 * count(*) FILTER (ratio > 1.01) / count(*), 1) AS pct_above_1,
               round(median(ratio), 4) AS p50
        FROM ({union}) GROUP BY 1, 2 ORDER BY 2, 4 DESC"""
    reports["Markets matching no candidate within 1%"] = """
        SELECT (SELECT count(*) FROM per_market) AS markets,
               (SELECT count(*) FROM per_market
                WHERE abs(maker_usd / volume - 1) > 0.01 AND abs(taker_usd / volume - 1) > 0.01
                  AND abs(both_legs_usd / volume - 1) > 0.01 AND abs(shares / volume - 1) > 0.01
                  AND abs(taker_shares / volume - 1) > 0.01) AS match_no_candidate,
               (SELECT count(*) FROM per_market
                WHERE maker_usd / volume < 0.99 AND taker_usd / volume < 0.99
                  AND shares / volume < 0.99) AS all_main_candidates_below_1"""

    lines = [f"Volume definition test: {label}"]
    lines += [f"  {name}: {desc}" for name, desc in CANDIDATES.items()]
    for title, sql in reports.items():
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
    report = "\n".join(lines)
    out = DATA_ROOT / "eda" / f"volume_definition_{label}.txt"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
