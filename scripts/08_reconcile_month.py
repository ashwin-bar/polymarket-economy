"""Reconcile users against trades for one bronze month (brief section 9, concern 7).

    python scripts/08_reconcile_month.py 2024 12

Local only. Both tables must already be in bronze for the month (same block range).
Saves the report to D:\\polymarket-economy-data\\eda\\reconcile_<YYYY-MM>.txt

Dataset card semantics being tested: each match emits one OrderFilled event per maker order
plus one summary event for the taker order. `trades` keeps the maker-order events only;
`users` keeps every event, keyed by the wallet whose order filled, with role maker / taker.
"""
import argparse
import sys
from pathlib import Path

import duckdb

DATA_ROOT = Path(r"D:\polymarket-economy-data")

# Exchange contracts tracked by the dataset, from its card on Hugging Face (main branch,
# "Contracts Tracked"), lower-cased to match the data.
EXCHANGE_CONTRACTS = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "CTF Exchange (V1)",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a": "NegRisk CTF Exchange (V1)",
    "0xe111180000d2663c0091e4f400237545b87b996b": "CTF Exchange (V2)",
    "0xe2222d279d744050d28e00520010520000310f59": "NegRisk CTF Exchange (V2)",
}

REPORTS = {
    "Rows and USD": """
        SELECT (SELECT count(*) FROM t) AS trades_rows,
               (SELECT count(*) FROM u) AS users_rows,
               round((SELECT count(*) FROM u) / (SELECT count(*) FROM t), 4) AS users_rows_per_trade,
               round((SELECT sum(usd_amount) FROM t) / 1e6, 3) AS trades_usd_m,
               round((SELECT sum(usd_amount) FROM u) / 1e6, 3) AS users_usd_m,
               round((SELECT sum(usd_amount) FROM u) / (SELECT sum(usd_amount) FROM t), 4) AS users_usd_per_trades_usd""",
    "users by role": """
        SELECT role, count(*) AS n_rows, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_rows,
               round(sum(usd_amount) / 1e6, 3) AS usd_m, count(DISTINCT address) AS distinct_wallets,
               count(DISTINCT transaction_hash) AS distinct_transactions
        FROM u GROUP BY 1 ORDER BY 1""",
    "users maker rows against trades, joined on (transaction_hash, log_index)": """
        WITH um AS (SELECT * FROM u WHERE role = 'maker'),
             j AS (SELECT t.transaction_hash AS t_key, um.transaction_hash AS u_key,
                          t.maker, um.address, t.usd_amount AS t_usd, um.usd_amount AS u_usd,
                          t.token_amount AS t_tok, um.token_amount AS u_tok, t.market_id AS t_mkt, um.market_id AS u_mkt
                   FROM t FULL OUTER JOIN um
                     ON t.transaction_hash = um.transaction_hash AND t.log_index = um.log_index)
        SELECT count(*) FILTER (t_key IS NOT NULL AND u_key IS NOT NULL) AS matched,
               count(*) FILTER (u_key IS NULL) AS trades_without_users_maker_row,
               count(*) FILTER (t_key IS NULL) AS users_maker_rows_without_trade,
               count(*) FILTER (maker <> address) AS address_differs_from_trades_maker,
               count(*) FILTER (abs(t_usd - u_usd) > 1e-6) AS usd_differs,
               count(*) FILTER (abs(t_tok - u_tok) > 1e-6) AS token_amount_differs,
               count(*) FILTER (t_mkt <> u_mkt) AS market_differs
        FROM j""",
    "users taker rows against trades, per transaction": """
        WITH tt AS (SELECT transaction_hash, sum(usd_amount) AS usd, count(*) AS fills,
                           count(DISTINCT taker) AS takers FROM t GROUP BY 1),
             ut AS (SELECT transaction_hash, sum(usd_amount) AS usd, count(*) AS n_rows,
                           count(DISTINCT address) AS wallets FROM u WHERE role = 'taker' GROUP BY 1),
             j AS (SELECT tt.transaction_hash AS t_key, ut.transaction_hash AS u_key,
                          tt.usd AS t_usd, ut.usd AS u_usd, ut.n_rows AS taker_rows, tt.takers
                   FROM tt FULL OUTER JOIN ut USING (transaction_hash))
        SELECT count(*) FILTER (t_key IS NOT NULL) AS transactions_in_trades,
               count(*) FILTER (u_key IS NOT NULL) AS transactions_with_taker_rows,
               count(*) FILTER (t_key IS NOT NULL AND u_key IS NULL) AS trades_tx_without_taker_row,
               count(*) FILTER (t_key IS NULL) AS taker_rows_tx_without_trades,
               count(*) FILTER (abs(t_usd - u_usd) > 0.01) AS tx_where_taker_usd_differs_from_trades_usd,
               round(sum(abs(t_usd - u_usd)) FILTER (abs(t_usd - u_usd) > 0.01) / 1e6, 3) AS abs_difference_usd_m,
               max(taker_rows) AS max_taker_rows_per_tx
        FROM j""",
    "Why taker USD differs from the makers' USD: direct vs complementary matches": """
        -- Direct match: maker is on the opposite side of the same token, so both legs carry the same USD.
        -- Complementary match: maker has the SAME direction on the other outcome token (mint or merge),
        -- so the maker's USD leg is token_amount - usd_amount of the taker's.
        WITH tk AS (SELECT transaction_hash, direction AS tdir, usd_amount AS tusd, token_amount AS ttok
                    FROM u WHERE role = 'taker'),
             j AS (SELECT tk.transaction_hash, any_value(tusd) AS tusd, any_value(ttok) AS ttok,
                          sum(m.usd_amount) AS musd, sum(m.token_amount) AS mtok,
                          sum(CASE WHEN m.direction <> tk.tdir THEN m.usd_amount
                                   ELSE m.token_amount - m.usd_amount END) AS expected_tusd,
                          bool_or(m.direction = tk.tdir) AS any_same, bool_and(m.direction = tk.tdir) AS all_same
                   FROM tk JOIN (SELECT * FROM u WHERE role = 'maker') m USING (transaction_hash)
                   GROUP BY 1)
        SELECT CASE WHEN NOT any_same THEN '1: all makers opposite direction (direct)'
                    WHEN all_same THEN '2: all makers same direction (complementary)'
                    ELSE '3: mixed' END AS match_kind,
               count(*) AS transactions,
               round(sum(tusd) / 1e6, 3) AS taker_usd_m, round(sum(musd) / 1e6, 3) AS makers_usd_m,
               count(*) FILTER (abs(tusd - musd) > 0.01) AS usd_differs,
               count(*) FILTER (abs(ttok - mtok) > 0.01) AS tokens_differ,
               count(*) FILTER (abs(tusd - expected_tusd) > 0.01) AS not_explained_by_rule,
               round(coalesce(sum(abs(tusd - expected_tusd)) FILTER (abs(tusd - expected_tusd) > 0.01), 0) / 1e6, 3)
                   AS unexplained_usd_m
        FROM j GROUP BY 1 ORDER BY 1""",
    "Direction values": """
        SELECT role, direction, count(*) AS n_rows FROM u GROUP BY 1, 2 ORDER BY 1, 2""",
    "Taker rows whose transaction has no row in trades": """
        SELECT count(*) AS taker_rows, round(sum(usd_amount), 2) AS usd,
               count(*) FILTER (usd_amount = 0) AS usd_zero, count(DISTINCT address) AS wallets,
               count(DISTINCT market_id) AS markets
        FROM u WHERE role = 'taker' AND transaction_hash NOT IN (SELECT transaction_hash FROM t)""",
    "users taker wallet against trades.taker (transactions with exactly one taker row)": """
        WITH ut AS (SELECT transaction_hash, any_value(address) AS address FROM u
                    WHERE role = 'taker' GROUP BY 1 HAVING count(*) = 1)
        SELECT count(*) AS fills_checked,
               count(*) FILTER (t.taker = ut.address) AS trades_taker_equals_users_taker_wallet,
               count(*) FILTER (t.taker <> ut.address) AS differs
        FROM t JOIN ut USING (transaction_hash)""",
    "Distinct wallets in the month": """
        SELECT (SELECT count(DISTINCT address) FROM u) AS users_wallets,
               (SELECT count(DISTINCT address) FROM u WHERE role = 'maker') AS users_maker_wallets,
               (SELECT count(DISTINCT address) FROM u WHERE role = 'taker') AS users_taker_wallets,
               (SELECT count(*) FROM (SELECT address FROM u WHERE role = 'maker'
                                      INTERSECT SELECT address FROM u WHERE role = 'taker')) AS wallets_in_both_roles,
               (SELECT count(DISTINCT maker) FROM t) AS trades_distinct_maker,
               (SELECT count(DISTINCT taker) FROM t) AS trades_distinct_taker,
               (SELECT count(*) FROM (SELECT maker FROM t UNION SELECT taker FROM t)) AS trades_maker_or_taker,
               (SELECT count(*) FROM (SELECT address FROM u EXCEPT
                                      (SELECT maker FROM t UNION SELECT taker FROM t))) AS users_wallets_not_in_trades,
               (SELECT count(*) FROM ((SELECT maker FROM t UNION SELECT taker FROM t)
                                      EXCEPT SELECT address FROM u)) AS trades_wallets_not_in_users""",
    "Exchange contract addresses appearing as a wallet (all must be 0)": """
        SELECT c.name, c.address,
               (SELECT count(*) FROM u WHERE lower(u.address) = c.address) AS users_address_rows,
               (SELECT count(*) FROM t WHERE lower(t.maker) = c.address) AS trades_maker_rows,
               (SELECT count(*) FROM t WHERE lower(t.taker) = c.address) AS trades_taker_rows
        FROM contracts c ORDER BY 1""",
    "Address format in users": """
        SELECT count(*) FILTER (address <> lower(address)) AS not_lowercase,
               count(*) FILTER (NOT regexp_full_match(address, '0x[0-9a-f]{40}')) AS not_a_20_byte_hex_address,
               count(*) FILTER (address IS NULL OR address = '') AS empty
        FROM u""",
    "Concentration: share of users rows and USD from the busiest wallets": """
        WITH w AS (SELECT address, count(*) AS n, sum(usd_amount) AS usd FROM u GROUP BY 1),
             r AS (SELECT *, row_number() OVER (ORDER BY n DESC) AS by_rows,
                          row_number() OVER (ORDER BY usd DESC) AS by_usd FROM w)
        SELECT round(100.0 * sum(n) FILTER (by_rows <= 1) / sum(n), 2) AS pct_rows_top_1,
               round(100.0 * sum(n) FILTER (by_rows <= 10) / sum(n), 2) AS pct_rows_top_10,
               round(100.0 * sum(n) FILTER (by_rows <= 100) / sum(n), 2) AS pct_rows_top_100,
               round(100.0 * sum(usd) FILTER (by_usd <= 10) / sum(usd), 2) AS pct_usd_top_10,
               round(100.0 * sum(usd) FILTER (by_usd <= 100) / sum(usd), 2) AS pct_usd_top_100,
               round(median(n)) AS median_rows_per_wallet
        FROM r""",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    label = f"{args.year:04d}-{args.month:02d}"

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for view, table in (("t", "trades"), ("u", "users")):
        month_dir = DATA_ROOT / "bronze" / table / f"year={args.year:04d}" / f"month={args.month:02d}"
        if not month_dir.exists():
            raise SystemExit(f"{month_dir} not found. Is D: mounted? Run 04_bronze_month.py first.")
        con.execute(f"""CREATE VIEW {view} AS SELECT * FROM
            read_parquet('{month_dir.as_posix()}/part-*.parquet', hive_partitioning = false)""")
    con.execute("CREATE TABLE contracts (address VARCHAR, name VARCHAR)")
    con.executemany("INSERT INTO contracts VALUES (?, ?)", list(EXCHANGE_CONTRACTS.items()))

    lines = [f"Reconciliation of users against trades: {label}"]
    for title, sql in REPORTS.items():
        rel = con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM ({sql})")
        lines.append(f"\n=== {title} ===")
        lines.append(" | ".join(rel.columns))
        lines.extend(" | ".join("" if v is None else v for v in row) for row in rel.fetchall())
    report = "\n".join(lines)
    out = DATA_ROOT / "eda" / f"reconcile_{label}.txt"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
