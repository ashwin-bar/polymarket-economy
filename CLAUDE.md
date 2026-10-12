# Polymarket Player Economy

Analytics portfolio project: big-data ETL into Databricks, then Tableau dashboards on trader cohorts, cross-sell and market listings.

**Read `docs/PROJECT_BRIEF.md` at the start of every session.** It holds the goals, architecture, data sources, metric definitions, known data issues, EDA plan and decision log.

## Environment
- Windows, PowerShell. Python 3.12 in `.venv` (`.venv\Scripts\activate`). Use double quotes in shell commands.
- Everything is on the USB drive D:. Code: `D:\polymarket-economy` (this repo). Data: `D:\polymarket-economy-data`.
- Check D: is mounted before any step. Remind me to commit and push to GitHub at the end of each session.

## Working rules
1. **One small step at a time.** Run a step, show the key output, say what it means and what you propose next. Wait for agreement before moving to the next step.
2. **Pilot with the smallest data first** (`markets.parquet`, one month, one column) before scaling.
3. **Ask before:** any download or remote scan likely to transfer more than 1 GB, any upload to Databricks, deleting anything on D:, installing packages, or changing the repo structure.
4. **Read only the columns a step needs.** Always read the Hugging Face `main` branch.
5. **Make every step resumable:** check what's already on disk and skip it; keep a manifest of completed months.
6. **Never put data in the repo.** Outputs go to `D:\polymarket-economy-data`; keep `.gitignore` covering data and `.venv`.
7. **No placeholders or guesses.** If something is unverified, say so.
8. **Record decisions** in the decision log (brief §11) with date and evidence.
9. **Flag robustness checkpoints.** Handle small checks inline; stop and tell me when a big-picture review is due (brief §10).
10. **Aggregates only** in anything published; no wallet-level tables on Tableau Public.

## Conventions
- DuckDB for laptop-side reads of the source files; PySpark / Databricks SQL in the warehouse.
- Bronze layout: `bronze\<table>\year=YYYY\month=MM\part-NN.parquet` (`part-00`, `part-01`, …), each file under 3 GB. A month over the limit gets more part files; all source columns are kept.
- Timestamps are UTC; a "day" is a UTC calendar day.
- Volume comes from `trades`; people and activity come from `users`.
