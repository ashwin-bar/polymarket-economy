# Polymarket Player Economy

> "Tourists or residents? Polymarket's player economy after the World Cup."

An analytics portfolio project: a big-data ETL pipeline into a Databricks lakehouse, then Tableau dashboards on trader cohorts, cross-sell between categories, and market listings.

**Status:** setup. No data has been ingested and no results exist yet.

## Design

- **Source systems:** the `SII-WANGZJ/Polymarket_data` Hugging Face dump (1.27B trades, 2.03B user fills) and the Polymarket Gamma and Data APIs.
- **Warehouse:** Databricks Free Edition, bronze → silver → gold.
- **Presentation:** Tableau Desktop Free Edition with a live connection to the warehouse for development and demo; Tableau Desktop Public Edition reading exported gold files for the published version.

Published outputs are aggregates only; no wallet-level tables.

## Documentation

The full brief (goals, architecture, data sources, metric definitions, known data issues, EDA plan and decision log) is in [docs/PROJECT_BRIEF.md](docs/PROJECT_BRIEF.md).

## Layout

- Code: this repo.
- Data: a separate folder outside the repo (`D:\polymarket-economy-data`), never committed.
