## Architectural Decision Record
Tables (in `transformer/models`) follow the medallion architecture design principle:

🥉 Staging – Raw data ingestion, untouched \
🥈 Marts – Cleaned and transformed data (facts and dimensions) \
🥇 Reports – Final data products for analytics, BI, or ML

This structure enables incremental processing and data quality improvements at each layer, with progressive refinement from raw ingestion to silver (cleaned and enriched) and gold (business-ready) tables. \
It assigns clear responsibilities per step, fully decouples ingestion from transformations, enhances governance and access control, and simplifies data object discovery for analysts and data scientists.

Generic tests for data quality are included in the `sources.yml` while specific tests on logic are in the `/tests` repo.

## Strategy
This project leverages a DuckDB data warehouse and runs a dbt project via the dbt-duckdb adapter with workflow (including ingestion from file sources) handled by a Typer app. \
The next first two tools to add are:
- A **data ingestion platform** (Fivetran, Airbyte) to load data from different sources into the source area of the DWH
- A **data orchestration platform** (Airflow, Dagster) to automate the workflow execution

## Results
Top tier mentors show a **62% rebooking rate** compared to the **24% (combined)** of standard mentors. \
This is confirmed in both calculation approaches (using booking sessions or mentoring sessions) with only minor difference. \
Results are shown by tier to highlight the relatively good performance of silver mentors and the alarming one of bronze. \
Booking system reliability looks good despite some minor data inconsistencies and several possible improvements (see `stg_events`).

| tier  | tier_category | total_users_w_sessions | rebooked_count | rebooking_rate_pct |
|-------|---------------|------------------------|----------------|--------------------|
| Gold  | top_tier      | 42                     | 26             | 61.90              |
| Silver| standard_tier | 57                     | 22             | 38.60              |
| Bronze| standard_tier | 33                     | 0              | 0.0                |


## How to Run with Docker
docker server installed and running locally
```
git clone <repo> && cd <project-dir>
docker compose up --build
```

## How to Run Locally
install [uv](https://docs.astral.sh/uv/)
```
git clone <repo> && cd <project-dir>
uv sync
source .venv/bin/activate
```

Complete pipeline:
```
uv run src/workflow.py full
uv run src/workflow.py report
```

Or step-by-step:
```
uv run src/workflow.py ingest
uv run src/workflow.py run
uv run src/workflow.py test
uv run src/workflow.py report
```

