# claims-lakehouse-databricks-azure

An event-driven claims lakehouse on Azure and Databricks. It ingests monthly healthcare claims
deliveries from two payers, CMS CCLF (Medicare fee-for-service) and Humana Medicare Advantage,
through a medallion architecture: landing → bronze → silver → gold.

It is the sibling of [claims-lakehouse-dbt-duckdb](https://github.com/fredr171/claims-lakehouse-dbt-duckdb)
(AWS, Lambda, dbt-duckdb). Both use the same synthetic data from
[synthetic-payer-files](https://github.com/fredr171/synthetic-payer-files) and must reproduce
its answer keys exactly. All data is synthetic.

> **Status:** phase 1 (local foundation) in progress. Architecture, design decisions and
> screenshots will be added as the build progresses.

## Local setup

Requires Python 3.12 and Java 17 (for local Spark). From PowerShell:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\ruff check .
.venv\Scripts\pytest -m "not slow"   # fast subset, as in CI
.venv\Scripts\pytest                 # everything, including the 36-month answer-key tests
```
