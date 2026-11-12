# Claims Lakehouse: Azure + Databricks

[![CI](https://github.com/fredr171/claims-lakehouse-databricks-azure/actions/workflows/ci.yml/badge.svg)](https://github.com/fredr171/claims-lakehouse-databricks-azure/actions/workflows/ci.yml)

An event-driven claims lakehouse on Azure and Databricks. It ingests monthly healthcare claims
deliveries from two payers, CMS CCLF (Medicare fee-for-service) and a fictional Medicare
Advantage payer, through a medallion architecture (landing → bronze → silver → gold), and
publishes payer-agnostic marts. A stopped build or a quarantined file becomes a GitHub issue
that closes itself when the data recovers.

**Same spec, two platforms.** This is the sibling of
[claims-lakehouse-dbt-duckdb](https://github.com/fredr171/claims-lakehouse-dbt-duckdb)
(AWS, Lambda, dbt-duckdb). Both consume the same synthetic data from
[synthetic-payer-files](https://github.com/fredr171/synthetic-payer-files) and must reproduce
its **answer keys** (the exact results a correct pipeline produces) exactly. The design
decisions are the same; the implementation is platform-native, and every place the platforms
forced a different choice is written down. All data is synthetic.

**What it shows:**
- Lakeflow Declarative Pipelines, Auto Loader, AUTO CDC (SCD2) and Unity Catalog on
  serverless Databricks, deployed with Asset Bundles, verified live against the answer keys
  for all 36 months.
- Whole-file validation in an Azure Function before Auto Loader, into a write-once (WORM)
  archive that is the audit record.
- Stop vs warn gating per payer: a bad delivery holds back only its own payer, and a
  corrected re-send recovers it.
- Unity Catalog column masks on PII, keyless storage, OIDC deploys, and Terraform for every
  resource, kept inside a $25/month budget.

**Stack:** Python 3.12, PySpark 4 (Databricks serverless), Lakeflow Declarative Pipelines,
Auto Loader, Unity Catalog, Azure Functions, Event Grid, ADLS Gen2, Terraform, Databricks
Asset Bundles, pytest, ruff, GitHub Actions.

## Architecture

```mermaid
flowchart LR
    payer["Payer CSV<br/>monthly delivery"] --> landing[("landing<br/>ADLS Gen2")]
    landing --> eg["Event Grid"] --> q[["landing-events queue"]] --> fn["Validation Function<br/>file-level checks"]
    fn -->|valid| archive[("archive<br/>checksum-keyed, immutable")]
    fn -->|invalid| quarantine[("quarantine<br/>+ error report")]
    archive --> al["Auto Loader<br/>(file-arrival job)"] --> bronze[("bronze<br/>Delta, all strings")]
    bronze --> cms["CCLF pipeline<br/>silver + conformed views"]
    bronze --> ma["MA payer pipeline<br/>silver + conformed views"]
    cms --> gold[("gold marts<br/>SQL, Unity Catalog")]
    ma --> gold
    gold --> check["Answer-key check"] --> outcome["Run outcome<br/>per feed"]
    outcome --> notifier["Notifier Function"] --> issues["GitHub issues"]
    fn -.->|"Azure Monitor alerts"| notifier
```

| Layer | Where | What it holds |
|---|---|---|
| Landing | ADLS Gen2 `landing` | CSVs exactly as delivered |
| Archive | ADLS Gen2 `archive` | Validated files, checksum-keyed, immutable: the audit record |
| Quarantine | ADLS Gen2 `quarantine` | Rejected files plus an error report |
| Bronze | Delta tables | Every validated delivery, all columns as strings, plus lineage |
| Silver | Lakeflow pipeline per payer | Current deliveries, typed; member history (SCD2); claim families with net paid |
| Gold | Delta tables (SQL) | Payer-agnostic members, claims, member months, PMPM |

## Key decisions

Each is explained in full, with the alternatives, in [docs/design.md](docs/design.md).

1. **Validate whole files before Auto Loader.** Auto Loader is row-oriented and cannot reject
   a file, so a Python Azure Function runs every file-level check first and Auto Loader only
   ever reads the archive.
2. **Checksum-keyed archive paths.** Auto Loader never reprocesses a path it has seen, so a
   corrected re-send under the same file name would otherwise be silently dropped.
3. **One Lakeflow pipeline per payer**, with the transformations in plain, importable PySpark
   modules that the pipelines call and the tests run locally.
4. **AUTO CDC for member history, plus a full refresh on supersession.** AUTO CDC handles
   out-of-order months but cannot retract a delivery that a re-send replaced, so the job
   detects new supersessions and full-refreshes that payer's pipeline.
5. **A one-row gate table** that every published silver table joins, so a failed stop
   expectation blocks the whole payer and leaves its last good tables in place.
6. **Gold is SQL over a payer registry.** Each payer supplies contracted conformed views;
   onboarding, retiring or offboarding a payer never changes mart SQL.
7. **PII is masked in Unity Catalog**, while the pipelines run as a service principal that
   can read real values, so history is still computed correctly.
8. **One GitHub issue per problem**, keyed by the feed the failing check reads, opened,
   commented and closed by the notifier Function. Issues carry identifiers and counts only.

### Where it differs from the sibling, and why
- **Line breaks inside quoted fields are rejected**, so bronze can read without multiLine.
- **Spark's `try_cast` is lenient** (it reads `2023-1-5` as a date and rounds `12.345`), so
  date and amount formats are checked explicitly before typing.
- **Delivery order is archive time, not ingest time**, because Auto Loader can ingest an
  original and its re-send in one micro-batch.
- **The archive is protected by container-level WORM**, because Unity Catalog requires a
  hierarchical namespace, which rules out blob versioning.

## Verified live

Every phase was run live on Azure and Databricks serverless, then torn down:
- a valid file is archived, a broken one is quarantined with its report, a repeat is a
  duplicate, and the archive refuses deletes;
- every delivery started its job run through the file-arrival trigger;
- after all 36 months, gold matched both answer keys; a corrected re-send took the
  full-refresh path and still matched;
- a duplicate-key delivery stopped CCLF while the MA payer refreshed, opened an issue, and a
  corrected re-send closed it; a quarantined file opened an alert issue.

Screenshots of those runs are in [docs/design.md](docs/design.md#7-notifier-one-github-issue-per-problem).
The live runs predate v2.0.0 of synthetic-payer-files, when the MA payer was still branded
"Humana", so issue titles and screenshots from them use that name. The renamed code has been
checked against the v2.0.0 answer keys locally, with the full test suite.

## Testing

`pytest` on a local Spark session; no Azure access needed. A local harness stands in for the
Function, the archive and Auto Loader, and for a job run of both payer pipelines plus gold.
- **Answer keys:** 36 generated months for both payers, matched exactly in silver (members,
  member changes, claim actions, net paid) and gold (net paid, the 17 payer switchers,
  member months).
- **Load order and re-sends:** random order, uneven batches and corrected re-sends all give
  the same silver and gold as a clean, in-order load.
- **Gating:** warns don't block; a stop holds back its payer, keeps blocking later months,
  and a re-send recovers it.
- **Every expectation** passes on a real row and fails on a copy broken to break it.

The fast subset runs in CI on every pull request. Details: [docs/design.md](docs/design.md#testing-in-detail).

## Run it

Requires Python 3.12 and Java 17 (for local Spark).

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\pytest -m "not slow"   # fast subset, as in CI
.venv\Scripts\pytest                 # everything, including the 36-month answer-key tests
```

Terraform, Function deploys, the GitHub deploy workflow and the Databricks bundle:
[docs/setup.md](docs/setup.md).

## Adapting it for an enterprise

The sibling repo's
[enterprise table](https://github.com/fredr171/claims-lakehouse-dbt-duckdb#adapting-it-for-an-enterprise)
applies here too. On this platform, the main changes would be VNet injection instead of public
cluster IPs, a locked WORM policy and account-level groups for the PII masks.

## How it was built

I designed this project and built it with [Claude Code](https://claude.com/claude-code) as an
AI pair programmer: I set the requirements and the design, made the decisions, and reviewed
the work, and Claude Code wrote much of the implementation. The design document records each
decision with its reasoning, and the answer-key tests and live runs verify the result.

## License

Released under the [MIT License](LICENSE).
