# CLAUDE.md

Guidance for Claude Code in this repository. Read it fully before starting any task.

## What this project is

An event-driven claims lakehouse on Azure and Databricks, following a medallion architecture
(landing → bronze → silver → gold). It processes monthly healthcare claims deliveries from two
payers: CMS CCLF (Medicare fee-for-service) and Humana Medicare Advantage.

It is the sibling of `fredr171/claims-lakehouse-dbt-duckdb` (AWS, Lambda, dbt-duckdb). Both
repos use the same synthetic data from `fredr171/synthetic-payer-files`, and both must produce
the same results as that generator's **answer keys**. The goal is to show the same design
decisions implemented with a different, platform-native stack. Where this repo differs from the
sibling, it should be a deliberate choice, explained in the README.

This is a **public portfolio repo** viewed by potential employers. Code, commit history, tests
and the README should be clean and easy to follow.

## Hard rules

- **Never run anything that creates, changes or destroys billable cloud resources without asking
  first.** This includes `terraform apply`, `terraform destroy`, `databricks bundle deploy`,
  starting clusters, warehouses, jobs or pipelines, and `az` commands that create resources.
  `terraform plan`, `terraform validate`, `databricks bundle validate`, and read-only `az`
  commands are fine.
- **No IDs or secrets in the repo.** Subscription ID, tenant ID, workspace URLs, tokens and keys
  come from environment variables, a git-ignored `*.tfvars` / `.env` file, or Key Vault.
  Provide `.example` files for anything a user must fill in.
- **Synthetic data only.** Never add real data. GitHub issues created by the notifier contain only
  identifiers and counts, never data values.
- **Prove checks can fail.** Every new data quality check or validation must first be shown to
  fail on deliberately broken data, with a test, before it is trusted.

## Environment

- Developer machine: Windows, PowerShell. Write commands and scripts that work there.
- Azure CLI is logged in (`az login --tenant <tenant>`, MFA required). Read the subscription and
  tenant from `az account show` or environment variables.
- Region: **Central US** (`centralus`) for every resource.
- Tag every Azure resource with `project = claims-lakehouse-azure`.
- Naming: `rg-claims-lakehouse-dev`, `stclaimslakehouse<suffix>`, and so on, with a `dev`
  environment suffix. Keep names consistent and defined once in Terraform locals.
- Budget: about $25 per month. Prefer the cheapest option that still demonstrates the design.
- Compute quota is small (10 regional vCPUs, DSv3 family only). **Use serverless compute by
  default** for jobs, pipelines and SQL. When classic compute is unavoidable, use
  `Standard_DS3_v2`, single-node or one worker, with auto-termination.
- Infrastructure is torn down between live runs with `infra/teardown.ps1` (`terraform destroy`
  plus the Databricks managed resource group Azure leaves behind). It must always work cleanly.

## Architecture

| Layer | Where | What it holds |
|---|---|---|
| Landing | ADLS Gen2 container `landing` | CSVs exactly as delivered |
| Archive | ADLS Gen2 container `archive` | Validated files, checksum-keyed, immutable. The audit record |
| Quarantine | ADLS Gen2 container `quarantine` | Rejected files plus an error report |
| Bronze | Delta tables (Unity Catalog) | Every validated delivery, all columns as strings, plus lineage columns |
| Silver | Delta tables / views (Lakeflow Declarative Pipelines) | Typed, renamed, current deliveries; member history (SCD2); claim families with net paid |
| Gold | Delta tables (Unity Catalog) | Payer-agnostic marts |

Flow: a file lands in `landing` → Event Grid → storage queue → **validation Azure Function**
(Flex Consumption, queue trigger) → valid files are
copied to `archive`, invalid files go to `quarantine` → a Lakeflow Job (file-arrival trigger on
the archive) runs Auto Loader into bronze, then the per-payer pipelines, then the answer-key
check → job outcomes go to the **notifier Azure Function** → GitHub issues.

## Design decisions (do not change without discussing)

### Ingestion and validation
- **File-level validation happens before Auto Loader**, in a Python Azure Function. Checks:
  file name, size, encoding, strict CSV parsing, exact header, field counts, non-empty keys.
  Auto Loader is row-oriented and cannot reject a whole file, so it never sees unvalidated files.
- A failed file goes to `quarantine` with an error report, and processing of other files continues.
- Validation rules are config-driven: one YAML file per file type, one prefix per payer.
- The Function computes a SHA-256 of each file and copies valid files to
  `archive/<feed>/<file_type>/delivery_month=<m>/checksum=<sha256>/<filename>`.
  A duplicate checksum is skipped.
- **Checksum-keyed paths are required for correctness**, not just tidiness: Auto Loader tracks
  files by path and does not reprocess a path it has seen, so a corrected re-send under the
  same name would otherwise be silently skipped.
- The archive container has a **container-level time-based immutability (WORM) policy**. The lake
  is hierarchical-namespace (Unity Catalog needs it), and those accounts support neither blob
  versioning nor version-level WORM. The policy stays unlocked so `terraform destroy` works;
  production would lock it. Nothing may rename, move or delete archive files (no Auto Loader
  `cleanSource`). **The archive files are the immutable record**; bronze Delta tables are a
  queryable copy, not the audit trail.

### Bronze
- Auto Loader reads the archive with every column as a string (codes keep leading zeros) and
  adds lineage columns from `_metadata` (file path, checksum, delivery month, archive time
  from the file's modification time, ingest timestamp).
- Append-only. No updates or deletes.

### Silver
- One Lakeflow Declarative Pipeline **per payer**, so a failure in one payer never blocks the
  other. Silver logic is written in **Python (PySpark)**, with transformation functions kept in
  plain importable modules so they can be unit tested locally.
- Staging: readable column names shared across payers, empty string → NULL, typed columns;
  codes and IDs stay strings.
- A **current-delivery index** picks the latest delivery of each file, ordered by archive time
  (`_archived_at`), then checksum; silver reads only current deliveries. Not ingest time: Auto
  Loader can ingest an original and its re-send in one micro-batch, giving both the same time.
- **Stop vs warn gating:** row-level expectations use warn (`expect`), drop (`expect_or_drop`),
  or stop (`expect_or_fail`). The aggregate rule (an amount rule escalates to stop when more
  than 1% of a month's lines break it) is **not** a row-level expectation: implement it as a
  per-month quality table with a fail expectation on the error ratio, or as a job task.
  A stop must leave previously published silver and gold intact: every published table of a
  payer depends on that payer's gate tables, so a failed expectation blocks the whole payer.
  Row expectations check formats on the delivered text, before typing (`try_cast` is lenient).
  Nothing retries a stop: the silver pipelines set `pipelines.numUpdateRetryAttempts: 0` and
  `report_outcome` sets `max_retries: 0` (the live run billed four retries of one stop).
- **Member history (SCD type 2)** uses AUTO CDC with `SEQUENCE BY` delivery month plus archive
  time (`_archived_at`) as a tiebreaker. Change type (new, update, termination) is derived, not trusted
  from the file.
- **Supersession:** AUTO CDC handles out-of-order arrivals but does not retract changes from a
  delivery that a corrected re-send replaced. This must be handled explicitly (for example,
  feeding AUTO CDC only from current deliveries and running a full refresh of the affected flow
  when a supersession occurs). The chosen approach must pass the answer-key re-send tests.
  Document the decision in the README.
  **Decided:** AUTO CDC reads current deliveries only, and a job task full-refreshes the
  payer pipeline when bronze holds more superseded member-file deliveries than the pipeline
  last published (a pipeline task cannot full-refresh a single flow).
- **Claim families** (an original plus later reversals or replacements) and net paid are
  materialized views over current deliveries. Status comes from which versions are active,
  never from file order.

### Gold
- Written in SQL. A **payer registry** (one config list) is the only place gold learns about
  payers. Each payer supplies conformed views (coverage spans, claim families, and its current
  deliveries for the delivery window) with identical columns and types, enforced by a contract
  check; payer code mappings (sex `1`/`2` vs `M`/`F`, `CANCELLED` vs `VOIDED`) happen in the
  conformed views, never in the marts. Mart SQL names its inputs with `${...}` placeholders
  that a thin renderer fills from the registry, so the same SQL runs locally and on Databricks.
- Marts: `dim_payer`, `dim_member` (one row per person across payers, keyed by MBI),
  `fct_claims`, `fct_member_months`, `agg_payer_monthly` (PMPM).
- Definitions: a member month = covered any day of the month; PMPM = net paid by service month.
- No names or addresses in gold. Names, addresses and birth dates in silver are protected with
  **Unity Catalog column masks**.
- Payer lifecycle by registry only: onboard (conformed views + registry entry), retire
  (`retired_month`), offboard (remove the entry).

### Orchestration
- A Lakeflow Job with a file-arrival trigger, max one concurrent run per payer feed.
- The trigger reads Unity Catalog file events from a **queue Terraform provides** (Event Grid
  subscription on the archive container → lake storage queue). Not Databricks' automatic
  mode: it needs Storage Account Contributor and did not finish provisioning on a lake with
  shared-key access disabled.
- Final job task: compare gold against the answer keys and fail the run on mismatch.

### Notifier
- A Python Azure Function receives **every** job outcome (passed, warned, stopped, error) and
  Azure Monitor alerts, and manages GitHub issues: open one issue per problem, comment while
  it persists, close it on recovery. Key issues by the feed the **failing check** reads, not
  every feed the run built.
- GitHub credentials (GitHub App or fine-grained token, issues scope on this repo only) live
  in Key Vault. **Decided:** a fine-grained token, read through a Key Vault reference.
- **Decided:** the job's last task (`report_outcome`, `run_if: ALL_DONE`) classifies the run
  per feed and POSTs it to the notifier (URL with function key in a Databricks secret scope
  only the runner reads), then fails the run on stopped or error. Alerts come through an
  action group (common alert schema). Issues are keyed `source/feed/check` in a hidden body
  marker; a report closes only issues for checks it ran to completion.

### Infrastructure and deployment
- **Terraform** owns infrastructure: resource group, storage, Event Grid, Functions, Key Vault,
  Databricks workspace, Unity Catalog external locations, storage credentials, catalogs,
  schemas and grants. Remote state in a separate storage account created by a bootstrap script.
- **Databricks Asset Bundles** own code-level resources: jobs, pipelines, notebooks/modules.
- CI: GitHub Actions running ruff and the fast tests on every PR. Deployment uses OIDC
  (workload identity federation), never stored Azure credentials.

## Testing

- `pytest` with a local Spark session; no Azure access needed for unit tests.
- Every validation check is tested against deliberately broken fixture files (BOM, ragged rows,
  bad encoding, missing keys, and so on).
- Answer-key tests: generate the 36 months for both payers, run the transformations, and match
  current members, member changes, claim actions, net paid by service month, the 17 payer
  switchers, and member months exactly. Loading months in uneven batches, out of order, or
  re-sending old months must give the same results as a full load.
- Gating tests: a stop blocks downstream and keeps blocking later months until a re-send
  recovers it; a warn does not block; a stop in one payer still refreshes the other payer.
- Mark slow tests so a fast subset runs in CI in about a minute.

## Build phases

Finish and test each phase before starting the next. Stop and ask at the end of each phase.

1. **Local foundation.** Repo scaffold, config, validation logic, silver and gold
   transformations as plain Python, pytest against the answer keys. No Azure.
2. **Infrastructure.** Remote state bootstrap, storage, Event Grid, validation Function,
   Key Vault, Databricks workspace. Pause for the one-time Databricks account console sign-in.
3. **Databricks.** Unity Catalog objects, Asset Bundles, Auto Loader, per-payer pipelines,
   gold, the job, and a live run against generated data.
4. **Operations and presentation.** Notifier and GitHub issues, CI with OIDC, AI/BI dashboard
   and saved queries in `queries/`, README with architecture diagram and screenshots.

## Conventions

- Match the Python version of the Databricks serverless environment for pipeline code; use ruff
  for linting and formatting.
- Small, focused commits with clear messages.
- Update the README and this file when a design decision changes.
