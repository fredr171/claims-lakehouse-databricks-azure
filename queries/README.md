# Saved queries

Queries over the gold marts, in Databricks SQL. The AI/BI dashboard
(`resources/dashboards/claims_lakehouse.lvdash.json`) is built from the same queries.

They name the dev catalog, `claims_lakehouse_dev`. The local tests run each one against gold
built from the 36 generated months (`tests/test_gold_answer_keys.py`), so they are known to
work and to agree with the answer keys.

| Query | Answers |
|---|---|
| `pmpm_by_payer_month.sql` | Net paid per member month (PMPM), by payer and service month |
| `member_months_by_payer.sql` | Covered members per month, by payer |
| `payer_switchers.sql` | Members covered by more than one payer, and where they are now |
| `claim_status_mix.sql` | Claims and net paid by conformed status, per payer |
| `payer_deliveries.sql` | Each payer's status and delivery window, from the registry |
