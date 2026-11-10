-- One row per member per payer per month of coverage: the denominator for per-member-per-
-- month (PMPM) metrics.
--
-- A month counts if the member was covered on any day of it. Months are bounded by the
-- payer's delivery window (dim_payer first/last_delivery_month): before it nothing is known,
-- after it (a retired payer, or data not delivered yet) coverage is not assumed. Overlapping
-- spans of one member at one payer count once.
--
-- When a member changes plans (or payer member ID) within a month at the same payer, the
-- month goes to max(plan_code) / max(payer_member_id): simple and deterministic, not "the
-- plan held longest". A business rule would replace it if plan-level PMPM mattered.

WITH windows AS (
    SELECT payer_id, first_delivery_month, last_delivery_month
    FROM ${dim_payer}
    WHERE last_delivery_month IS NOT NULL
),

bounded AS (
    SELECT
        coverage.payer_id,
        coverage.beneficiary_mbi AS member_key,
        coverage.payer_member_id,
        coverage.plan_code,
        greatest(trunc(coverage.coverage_start_date, 'MM'), windows.first_delivery_month)
            AS from_month,
        least(
            trunc(coalesce(coverage.coverage_end_date, windows.last_delivery_month), 'MM'),
            windows.last_delivery_month
        ) AS to_month
    FROM ${coverage} AS coverage
    JOIN windows ON windows.payer_id = coverage.payer_id
),

months AS (
    SELECT
        payer_id,
        member_key,
        payer_member_id,
        plan_code,
        explode(sequence(from_month, to_month, INTERVAL 1 MONTH)) AS month
    FROM bounded
    WHERE from_month <= to_month
)

SELECT
    payer_id,
    member_key,
    month,
    max(payer_member_id) AS payer_member_id,
    max(plan_code) AS plan_code
FROM months
GROUP BY payer_id, member_key, month
