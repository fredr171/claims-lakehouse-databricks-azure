-- STOP check: the gold summary must add up to the gold facts, per payer: net paid and
-- non-voided claim counts (agg_payer_monthly vs fct_claims) and member months (vs
-- fct_member_months). Any row returned means a join or filter lost or duplicated something,
-- and the marts must not be published.

WITH summary AS (
    SELECT
        payer_id,
        sum(net_paid_amount)    AS net_paid_amount,
        sum(claim_count)        AS claim_count,
        sum(member_months)      AS member_months
    FROM ${agg_payer_monthly}
    GROUP BY payer_id
),

claims AS (
    SELECT
        payer_id,
        sum(net_paid_amount)                                AS net_paid_amount,
        count(*) FILTER (WHERE claim_status <> 'VOIDED')    AS claim_count
    FROM ${fct_claims}
    GROUP BY payer_id
),

member_months AS (
    SELECT payer_id, count(*) AS member_months
    FROM ${fct_member_months}
    GROUP BY payer_id
)

SELECT
    coalesce(summary.payer_id, claims.payer_id, member_months.payer_id) AS payer_id,
    summary.net_paid_amount         AS summary_net_paid,
    claims.net_paid_amount          AS fact_net_paid,
    summary.claim_count             AS summary_claims,
    claims.claim_count              AS fact_claims,
    summary.member_months           AS summary_member_months,
    member_months.member_months     AS fact_member_months
FROM summary
FULL OUTER JOIN claims ON claims.payer_id = summary.payer_id
FULL OUTER JOIN member_months
    ON member_months.payer_id = coalesce(summary.payer_id, claims.payer_id)
WHERE summary.net_paid_amount IS DISTINCT FROM coalesce(claims.net_paid_amount, 0)
    OR summary.claim_count IS DISTINCT FROM coalesce(claims.claim_count, 0)
    OR summary.member_months IS DISTINCT FROM coalesce(member_months.member_months, 0)
