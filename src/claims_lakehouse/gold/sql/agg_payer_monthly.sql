-- One row per payer per month: members, claims and net paid by SERVICE month, and PMPM
-- (net paid per member month). Built only from the other marts.
--
-- Voided claims are left out of claim_count, but their net (zero) is still summed, so
-- net_paid_amount equals fct_claims in total. PMPM is NULL where there are no member months
-- (service months before the payer's delivery window).

WITH member_months AS (
    SELECT payer_id, month, count(*) AS member_months
    FROM ${fct_member_months}
    GROUP BY payer_id, month
),

claims AS (
    SELECT
        payer_id,
        service_month                                       AS month,
        count(*) FILTER (WHERE claim_status <> 'VOIDED')    AS claim_count,
        sum(net_paid_amount)                                AS net_paid_amount
    FROM ${fct_claims}
    GROUP BY payer_id, service_month
)

SELECT
    coalesce(member_months.payer_id, claims.payer_id)           AS payer_id,
    coalesce(member_months.month, claims.month)                 AS month,
    coalesce(member_months.member_months, 0)                    AS member_months,
    coalesce(claims.claim_count, 0)                             AS claim_count,
    CAST(coalesce(claims.net_paid_amount, 0) AS DECIMAL(18, 2)) AS net_paid_amount,
    CAST(
        coalesce(claims.net_paid_amount, 0) / nullif(member_months.member_months, 0)
        AS DECIMAL(18, 2)
    )                                                           AS pmpm_net_paid
FROM member_months
FULL OUTER JOIN claims
    ON claims.payer_id = member_months.payer_id
    AND claims.month = member_months.month
