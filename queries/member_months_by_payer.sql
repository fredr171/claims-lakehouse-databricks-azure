-- Covered members per month, by payer (one member month = covered on any day of the month).
SELECT
    payer_id,
    month,
    count(*)                    AS member_months,
    count(DISTINCT member_key)  AS members
FROM claims_lakehouse_dev.gold.fct_member_months
GROUP BY payer_id, month
ORDER BY payer_id, month
