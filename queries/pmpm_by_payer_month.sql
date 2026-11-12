-- Net paid per member month (PMPM), by payer and service month.
-- A member month is a member covered on any day of the month; net paid is by service month.
SELECT
    payer_id,
    month,
    member_months,
    claim_count,
    net_paid_amount,
    pmpm_net_paid
FROM claims_lakehouse_dev.gold.agg_payer_monthly
WHERE member_months > 0
ORDER BY payer_id, month
