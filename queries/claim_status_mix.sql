-- Claims and net paid by conformed status, per payer. Payer codes (CANCELLED vs VOIDED) are
-- mapped in each payer's conformed views, so the statuses compare across payers.
SELECT
    payer_id,
    claim_status,
    count(*)                AS claims,
    sum(net_paid_amount)    AS net_paid_amount
FROM claims_lakehouse_dev.gold.fct_claims
GROUP BY payer_id, claim_status
ORDER BY payer_id, claim_status
