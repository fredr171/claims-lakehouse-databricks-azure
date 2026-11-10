-- One row per claim family across all payers: the claim as it stands today, after every
-- reversal, cancellation, adjustment and replacement (netting happened in silver).
-- claim_key is unique across payers; member_key joins dim_member.

SELECT
    concat(payer_id, ':', payer_claim_id)   AS claim_key,
    payer_id,
    payer_claim_id,
    beneficiary_mbi                         AS member_key,
    payer_member_id,
    claim_category,
    claim_status,
    payer_claim_status,
    service_start_date,
    service_month,
    first_delivery_month,
    latest_delivery_month,
    net_paid_amount,
    is_capitated
FROM ${claims}
