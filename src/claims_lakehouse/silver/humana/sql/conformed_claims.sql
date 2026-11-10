-- Humana MA claims in the conformed contract (claims_lakehouse.gold.contract): one row per
-- claim family. Gold unions it with every registered payer's.
--
-- Mapping: claim form I -> INSTITUTIONAL, P -> PROFESSIONAL. Status ORIGINAL -> PAID,
-- REPLACED -> ADJUSTED, VOIDED -> VOIDED, DENIED -> DENIED. Capitated encounters keep
-- is_capitated (they pay nothing per claim).

SELECT
    CAST('${payer_id}' AS STRING)                                   AS payer_id,
    CAST(claim_family_key AS STRING)                                AS payer_claim_id,
    CAST(member_id AS STRING)                                       AS payer_member_id,
    CAST(beneficiary_mbi AS STRING)                                 AS beneficiary_mbi,
    CAST(
        CASE claim_form_type_code WHEN 'I' THEN 'INSTITUTIONAL' WHEN 'P' THEN 'PROFESSIONAL' END
        AS STRING
    )                                                               AS claim_category,
    CAST(family_status AS STRING)                                   AS payer_claim_status,
    CAST(
        CASE family_status
            WHEN 'ORIGINAL' THEN 'PAID'
            WHEN 'REPLACED' THEN 'ADJUSTED'
            WHEN 'VOIDED' THEN 'VOIDED'
            WHEN 'DENIED' THEN 'DENIED'
        END AS STRING
    )                                                               AS claim_status,
    CAST(service_start_date AS DATE)                                AS service_start_date,
    CAST(to_date(concat(service_month, '-01')) AS DATE)             AS service_month,
    CAST(to_date(concat(first_delivery_month, '-01')) AS DATE)      AS first_delivery_month,
    CAST(to_date(concat(latest_delivery_month, '-01')) AS DATE)     AS latest_delivery_month,
    CAST(net_paid_amount AS DECIMAL(18, 2))                         AS net_paid_amount,
    CAST(is_capitated AS BOOLEAN)                                   AS is_capitated
FROM claim_families
