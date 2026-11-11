-- CMS Medicare FFS claims in the conformed contract (claims_lakehouse.gold.contract):
-- one row per claim family. Gold unions it with every registered payer's.
--
-- Mapping: Part A -> INSTITUTIONAL, Part B -> PROFESSIONAL. Status ORIGINAL -> PAID,
-- ADJUSTED -> ADJUSTED, CANCELLED -> VOIDED (CCLF has no denied claims). FFS has no
-- capitation.

SELECT
    CAST('${payer_id}' AS STRING)                                   AS payer_id,
    CAST(claim_family_key AS STRING)                                AS payer_claim_id,
    CAST(beneficiary_mbi AS STRING)                                 AS payer_member_id,
    CAST(beneficiary_mbi AS STRING)                                 AS beneficiary_mbi,
    CAST(CASE part WHEN 'A' THEN 'INSTITUTIONAL' WHEN 'B' THEN 'PROFESSIONAL' END AS STRING)
                                                                    AS claim_category,
    CAST(family_status AS STRING)                                   AS payer_claim_status,
    CAST(
        CASE family_status
            WHEN 'ORIGINAL' THEN 'PAID'
            WHEN 'ADJUSTED' THEN 'ADJUSTED'
            WHEN 'CANCELLED' THEN 'VOIDED'
        END AS STRING
    )                                                               AS claim_status,
    CAST(service_start_date AS DATE)                                AS service_start_date,
    CAST(to_date(concat(service_month, '-01')) AS DATE)             AS service_month,
    CAST(to_date(concat(first_delivery_month, '-01')) AS DATE)      AS first_delivery_month,
    CAST(to_date(concat(latest_delivery_month, '-01')) AS DATE)     AS latest_delivery_month,
    CAST(net_paid_amount AS DECIMAL(18, 2))                         AS net_paid_amount,
    CAST(false AS BOOLEAN)                                          AS is_capitated
FROM claim_families
