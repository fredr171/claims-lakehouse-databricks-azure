-- MA payer current deliveries in the conformed contract (claims_lakehouse.gold.contract):
-- one row per file type per delivery month. dim_payer takes the payer's delivery window
-- from it.

SELECT
    CAST('${payer_id}' AS STRING) AS payer_id,
    CAST(_file_type AS STRING) AS file_type,
    CAST(to_date(concat(_delivery_month, '-01')) AS DATE) AS delivery_month,
    CAST(_checksum AS STRING) AS checksum
FROM (
    SELECT * FROM member_eligibility_deliveries
    UNION ALL SELECT * FROM medical_claim_lines_deliveries
)
WHERE is_current
