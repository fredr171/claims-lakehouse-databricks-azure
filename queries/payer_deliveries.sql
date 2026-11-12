-- Each payer from the registry: status, and the window of months it has delivered.
SELECT
    payer_id,
    payer_name,
    line_of_business,
    payer_status,
    first_delivery_month,
    last_delivery_month,
    current_file_count
FROM claims_lakehouse_dev.gold.dim_payer
ORDER BY payer_id
