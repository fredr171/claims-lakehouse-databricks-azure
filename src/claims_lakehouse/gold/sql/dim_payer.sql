-- One row per registered payer (config/payers.yml), with its delivery window.
--
-- A payer appears as soon as it is registered (awaiting_data until its first delivery
-- reaches silver) and stays after it is retired: retirement ends its window, never its
-- history. first/last_delivery_month bound fct_member_months.

WITH deliveries AS (
    SELECT
        payer_id,
        min(delivery_month) AS first_delivery_month,
        max(delivery_month) AS last_delivery_month,
        count(*)            AS current_file_count
    FROM ${deliveries}
    GROUP BY payer_id
)

SELECT
    registry.payer_id,
    registry.payer_name,
    registry.line_of_business,
    registry.feed,
    registry.onboarded_month,
    registry.retired_month,
    CASE
        WHEN registry.retired_month IS NOT NULL THEN 'retired'
        WHEN deliveries.last_delivery_month IS NULL THEN 'awaiting_data'
        ELSE 'active'
    END                                         AS payer_status,
    deliveries.first_delivery_month,
    deliveries.last_delivery_month,
    coalesce(deliveries.current_file_count, 0)  AS current_file_count
FROM ${payer_registry} AS registry
LEFT JOIN deliveries ON deliveries.payer_id = registry.payer_id
