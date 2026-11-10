-- One row per PERSON across all payers, keyed by MBI (the identity Medicare assigns and every
-- payer delivers). A beneficiary who moves from FFS to Medicare Advantage is one member with
-- spans at both payers.
--
-- Attributes come from the most recently delivered span (any payer). current_payer_id is the
-- payer of the latest open span at an active payer; spans at a retired payer are history,
-- not current coverage. No names, addresses or full birth dates: those stay in silver.

WITH coverage AS (
    SELECT * FROM ${coverage}
),

latest_attributes AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY beneficiary_mbi
                ORDER BY last_delivery_month DESC, coverage_start_date DESC, payer_id
            ) AS attribute_rank
        FROM coverage
    )
    WHERE attribute_rank = 1
),

current_coverage AS (
    SELECT beneficiary_mbi, payer_id, coverage_start_date
    FROM (
        SELECT
            coverage.beneficiary_mbi,
            coverage.payer_id,
            coverage.coverage_start_date,
            row_number() OVER (
                PARTITION BY coverage.beneficiary_mbi
                ORDER BY coverage.coverage_start_date DESC, coverage.payer_id
            ) AS current_rank
        FROM coverage
        JOIN ${dim_payer} AS payers ON payers.payer_id = coverage.payer_id
        WHERE coverage.coverage_end_date IS NULL AND payers.payer_status = 'active'
    )
    WHERE current_rank = 1
),

history AS (
    SELECT
        beneficiary_mbi,
        count(DISTINCT payer_id)    AS payer_count,
        count(*)                    AS coverage_span_count,
        min(coverage_start_date)    AS first_coverage_start_date
    FROM coverage
    GROUP BY beneficiary_mbi
)

SELECT
    latest_attributes.beneficiary_mbi           AS member_key,
    latest_attributes.beneficiary_mbi,
    latest_attributes.birth_year,
    latest_attributes.sex,
    latest_attributes.state_code,
    latest_attributes.is_dual,
    latest_attributes.is_deceased,
    current_coverage.payer_id                   AS current_payer_id,
    current_coverage.coverage_start_date        AS current_coverage_start_date,
    history.payer_count,
    history.payer_count > 1                     AS is_multi_payer,
    history.coverage_span_count,
    history.first_coverage_start_date
FROM latest_attributes
JOIN history ON history.beneficiary_mbi = latest_attributes.beneficiary_mbi
LEFT JOIN current_coverage ON current_coverage.beneficiary_mbi = latest_attributes.beneficiary_mbi
