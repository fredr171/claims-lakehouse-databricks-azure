-- MA payer coverage in the conformed contract (claims_lakehouse.gold.contract): one row
-- per enrollment span of a member. Gold unions it with every registered payer's.
--
-- A span is an enrollment start date of a member; its end date (termination) and
-- attributes come from the latest member-history version with that start. Coverage ends at
-- death if that comes first.
-- Code mapping: gender M/F kept (else U); DUAL_FLAG Y -> dual; plan = contract-PBP.

WITH spans AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY member_id, enrollment_start_date
            ORDER BY valid_from DESC
        ) AS span_rank
    FROM member_versions
    WHERE enrollment_start_date IS NOT NULL
)

SELECT
    CAST('${payer_id}' AS STRING)                                   AS payer_id,
    CAST(member_id AS STRING)                                       AS payer_member_id,
    CAST(beneficiary_mbi AS STRING)                                 AS beneficiary_mbi,
    CAST(enrollment_start_date AS DATE)                             AS coverage_start_date,
    CAST(
        CASE
            WHEN death_date IS NOT NULL
                AND (enrollment_end_date IS NULL OR death_date < enrollment_end_date)
                THEN death_date
            ELSE enrollment_end_date
        END AS DATE
    )                                                               AS coverage_end_date,
    CAST(concat(contract_id, '-', plan_benefit_package_id) AS STRING) AS plan_code,
    CAST(year(birth_date) AS INT)                                   AS birth_year,
    CAST(CASE WHEN gender_code IN ('M', 'F') THEN gender_code ELSE 'U' END AS STRING) AS sex,
    CAST(usps_state_code AS STRING)                                 AS state_code,
    CAST(coalesce(dual_flag = 'Y', false) AS BOOLEAN)               AS is_dual,
    CAST(death_date IS NOT NULL AS BOOLEAN)                         AS is_deceased,
    CAST(to_date(concat(last_delivery_month, '-01')) AS DATE)       AS last_delivery_month
FROM spans
WHERE span_rank = 1
