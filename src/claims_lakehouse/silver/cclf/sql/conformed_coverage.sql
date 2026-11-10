-- CMS Medicare FFS coverage in the conformed contract (claims_lakehouse.gold.contract):
-- one row per coverage span of a beneficiary. Gold unions it with every registered payer's.
--
-- A span is a coverage start date (BENE_RNG_BGN_DT); its end date and attributes come from
-- the latest member-history version with that start. A beneficiary who leaves FFS and
-- returns has two spans. Coverage ends at death if that comes first.
-- Code mapping: sex 1/2 -> M/F (else U); dual status NA -> not dual. FFS has no plan.

WITH spans AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY beneficiary_mbi, coverage_start_date
            ORDER BY valid_from DESC
        ) AS span_rank
    FROM beneficiary_versions
    WHERE coverage_start_date IS NOT NULL
)

SELECT
    CAST('${payer_id}' AS STRING)                                   AS payer_id,
    CAST(beneficiary_mbi AS STRING)                                 AS payer_member_id,
    CAST(beneficiary_mbi AS STRING)                                 AS beneficiary_mbi,
    CAST(coverage_start_date AS DATE)                               AS coverage_start_date,
    CAST(
        CASE
            WHEN death_date IS NOT NULL
                AND (coverage_end_date IS NULL OR death_date < coverage_end_date)
                THEN death_date
            ELSE coverage_end_date
        END AS DATE
    )                                                               AS coverage_end_date,
    CAST('MEDICARE_FFS' AS STRING)                                  AS plan_code,
    CAST(year(birth_date) AS INT)                                   AS birth_year,
    CAST(CASE sex_code WHEN '1' THEN 'M' WHEN '2' THEN 'F' ELSE 'U' END AS STRING) AS sex,
    CAST(usps_state_code AS STRING)                                 AS state_code,
    CAST(coalesce(dual_status_code, 'NA') <> 'NA' AS BOOLEAN)       AS is_dual,
    CAST(death_date IS NOT NULL AS BOOLEAN)                         AS is_deceased,
    CAST(to_date(concat(last_delivery_month, '-01')) AS DATE)       AS last_delivery_month
FROM spans
WHERE span_rank = 1
