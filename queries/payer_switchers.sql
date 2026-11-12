-- Members covered by more than one payer (for example, Medicare FFS to Medicare Advantage),
-- with the payer covering them now. One row per person, keyed by MBI.
SELECT
    member_key,
    payer_count,
    coverage_span_count,
    first_coverage_start_date,
    current_payer_id,
    current_coverage_start_date
FROM claims_lakehouse_dev.gold.dim_member
WHERE is_multi_payer
ORDER BY member_key
