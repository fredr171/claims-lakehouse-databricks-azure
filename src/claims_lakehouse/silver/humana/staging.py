"""MA payer staging: readable names and types for HUM_ELIG and HUM_MEDCLM.

Input is bronze rows from current deliveries only (claims_lakehouse.silver.deliveries).
No rows are filtered or de-duplicated here. Names follow the sibling repo
(claims-lakehouse-dbt-duckdb) and match CCLF's where the meaning is the same
(beneficiary_mbi, claim_start_date, paid_amount, ...).

HUM_ELIG holds synthetic names, addresses and birth dates: Unity Catalog column masks
protect them in silver, and gold never selects them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from claims_lakehouse.silver.common import AMOUNT, DATE, INTEGER, QUANTITY, STRING, stage

MEMBER_ELIGIBILITY = [
    # identifiers
    ("member_id", "MEMBER_ID", STRING),
    ("beneficiary_mbi", "MBI", STRING),
    # demographics (synthetic)
    ("first_name", "FIRST_NAME", STRING),
    ("middle_initial", "MIDDLE_INITIAL", STRING),
    ("last_name", "LAST_NAME", STRING),
    ("birth_date", "DOB", DATE),
    ("gender_code", "GENDER", STRING),  # M/F; mapped to a shared code in the conformed view
    ("death_date", "DEATH_DT", DATE),
    # address (synthetic)
    ("address_line_1", "ADDRESS_LINE_1", STRING),
    ("city", "CITY", STRING),
    ("usps_state_code", "STATE", STRING),
    ("zip5_code", "ZIP", STRING),
    ("county_fips_code", "COUNTY_FIPS", STRING),
    # plan
    ("contract_id", "CONTRACT_ID", STRING),
    ("plan_benefit_package_id", "PBP_ID", STRING),
    ("plan_name", "PLAN_NAME", STRING),
    ("product_type", "PRODUCT_TYPE", STRING),
    ("enrollment_start_date", "ENROLLMENT_START_DT", DATE),
    ("enrollment_end_date", "ENROLLMENT_END_DT", DATE),
    # attributed primary care provider
    ("pcp_npi", "PCP_NPI", STRING),
    ("pcp_tin", "PCP_TIN", STRING),
    ("pcp_attribution_start_date", "PCP_ATTRIBUTION_START_DT", DATE),
    # status flags (Y/N, as delivered)
    ("dual_flag", "DUAL_FLAG", STRING),
    ("low_income_subsidy_flag", "LIS_FLAG", STRING),
    ("esrd_flag", "ESRD_FLAG", STRING),
    ("hospice_flag", "HOSPICE_FLAG", STRING),
    # file
    ("file_as_of_date", "FILE_AS_OF_DT", DATE),
    # Synthetic helper from the generator, not a real field. Kept only so tests can check
    # the change type that member history derives itself.
    ("record_change_type", "RECORD_CHANGE_TYPE", STRING),
]

MEDICAL_CLAIM_LINES = [
    # identifiers
    ("claim_id", "CLAIM_ID", STRING),
    ("line_number", "CLAIM_LINE_NUM", INTEGER),
    ("original_claim_id", "ORIG_CLAIM_ID", STRING),
    ("member_id", "MEMBER_ID", STRING),
    ("beneficiary_mbi", "MBI", STRING),
    ("contract_id", "CONTRACT_ID", STRING),
    ("plan_benefit_package_id", "PBP_ID", STRING),
    # claim classification
    ("claim_status_code", "CLAIM_STATUS", STRING),  # P paid, D denied, R reversed
    ("claim_frequency_code", "CLAIM_FREQ_CD", STRING),  # 1 original, 7 replacement, 8 void
    ("claim_form_type_code", "CLAIM_FORM_TYPE", STRING),  # I institutional, P professional
    ("bill_type_code", "BILL_TYPE", STRING),
    # institutional stay
    ("admission_date", "ADMIT_DT", DATE),
    ("discharge_date", "DISCHARGE_DT", DATE),
    ("admission_type_code", "ADMIT_TYPE", STRING),
    ("admission_source_code", "ADMIT_SOURCE", STRING),
    ("patient_discharge_status_code", "DISCHARGE_STATUS", STRING),
    ("drg_code", "DRG_CD", STRING),
    # service line
    ("revenue_center_code", "REVENUE_CD", STRING),
    ("procedure_code", "PROCEDURE_CD", STRING),
    ("procedure_modifier_1_code", "MODIFIER_1", STRING),
    ("service_unit_quantity", "UNITS", QUANTITY),
    ("place_of_service_code", "PLACE_OF_SERVICE", STRING),
    # providers
    ("billing_npi", "BILLING_NPI", STRING),
    ("billing_tin", "BILLING_TIN", STRING),
    ("rendering_npi", "RENDERING_NPI", STRING),
    ("attending_npi", "ATTENDING_NPI", STRING),
    ("facility_ccn", "FACILITY_CCN", STRING),
    ("provider_specialty_code", "PROVIDER_SPECIALTY", STRING),
    ("in_network_flag", "IN_NETWORK_FLAG", STRING),
    ("capitated_flag", "CAPITATED_FLAG", STRING),
    # diagnoses (ICD-10-CM without the decimal point, as delivered; 1 = principal)
    ("icd_version_indicator", "ICD_VERSION", STRING),
    *[(f"diagnosis_{n}_code", f"DX_CODE_{n}", STRING) for n in range(1, 13)],
    # dates
    ("claim_start_date", "SERVICE_FROM_DT", DATE),
    ("claim_end_date", "SERVICE_TO_DT", DATE),
    ("line_start_date", "LINE_FROM_DT", DATE),
    ("line_end_date", "LINE_TO_DT", DATE),
    ("received_date", "RECEIVED_DT", DATE),
    ("processed_date", "PROCESSED_DT", DATE),
    ("paid_date", "PAID_DT", DATE),
    # amounts (negated on reversals)
    ("billed_amount", "BILLED_AMT", AMOUNT),
    ("allowed_amount", "ALLOWED_AMT", AMOUNT),
    ("paid_amount", "PAID_AMT", AMOUNT),
    ("copay_amount", "COPAY_AMT", AMOUNT),
    ("coinsurance_amount", "COINSURANCE_AMT", AMOUNT),
    ("deductible_amount", "DEDUCTIBLE_AMT", AMOUNT),
]

STAGING = {
    "member_eligibility": MEMBER_ELIGIBILITY,
    "medical_claim_lines": MEDICAL_CLAIM_LINES,
}


def stage_file(current_bronze: DataFrame, file_type: str) -> DataFrame:
    """Stage one MA payer file type from its current-delivery bronze rows."""
    return stage(current_bronze, STAGING[file_type])


# The member attributes whose change starts a new member-history version. FILE_AS_OF_DT
# moves every month without a real change, and the helper column is not tracked either.
MEMBER_TRACKED_COLUMNS = [
    target
    for target, _, _ in MEMBER_ELIGIBILITY
    if target not in ("member_id", "file_as_of_date", "record_change_type")
]
