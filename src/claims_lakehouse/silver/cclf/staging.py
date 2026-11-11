"""CCLF staging: readable names and types for the four CCLF files.

Input is bronze rows from current deliveries only (claims_lakehouse.silver.deliveries).
No rows are filtered or de-duplicated here: claim versions and beneficiary changes are
resolved downstream. Names follow the sibling repo (claims-lakehouse-dbt-duckdb).

CCLF8 holds synthetic names, addresses and birth dates: Unity Catalog column masks
protect them in silver, and gold never selects them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from claims_lakehouse.silver.common import AMOUNT, DATE, INTEGER, QUANTITY, STRING, stage

BENEFICIARY_DEMOGRAPHICS = [
    # identifiers
    ("beneficiary_mbi", "BENE_MBI_ID", STRING),
    ("beneficiary_hicn", "BENE_HIC_NUM", STRING),
    # demographics
    ("birth_date", "BENE_DOB", DATE),
    ("death_date", "BENE_DEATH_DT", DATE),
    ("age", "BENE_AGE", INTEGER),  # as of the delivery month
    ("sex_code", "BENE_SEX_CD", STRING),
    ("race_code", "BENE_RACE_CD", STRING),
    # name and address (synthetic)
    ("first_name", "BENE_1ST_NAME", STRING),
    ("middle_name", "BENE_MIDL_NAME", STRING),
    ("last_name", "BENE_LAST_NAME", STRING),
    ("address_line_1", "BENE_LINE_1_ADR", STRING),
    ("city", "GEO_ZIP_PLC_NAME", STRING),
    ("usps_state_code", "GEO_USPS_STATE_CD", STRING),
    ("zip5_code", "GEO_ZIP5_CD", STRING),
    ("zip9_code", "BENE_ZIP_CD", STRING),
    ("fips_state_code", "BENE_FIPS_STATE_CD", STRING),
    ("fips_county_code", "BENE_FIPS_CNTY_CD", STRING),
    # entitlement and enrollment
    ("medicare_status_code", "BENE_MDCR_STUS_CD", STRING),
    ("dual_status_code", "BENE_DUAL_STUS_CD", STRING),
    ("original_entitlement_reason_code", "BENE_ORGNL_ENTLMT_RSN_CD", STRING),
    ("entitlement_buyin_indicator", "BENE_ENTLMT_BUYIN_IND", STRING),
    ("part_a_enrollment_start_date", "BENE_PART_A_ENRLMT_BGN_DT", DATE),
    ("part_b_enrollment_start_date", "BENE_PART_B_ENRLMT_BGN_DT", DATE),
    ("coverage_start_date", "BENE_RNG_BGN_DT", DATE),
    ("coverage_end_date", "BENE_RNG_END_DT", DATE),
    # Synthetic helper from the generator, NOT a real CCLF field. Kept only so tests can
    # check the change type that member history derives itself.
    ("record_change_type", "RECORD_CHANGE_TYPE", STRING),
]

CLAIMS_HEADER = [
    # identifiers
    ("claim_id", "CUR_CLM_UNIQ_ID", STRING),
    ("claim_control_number", "CLM_CNTL_NUM", STRING),
    ("original_claim_control_number", "CLM_ORG_CNTL_NUM", STRING),
    ("beneficiary_mbi", "BENE_MBI_ID", STRING),
    ("beneficiary_hicn", "BENE_HIC_NUM", STRING),
    # providers
    ("facility_ccn", "PRVDR_OSCAR_NUM", STRING),
    ("facility_npi", "FAC_PRVDR_NPI_NUM", STRING),
    ("attending_npi", "ATNDG_PRVDR_NPI_NUM", STRING),
    ("facility_fips_state_code", "PRVDR_FAC_FIPS_ST_CD", STRING),
    # claim classification
    ("claim_type_code", "CLM_TYPE_CD", STRING),
    ("adjustment_type_code", "CLM_ADJSMT_TYPE_CD", STRING),  # 0 original, 1 cancel, 2 adjust
    ("bill_facility_type_code", "CLM_BILL_FAC_TYPE_CD", STRING),
    ("bill_classification_code", "CLM_BILL_CLSFCTN_CD", STRING),
    ("bill_frequency_code", "CLM_BILL_FREQ_CD", STRING),
    ("outpatient_service_type_code", "CLM_OP_SRVC_TYPE_CD", STRING),
    ("admission_type_code", "CLM_ADMSN_TYPE_CD", STRING),
    ("admission_source_code", "CLM_ADMSN_SRC_CD", STRING),
    ("patient_discharge_status_code", "BENE_PTNT_STUS_CD", STRING),
    ("drg_code", "DGNS_DRG_CD", STRING),
    ("medicare_nonpayment_reason_code", "CLM_MDCR_NPMT_RSN_CD", STRING),
    ("primary_payer_code", "CLM_NCH_PRMRY_PYR_CD", STRING),
    # diagnoses (ICD-10-CM without the decimal point, as delivered)
    ("principal_diagnosis_code", "PRNCPL_DGNS_CD", STRING),
    ("admitting_diagnosis_code", "ADMTG_DGNS_CD", STRING),
    ("icd_version_indicator", "DGNS_PRCDR_ICD_IND", STRING),
    # dates
    ("claim_start_date", "CLM_FROM_DT", DATE),
    ("claim_end_date", "CLM_THRU_DT", DATE),
    ("claim_effective_date", "CLM_EFCTV_DT", DATE),
    ("claim_load_date", "CLM_IDR_LD_DT", DATE),
    # amounts (negative on cancellations)
    ("payment_amount", "CLM_PMT_AMT", AMOUNT),
    ("total_charge_amount", "CLM_MDCR_INSTNL_TOT_CHRG_AMT", AMOUNT),
]

CLAIMS_DIAGNOSIS = [
    ("claim_id", "CUR_CLM_UNIQ_ID", STRING),
    ("diagnosis_sequence_number", "CLM_VAL_SQNC_NUM", INTEGER),  # 0 = principal
    ("beneficiary_mbi", "BENE_MBI_ID", STRING),
    ("beneficiary_hicn", "BENE_HIC_NUM", STRING),
    ("facility_ccn", "PRVDR_OSCAR_NUM", STRING),
    ("claim_type_code", "CLM_TYPE_CD", STRING),
    ("claim_product_type_code", "CLM_PROD_TYPE_CD", STRING),
    ("diagnosis_code", "CLM_DGNS_CD", STRING),
    ("present_on_admission_indicator", "CLM_POA_IND", STRING),
    ("icd_version_indicator", "DGNS_PRCDR_ICD_IND", STRING),
    ("claim_start_date", "CLM_FROM_DT", DATE),
    ("claim_end_date", "CLM_THRU_DT", DATE),
]

PART_B_PHYSICIAN_LINES = [
    # identifiers
    ("claim_id", "CUR_CLM_UNIQ_ID", STRING),
    ("line_number", "CLM_LINE_NUM", INTEGER),
    ("claim_control_number", "CLM_CNTL_NUM", STRING),
    ("beneficiary_mbi", "BENE_MBI_ID", STRING),
    ("beneficiary_hicn", "BENE_HIC_NUM", STRING),
    # rendering provider
    ("rendering_npi", "RNDRG_PRVDR_NPI_NUM", STRING),
    ("rendering_provider_tin", "CLM_RNDRG_PRVDR_TAX_NUM", STRING),
    ("rendering_provider_type_code", "RNDRG_PRVDR_TYPE_CD", STRING),
    ("rendering_provider_fips_state_code", "RNDRG_PRVDR_FIPS_ST_CD", STRING),
    ("provider_specialty_code", "CLM_PRVDR_SPCLTY_CD", STRING),
    # claim classification
    ("claim_type_code", "CLM_TYPE_CD", STRING),
    ("adjustment_type_code", "CLM_ADJSMT_TYPE_CD", STRING),
    ("carrier_payment_denial_code", "CLM_CARR_PMT_DNL_CD", STRING),
    ("processing_indicator_code", "CLM_PRCSG_IND_CD", STRING),
    # service
    ("hcpcs_code", "CLM_LINE_HCPCS_CD", STRING),
    ("hcpcs_modifier_1_code", "HCPCS_1_MDFR_CD", STRING),
    ("type_of_service_code", "CLM_FED_TYPE_SRVC_CD", STRING),
    ("place_of_service_code", "CLM_POS_CD", STRING),
    ("service_unit_quantity", "CLM_LINE_SRVC_UNIT_QTY", QUANTITY),
    ("line_primary_payer_code", "CLM_LINE_PRMRY_PYR_CD", STRING),
    # diagnoses
    ("line_diagnosis_code", "CLM_LINE_DGNS_CD", STRING),
    ("diagnosis_1_code", "CLM_DGNS_1_CD", STRING),
    ("diagnosis_2_code", "CLM_DGNS_2_CD", STRING),
    ("diagnosis_3_code", "CLM_DGNS_3_CD", STRING),
    ("icd_version_indicator", "DGNS_PRCDR_ICD_IND", STRING),
    # dates
    ("claim_start_date", "CLM_FROM_DT", DATE),
    ("claim_end_date", "CLM_THRU_DT", DATE),
    ("line_start_date", "CLM_LINE_FROM_DT", DATE),
    ("line_end_date", "CLM_LINE_THRU_DT", DATE),
    ("claim_effective_date", "CLM_EFCTV_DT", DATE),
    ("claim_load_date", "CLM_IDR_LD_DT", DATE),
    # amounts (negative on cancellations)
    ("line_paid_amount", "CLM_LINE_CVRD_PD_AMT", AMOUNT),
    ("line_allowed_amount", "CLM_LINE_ALOWD_CHRG_AMT", AMOUNT),
]

STAGING = {
    "beneficiary_demographics": BENEFICIARY_DEMOGRAPHICS,
    "claims_header": CLAIMS_HEADER,
    "claims_diagnosis": CLAIMS_DIAGNOSIS,
    "part_b_physician_lines": PART_B_PHYSICIAN_LINES,
}


def stage_file(current_bronze: DataFrame, file_type: str) -> DataFrame:
    """Stage one CCLF file type from its current-delivery bronze rows."""
    return stage(current_bronze, STAGING[file_type])


# The beneficiary attributes whose change starts a new member-history version. Age is not
# tracked: it moves forward every month without a real change. The helper column is not
# tracked either.
BENEFICIARY_TRACKED_COLUMNS = [
    target
    for target, _, _ in BENEFICIARY_DEMOGRAPHICS
    if target not in ("beneficiary_mbi", "age", "record_change_type")
]
