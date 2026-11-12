"""MA payer quality: row expectations on each file's current rows, and its gate tables.

Stop (FAIL) when a value would make silver wrong: the MBI (gold keys people by it),
enrollment dates, the claim frequency, status and form codes, the link from a reversal or
replacement to its original, dates that pick a service month, and paid amounts.
Everything else only warns. Columns are the delivered (bronze) names.

Line amount rules are a gate table, not a row expectation: one odd line warns, more than
1% (and at least 5) of a month's lines stop (claims_lakehouse.silver.quality).
"""

from __future__ import annotations

from claims_lakehouse.silver.humana.claim_families import ORIGINAL, REPLACEMENT, VOID
from claims_lakehouse.silver.humana.staging import STAGING
from claims_lakehouse.silver.quality import (
    Action,
    Expectation,
    format_checks,
    one_of,
    present,
)

YES_NO = ("Y", "N")

ROW_EXPECTATIONS: dict[str, list[Expectation]] = {
    "member_eligibility": [
        *format_checks(
            STAGING["member_eligibility"],
            stop_columns=("ENROLLMENT_START_DT", "ENROLLMENT_END_DT"),
        ),
        present("MBI"),
        present("ENROLLMENT_START_DT"),
        one_of("GENDER", ("M", "F")),
        one_of("PRODUCT_TYPE", ("HMO", "PPO", "D-SNP")),
        *[one_of(c, YES_NO) for c in ("DUAL_FLAG", "LIS_FLAG", "ESRD_FLAG", "HOSPICE_FLAG")],
    ],
    "medical_claim_lines": [
        *format_checks(
            STAGING["medical_claim_lines"],
            stop_columns=("SERVICE_FROM_DT", "PROCESSED_DT", "PAID_AMT"),
        ),
        *[present(c) for c in ("SERVICE_FROM_DT", "PROCESSED_DT", "PAID_AMT")],
        one_of("CLAIM_FREQ_CD", (ORIGINAL, REPLACEMENT, VOID), Action.FAIL),
        one_of("CLAIM_STATUS", ("P", "D", "R"), Action.FAIL),
        one_of("CLAIM_FORM_TYPE", ("I", "P"), Action.FAIL),
        Expectation(
            "ORIG_CLAIM_ID_links_action",
            f"CLAIM_FREQ_CD = '{ORIGINAL}' OR coalesce(trim(ORIG_CLAIM_ID) != '', false)",
            Action.FAIL,
        ),
        one_of("CAPITATED_FLAG", YES_NO),
        one_of("IN_NETWORK_FLAG", YES_NO),
    ],
}

# file type -> (key, unique across every current delivery rather than within its file)
UNIQUE_KEYS: dict[str, tuple[tuple[str, ...], bool]] = {
    "member_eligibility": (("MEMBER_ID",), False),
    "medical_claim_lines": (("CLAIM_ID", "CLAIM_LINE_NUM", "CLAIM_FREQ_CD"), True),
}
