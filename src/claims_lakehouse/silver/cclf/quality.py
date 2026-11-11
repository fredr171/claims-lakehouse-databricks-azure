"""CCLF quality: row expectations on each file's current rows, and its gate tables.

Stop (FAIL) when a value would make silver wrong: the claim family key, adjustment type,
dates that pick a version or a service month, paid amounts, and coverage dates. Everything
else only warns: odd but safe. Columns are the delivered (bronze) names.
"""

from __future__ import annotations

from claims_lakehouse.silver.cclf.staging import STAGING
from claims_lakehouse.silver.quality import (
    Action,
    Expectation,
    format_checks,
    one_of,
    present,
)

ADJUSTMENT_TYPES = ("0", "1", "2")
SEX_CODES = ("0", "1", "2")  # 0 unknown, 1 male, 2 female

_CLAIM_STOPS = ("CLM_FROM_DT", "CLM_IDR_LD_DT")

ROW_EXPECTATIONS: dict[str, list[Expectation]] = {
    "beneficiary_demographics": [
        *format_checks(
            STAGING["beneficiary_demographics"],
            stop_columns=("BENE_RNG_BGN_DT", "BENE_RNG_END_DT"),
        ),
        present("BENE_RNG_BGN_DT"),
        one_of("BENE_SEX_CD", SEX_CODES),
    ],
    "claims_header": [
        *format_checks(STAGING["claims_header"], stop_columns=(*_CLAIM_STOPS, "CLM_PMT_AMT")),
        *[present(c) for c in ("CLM_CNTL_NUM", *_CLAIM_STOPS, "CLM_PMT_AMT")],
        one_of("CLM_ADJSMT_TYPE_CD", ADJUSTMENT_TYPES, Action.FAIL),
    ],
    "claims_diagnosis": format_checks(STAGING["claims_diagnosis"]),
    "part_b_physician_lines": [
        *format_checks(
            STAGING["part_b_physician_lines"],
            stop_columns=(*_CLAIM_STOPS, "CLM_LINE_CVRD_PD_AMT"),
        ),
        *[present(c) for c in ("CLM_CNTL_NUM", *_CLAIM_STOPS, "CLM_LINE_CVRD_PD_AMT")],
        one_of("CLM_ADJSMT_TYPE_CD", ADJUSTMENT_TYPES, Action.FAIL),
    ],
}

# file type -> (key, unique across every current delivery rather than within its file)
UNIQUE_KEYS: dict[str, tuple[tuple[str, ...], bool]] = {
    "beneficiary_demographics": (("BENE_MBI_ID",), False),
    "claims_header": (("CUR_CLM_UNIQ_ID",), True),
    "claims_diagnosis": (("CUR_CLM_UNIQ_ID", "CLM_VAL_SQNC_NUM"), True),
    "part_b_physician_lines": (("CUR_CLM_UNIQ_ID", "CLM_LINE_NUM"), True),
}
