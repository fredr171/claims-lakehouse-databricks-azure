"""How the Humana silver tables are built from bronze, and the expectations on each.

The Humana pipeline (phase 3) defines one table per entry, each calling the same
functions; the local tests call build() directly. EXPECTATIONS names the expectations each
table carries (claims_lakehouse.silver.quality).
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame

from claims_lakehouse.silver.deliveries import current_rows, delivery_index
from claims_lakehouse.silver.humana.amount_rules import amount_problem
from claims_lakehouse.silver.humana.claim_families import claim_families, claim_versions
from claims_lakehouse.silver.humana.quality import ROW_EXPECTATIONS, UNIQUE_KEYS
from claims_lakehouse.silver.humana.staging import MEMBER_TRACKED_COLUMNS, stage_file
from claims_lakehouse.silver.member_history import member_versions
from claims_lakehouse.silver.quality import (
    KEYS_UNIQUE,
    RULE_BREAK_EXPECTATIONS,
    Expectation,
    duplicate_key_check,
    rule_breaks_by_month,
)

FILE_TYPES = ("member_eligibility", "medical_claim_lines")


# Silver table -> the expectations it carries. Tables not listed carry none.
EXPECTATIONS: dict[str, list[Expectation]] = {
    **{f"{ft}_current": ROW_EXPECTATIONS[ft] for ft in FILE_TYPES},
    **{f"{ft}_key_check": [KEYS_UNIQUE] for ft in FILE_TYPES},
    "medical_claim_line_amount_check": RULE_BREAK_EXPECTATIONS,
}


def build(bronze: Mapping[str, DataFrame]) -> dict[str, DataFrame]:
    """bronze: file type -> its bronze table. Returns silver table name -> DataFrame."""
    tables: dict[str, DataFrame] = {}
    for file_type in FILE_TYPES:
        tables[f"{file_type}_deliveries"] = delivery_index(bronze[file_type])
        current = current_rows(bronze[file_type], tables[f"{file_type}_deliveries"])
        tables[f"{file_type}_current"] = current
        key, across_months = UNIQUE_KEYS[file_type]
        tables[f"{file_type}_key_check"] = duplicate_key_check(current, key, across_months)
        tables[f"stg_{file_type}"] = stage_file(current, file_type)

    tables["member_versions"] = member_versions(
        tables["stg_member_eligibility"],
        key="member_id",
        tracked=MEMBER_TRACKED_COLUMNS,
        coverage_end="enrollment_end_date",
    )
    tables["medical_claim_line_amount_check"] = rule_breaks_by_month(
        tables["stg_medical_claim_lines"], amount_problem()
    )
    tables["claim_versions"] = claim_versions(tables["stg_medical_claim_lines"])
    tables["claim_families"] = claim_families(tables["claim_versions"])
    return tables
