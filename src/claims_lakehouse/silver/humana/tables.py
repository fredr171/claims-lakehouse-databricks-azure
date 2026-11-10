"""How the Humana silver tables are built from bronze, in one place.

The Humana pipeline (phase 3) defines one table per entry, each calling the same
functions; the local tests call build() directly.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame

from claims_lakehouse.silver.deliveries import current_rows, delivery_index
from claims_lakehouse.silver.humana.claim_families import claim_families, claim_versions
from claims_lakehouse.silver.humana.staging import MEMBER_TRACKED_COLUMNS, stage_file
from claims_lakehouse.silver.member_history import member_versions

FILE_TYPES = ("member_eligibility", "medical_claim_lines")


def build(bronze: Mapping[str, DataFrame]) -> dict[str, DataFrame]:
    """bronze: file type -> its bronze table. Returns silver table name -> DataFrame."""
    tables: dict[str, DataFrame] = {}
    for file_type in FILE_TYPES:
        tables[f"{file_type}_deliveries"] = delivery_index(bronze[file_type])
        current = current_rows(bronze[file_type], tables[f"{file_type}_deliveries"])
        tables[f"stg_{file_type}"] = stage_file(current, file_type)

    tables["member_versions"] = member_versions(
        tables["stg_member_eligibility"],
        key="member_id",
        tracked=MEMBER_TRACKED_COLUMNS,
        coverage_end="enrollment_end_date",
    )
    tables["claim_versions"] = claim_versions(tables["stg_medical_claim_lines"])
    tables["claim_families"] = claim_families(tables["claim_versions"])
    return tables
