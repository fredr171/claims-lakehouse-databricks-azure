"""CCLF claim families: an original claim version plus its cancellation or adjustment.

A family is (part, CLM_CNTL_NUM): every version of a claim shares the control number and
has its own claim_id (CUR_CLM_UNIQ_ID). CLM_ADJSMT_TYPE_CD: 0 original, 1 cancellation,
2 adjustment (a full replacement, not a delta).

Status comes from which versions are present, never from file order:
    CANCELLED  any cancellation          net paid 0
    ADJUSTED   any adjustment, no cancel  net paid = the adjustment's amount
    ORIGINAL   only the original          net paid = the original's amount
If a family ever has two adjustments, the one with the latest claim load date (then
claim_id) wins; that is the claim's own processing date, not the order files arrived in.

Part A amount = CCLF1 CLM_PMT_AMT. Part B amount = the sum of the claim's CCLF5 line
paid amounts. Service month = month of the original's CLM_FROM_DT.

Partial history is allowed: an action whose original is not loaded still forms a family
(has_original false), with its service month from the earliest version's start date.
Input is staged rows from current deliveries, so a re-sent month replaces its versions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from claims_lakehouse.silver.common import AMOUNT

ORIGINAL, CANCELLATION, ADJUSTMENT = "0", "1", "2"
FAMILY_KEY = ("part", "claim_control_number")
# Part B version (CCLF5 line rollup) key, and the columns it takes from any one line. Every
# line of a version must agree on them: the part_b_version_check gate table stops otherwise.
PART_B_VERSION_KEY = ("claim_control_number", "claim_id")
PART_B_LINE_AGREED_COLUMNS = ("beneficiary_mbi", "adjustment_type_code")
_VERSION_COLUMNS = (
    "part",
    "claim_control_number",
    "claim_id",
    "beneficiary_mbi",
    "adjustment_type_code",
    "claim_start_date",
    "claim_load_date",
    "delivery_month",
    "paid_amount",
)


def claim_versions(claims_header: DataFrame, part_b_lines: DataFrame) -> DataFrame:
    """One row per claim version: Part A headers, and Part B lines rolled up to claims."""
    part_a = claims_header.select(
        F.lit("A").alias("part"),
        "claim_control_number",
        "claim_id",
        "beneficiary_mbi",
        "adjustment_type_code",
        "claim_start_date",
        "claim_load_date",
        F.col("_delivery_month").alias("delivery_month"),
        F.col("payment_amount").alias("paid_amount"),
    )
    # F.first is safe: every line of a version agrees on these (part_b_version_check).
    part_b = part_b_lines.groupBy(
        "claim_control_number", "claim_id", F.col("_delivery_month").alias("delivery_month")
    ).agg(
        F.lit("B").alias("part"),
        F.first("beneficiary_mbi").alias("beneficiary_mbi"),
        F.first("adjustment_type_code").alias("adjustment_type_code"),
        F.min("claim_start_date").alias("claim_start_date"),
        F.max("claim_load_date").alias("claim_load_date"),
        F.sum("line_paid_amount").cast(AMOUNT).alias("paid_amount"),
    )
    return part_a.unionByName(part_b.select(*_VERSION_COLUMNS))


def claim_families(versions: DataFrame) -> DataFrame:
    """One row per (part, claim_control_number)."""
    version = F.struct(
        "claim_id", "beneficiary_mbi", "claim_start_date", "delivery_month", "paid_amount"
    )
    by_processing = F.struct("claim_load_date", "claim_id")

    originals = (
        versions.where(F.col("adjustment_type_code") == ORIGINAL)
        .groupBy(*FAMILY_KEY)
        .agg(
            F.min_by(version, by_processing).alias("original"),
            F.count(F.lit(1)).alias("original_count"),
        )
    )
    # The deciding action: any cancellation beats an adjustment; then the latest processed.
    actions = (
        versions.where(F.col("adjustment_type_code").isin(CANCELLATION, ADJUSTMENT))
        .groupBy(*FAMILY_KEY)
        .agg(
            F.max_by(
                F.struct("adjustment_type_code", version.alias("v")),
                F.struct(
                    (F.col("adjustment_type_code") == CANCELLATION).alias("is_cancel"),
                    by_processing,
                ),
            ).alias("action"),
            F.count(F.lit(1)).alias("action_count"),
        )
    )
    overall = versions.groupBy(*FAMILY_KEY).agg(
        F.min_by("beneficiary_mbi", by_processing).alias("first_beneficiary_mbi"),
        F.min("claim_start_date").alias("earliest_claim_start_date"),
        F.count(F.lit(1)).alias("version_count"),
        F.min("delivery_month").alias("first_delivery_month"),
        F.max("delivery_month").alias("latest_delivery_month"),
    )

    action_type = F.col("action.adjustment_type_code")
    status = (
        F.when(action_type == CANCELLATION, "CANCELLED")
        .when(action_type == ADJUSTMENT, "ADJUSTED")
        .otherwise("ORIGINAL")
    )
    net_paid = (
        F.when(action_type == CANCELLATION, F.lit(0))
        .when(action_type == ADJUSTMENT, F.col("action.v.paid_amount"))
        .otherwise(F.col("original.paid_amount"))
    )
    service_start = F.coalesce("original.claim_start_date", "earliest_claim_start_date")

    return (
        overall.join(originals, list(FAMILY_KEY), "left")
        .join(actions, list(FAMILY_KEY), "left")
        .select(
            F.concat_ws(":", *FAMILY_KEY).alias("claim_family_key"),
            *FAMILY_KEY,
            F.coalesce("original.beneficiary_mbi", "first_beneficiary_mbi").alias(
                "beneficiary_mbi"
            ),
            F.col("original").isNotNull().alias("has_original"),
            F.col("original.claim_id").alias("original_claim_id"),
            F.col("original.delivery_month").alias("original_delivery_month"),
            F.col("original.paid_amount").alias("original_amount"),
            F.when(action_type == CANCELLATION, "CANCEL")
            .when(action_type == ADJUSTMENT, "ADJUST")
            .alias("action_type"),
            F.col("action.v.claim_id").alias("action_claim_id"),
            F.col("action.v.delivery_month").alias("action_delivery_month"),
            F.col("action.v.paid_amount").alias("action_amount"),
            status.alias("family_status"),
            net_paid.cast(AMOUNT).alias("net_paid_amount"),
            service_start.alias("service_start_date"),
            F.date_format(service_start, "yyyy-MM").alias("service_month"),
            "version_count",
            F.coalesce("original_count", F.lit(0)).alias("original_count"),
            F.coalesce("action_count", F.lit(0)).alias("action_count"),
            "first_delivery_month",
            "latest_delivery_month",
        )
    )
