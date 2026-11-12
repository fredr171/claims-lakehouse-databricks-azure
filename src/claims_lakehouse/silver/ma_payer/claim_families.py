"""MA payer claim families: an original claim plus its reversal and replacement.

MAP_MEDCLM is line-level, so lines roll up to claim versions first (claim_id plus
CLAIM_FREQ_CD: a reversal reuses its original's claim_id). CLAIM_FREQ_CD:
    1  original
    8  reversal (void): same claim_id as the claim it reverses, amounts negated
    7  replacement: a new claim_id, always delivered with a reversal of the original
Reversals and replacements point back through ORIG_CLAIM_ID, so the family key is
coalesce(original_claim_id, claim_id), the original's claim_id.

Status comes from which versions are still active (not reversed), never from file order:
    REPLACED  an active replacement             net paid = that replacement's paid
    DENIED    the active original is denied (D) net paid 0
    ORIGINAL  the active original               net paid = the original's paid
    VOIDED    nothing active                    net paid 0
A replacement that is itself reversed later is no longer active, so a chain
original -> replacement -> reversal of the replacement ends VOIDED.

Because reversals carry negated amounts, net paid also equals SUM(PAID_AMT) over every
version; the answer-key tests check both. The status rule is the one used here, as for
CCLF, so a reversal whose original is not loaded nets to 0, not to a negative amount.

Service month = month of the original's SERVICE_FROM_DT. Partial history is allowed: a
family without its original (has_original false) takes the earliest version's start date.
Input is staged rows from current deliveries, so a re-sent month replaces its versions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from claims_lakehouse.silver.common import AMOUNT

ORIGINAL, REPLACEMENT, VOID = "1", "7", "8"
DENIED = "D"
CAPITATED = "Y"


def claim_versions(claim_lines: DataFrame) -> DataFrame:
    """One row per claim version: (claim_id, claim_frequency_code) in a delivery."""
    return claim_lines.groupBy(
        "claim_id", "claim_frequency_code", F.col("_delivery_month").alias("delivery_month")
    ).agg(
        F.coalesce(F.first("original_claim_id", ignorenulls=True), F.col("claim_id")).alias(
            "claim_family_key"
        ),
        F.first("claim_status_code").alias("claim_status_code"),
        F.first("claim_form_type_code").alias("claim_form_type_code"),
        F.first("member_id").alias("member_id"),
        F.first("beneficiary_mbi").alias("beneficiary_mbi"),
        F.first("contract_id").alias("contract_id"),
        F.min("claim_start_date").alias("claim_start_date"),
        F.max("processed_date").alias("processed_date"),
        F.count(F.lit(1)).alias("line_count"),
        F.sum("paid_amount").cast(AMOUNT).alias("paid_amount"),
        F.sum("allowed_amount").cast(AMOUNT).alias("allowed_amount"),
        F.max(F.col("capitated_flag") == CAPITATED).alias("is_capitated"),
    )


def claim_families(versions: DataFrame) -> DataFrame:
    """One row per claim_family_key."""
    frequency = F.col("claim_frequency_code")
    # An original or replacement is reversed when its claim_id also has a reversal.
    reversed_ = (frequency != VOID) & F.max(frequency == VOID).over(
        Window.partitionBy("claim_family_key", "claim_id")
    )
    sequenced = versions.withColumn("is_reversed", reversed_)

    by_processing = F.struct("delivery_month", "processed_date", "claim_id")
    is_original, is_replacement = frequency == ORIGINAL, frequency == REPLACEMENT
    active = ~F.col("is_reversed")
    # Inside an aggregate, F.when without otherwise gives NULL, which min_by/max_by and
    # count ignore: these restrict an aggregate to one kind of version.
    original = F.when(
        is_original,
        F.struct(
            "claim_id",
            "claim_status_code",
            "delivery_month",
            "claim_start_date",
            "paid_amount",
            "allowed_amount",
            "line_count",
            "is_capitated",
        ),
    )
    replacement = F.when(is_replacement, F.struct("claim_id", "delivery_month", "paid_amount"))

    families = sequenced.groupBy("claim_family_key").agg(
        F.min_by(original, F.when(is_original, by_processing)).alias("original"),
        F.sum(is_original.cast("int")).alias("original_count"),
        F.sum((is_original & active).cast("int")).alias("active_originals"),
        # Prefer an active replacement; among several, the latest processed.
        F.max_by(
            replacement,
            F.when(is_replacement, F.struct(active.alias("is_active"), by_processing)),
        ).alias("replacement"),
        F.sum(is_replacement.cast("int")).alias("replacement_count"),
        F.sum((is_replacement & active).cast("int")).alias("active_replacements"),
        F.sum((frequency == VOID).cast("int")).alias("reversal_count"),
        F.max(F.when(~is_original, F.col("delivery_month"))).alias("action_delivery_month"),
        F.min_by(
            F.struct("member_id", "beneficiary_mbi", "contract_id", "claim_form_type_code"),
            by_processing,
        ).alias("first"),
        F.min("claim_start_date").alias("earliest_claim_start_date"),
        F.sum("paid_amount").alias("sum_of_version_paid"),
        F.count(F.lit(1)).alias("version_count"),
        F.min("delivery_month").alias("first_delivery_month"),
        F.max("delivery_month").alias("latest_delivery_month"),
    )

    original_denied = F.col("original.claim_status_code") == DENIED
    status = (
        F.when(F.col("active_replacements") > 0, "REPLACED")
        .when((F.col("active_originals") > 0) & original_denied, "DENIED")
        .when(F.col("active_originals") > 0, "ORIGINAL")
        .otherwise("VOIDED")
    )
    net_paid = (
        F.when(F.col("active_replacements") > 0, F.col("replacement.paid_amount"))
        .when((F.col("active_originals") > 0) & ~original_denied, F.col("original.paid_amount"))
        .otherwise(F.lit(0))
    )
    service_start = F.coalesce("original.claim_start_date", "earliest_claim_start_date")
    is_capitated = F.coalesce(F.col("original.is_capitated"), F.lit(False))

    return families.select(
        "claim_family_key",
        F.col("first.member_id").alias("member_id"),
        F.col("first.beneficiary_mbi").alias("beneficiary_mbi"),
        F.col("first.contract_id").alias("contract_id"),
        F.col("first.claim_form_type_code").alias("claim_form_type_code"),
        F.col("original").isNotNull().alias("has_original"),
        F.col("original.claim_id").alias("original_claim_id"),
        F.col("original.claim_status_code").alias("original_status_code"),
        F.col("original.delivery_month").alias("original_delivery_month"),
        F.col("original.paid_amount").alias("original_paid_amount"),
        F.col("original.line_count").alias("original_line_count"),
        is_capitated.alias("is_capitated"),
        F.when(is_capitated, F.col("original.allowed_amount")).alias("capitated_allowed_amount"),
        F.when(F.col("replacement_count") > 0, "REPLACE")
        .when(F.col("reversal_count") > 0, "VOID")
        .alias("action_type"),
        "action_delivery_month",
        F.col("replacement.claim_id").alias("replacement_claim_id"),
        F.col("replacement.paid_amount").alias("replacement_paid_amount"),
        status.alias("family_status"),
        net_paid.cast(AMOUNT).alias("net_paid_amount"),
        F.col("sum_of_version_paid").cast(AMOUNT).alias("sum_of_version_paid"),
        service_start.alias("service_start_date"),
        F.date_format(service_start, "yyyy-MM").alias("service_month"),
        "version_count",
        "original_count",
        "reversal_count",
        "replacement_count",
        "first_delivery_month",
        "latest_delivery_month",
    )
