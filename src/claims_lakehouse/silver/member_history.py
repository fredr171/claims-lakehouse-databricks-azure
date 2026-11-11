"""Member history (SCD type 2), shared by every payer.

This is the batch reference for what AUTO CDC builds in the pipeline (phase 3): keys
sequenced by delivery month then _archived_at, history tracked only on the tracked
columns, fed from current deliveries only. Recomputing from current deliveries is what
makes a corrected re-send retract the changes of the delivery it replaced (see the README
on supersession). The phase 3 live run checks the AUTO CDC table against the same answer
keys these tests use.

A version starts when a tracked column differs from the member's previous delivered row,
compared NULL-safely. That works whether a payer sends only new and changed members (CCLF8)
or its whole population every month: an unchanged row just extends the current version.

The change type is derived, never read from the file:
    NEW        the member's first version
    TERMINATE  the coverage end date goes from empty to set
    UPDATE     any other change
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SEQUENCE_COLUMNS = ("_delivery_month", "_archived_at")


def member_versions(
    staged: DataFrame, key: str, tracked: Sequence[str], coverage_end: str
) -> DataFrame:
    """One row per member per version.

    valid_from / valid_to   delivery month the version arrived / the next version arrived
                            (NULL while current); YYYY-MM
    changed_columns         tracked columns that differ from the previous version
    first_delivery_month    the member's first delivered row (any version)
    last_delivery_month     the version's last delivered row (later rows that did not change it)
    """
    if coverage_end not in tracked:
        raise ValueError(f"coverage_end {coverage_end!r} must be a tracked column")
    by_delivery = Window.partitionBy(key).orderBy(*SEQUENCE_COLUMNS)
    attributes = F.struct(*tracked)
    previous = F.lag(attributes).over(by_delivery)

    changed = F.array_compact(
        F.array(*[F.when(~F.col(c).eqNullSafe(previous[c]), F.lit(c)) for c in tracked])
    )
    rows = (
        staged.withColumn("_previous_end", previous[coverage_end])
        .withColumn("_is_first", previous.isNull())
        .withColumn("changed_columns", F.when(previous.isNull(), F.array()).otherwise(changed))
        .withColumn("_starts", F.col("_is_first") | (F.size("changed_columns") > 0))
        .withColumn(
            "version_number",
            F.sum(F.col("_starts").cast("int")).over(
                by_delivery.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn("first_delivery_month", F.min("_delivery_month").over(Window.partitionBy(key)))
    )

    spans = rows.groupBy(key, "version_number").agg(
        F.max("_delivery_month").alias("last_delivery_month")
    )
    by_version = Window.partitionBy(key).orderBy("version_number")
    change_type = (
        F.when(F.col("_is_first"), "NEW")
        .when(F.col("_previous_end").isNull() & F.col(coverage_end).isNotNull(), "TERMINATE")
        .otherwise("UPDATE")
    )
    return (
        rows.where("_starts")
        .join(spans, [key, "version_number"])
        .select(
            key,
            "version_number",
            F.col("_delivery_month").alias("valid_from"),
            F.lead("_delivery_month").over(by_version).alias("valid_to"),
            F.lead("_delivery_month").over(by_version).isNull().alias("is_current"),
            change_type.alias("change_type"),
            "changed_columns",
            *tracked,
            "first_delivery_month",
            "last_delivery_month",
        )
    )


def history_source(staged: DataFrame, key: str, tracked: Sequence[str]) -> DataFrame:
    """What the pipeline feeds AUTO CDC: the key, the tracked columns, and the delivery
    month (untracked, so AUTO CDC updates it in place: the version's last delivery month).
    Sequence by struct(_delivery_month, _archived_at), excluding _archived_at."""
    return staged.select(key, *tracked, *SEQUENCE_COLUMNS)


def versions_from_history(
    history: DataFrame, key: str, tracked: Sequence[str], coverage_end: str
) -> DataFrame:
    """member_versions() columns from an AUTO CDC SCD type 2 table of history_source().

    AUTO CDC gives one row per version with __START_AT / __END_AT (the sequence values of
    the version's first row and of the next version's first row) and the untracked
    _delivery_month of the version's last row. The rest is derived the same way as in
    member_versions().
    """
    if coverage_end not in tracked:
        raise ValueError(f"coverage_end {coverage_end!r} must be a tracked column")
    start = F.col("__START_AT")
    by_version = Window.partitionBy(key).orderBy(
        start.getField("_delivery_month"), start.getField("_archived_at")
    )
    previous = F.lag(F.struct(*tracked)).over(by_version)
    changed = F.array_compact(
        F.array(*[F.when(~F.col(c).eqNullSafe(previous[c]), F.lit(c)) for c in tracked])
    )
    change_type = (
        F.when(previous.isNull(), "NEW")
        .when(previous[coverage_end].isNull() & F.col(coverage_end).isNotNull(), "TERMINATE")
        .otherwise("UPDATE")
    )
    return history.select(
        key,
        F.row_number().over(by_version).cast("long").alias("version_number"),  # as in batch
        start.getField("_delivery_month").alias("valid_from"),
        F.col("__END_AT").getField("_delivery_month").alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
        change_type.alias("change_type"),
        F.when(previous.isNull(), F.array().cast("array<string>"))
        .otherwise(changed)
        .alias("changed_columns"),
        *tracked,
        F.min(start.getField("_delivery_month"))
        .over(Window.partitionBy(key))
        .alias("first_delivery_month"),
        F.col("_delivery_month").alias("last_delivery_month"),
    )


def current_members(versions: DataFrame) -> DataFrame:
    """The current version of every member."""
    return versions.where("is_current")
