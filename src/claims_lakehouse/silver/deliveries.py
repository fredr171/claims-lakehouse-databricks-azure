"""The current-delivery index: which delivery of each file silver reads.

A file is identified by (feed, file type, delivery month). Every delivery of it stays in
bronze (append-only); a corrected re-send is a new delivery with a new checksum. The
current delivery is the one archived last. Arrival order between different months does not
matter: each month's file is judged only against other deliveries of the same month.

Deliveries are ordered by _archived_at, the time the validation Function wrote the file to
the archive, not by _ingested_at: Auto Loader can ingest an original and its re-send in the
same micro-batch, giving both the same ingest time. A tie on _archived_at falls back to the
checksum, so the choice is always deterministic.

The index is built from bronze rows, so a delivery with no rows would be invisible to it;
validation's has_rows check guarantees that never happens.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

FILE_KEY = ("_feed", "_file_type", "_delivery_month")
DELIVERY_COLUMNS = (*FILE_KEY, "_checksum", "_source_file", "_archived_at")


def delivery_index(bronze: DataFrame) -> DataFrame:
    """One row per delivery: its row count, version and whether it is current.

    delivery_version   1 for the first delivery of a file, 2 for its first re-send, ...
    delivery_count     how many deliveries the file has had
    is_current         the latest delivery of the file
    """
    by_arrival = Window.partitionBy(*FILE_KEY).orderBy("_archived_at", "_checksum")
    per_file = Window.partitionBy(*FILE_KEY)
    return (
        bronze.groupBy(*DELIVERY_COLUMNS)
        .agg(F.count(F.lit(1)).alias("row_count"))
        .withColumn("delivery_version", F.row_number().over(by_arrival))
        .withColumn("delivery_count", F.count(F.lit(1)).over(per_file))
        .withColumn("is_current", F.col("delivery_version") == F.col("delivery_count"))
    )


def current_rows(bronze: DataFrame, index: DataFrame | None = None) -> DataFrame:
    """Bronze rows from current deliveries only, with the same columns as bronze."""
    index = delivery_index(bronze) if index is None else index
    current = index.where("is_current").select(*FILE_KEY, "_checksum")
    # The join moves its keys to the front; keep bronze's column order.
    return bronze.join(current, on=[*FILE_KEY, "_checksum"], how="left_semi").select(
        *bronze.columns
    )
