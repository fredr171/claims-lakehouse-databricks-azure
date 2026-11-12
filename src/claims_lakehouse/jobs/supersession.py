"""Whether a payer's pipeline needs a full refresh before this run.

AUTO CDC applies each new delivery's rows on top of what it has already applied; it cannot
take back rows of a delivery that a corrected re-send later superseded. So when bronze holds
more superseded member-file deliveries than the payer's member history was last built with,
the pipeline must rebuild from current deliveries only (a full refresh). Every other change,
including a re-send of a claims file, is handled by the materialized views, which recompute.

The pipeline publishes the count it was built with (<history table>_superseded); it is
gated, so after a stopped update it keeps the old count and the next run still refreshes.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from claims_lakehouse.silver.deliveries import delivery_index


def superseded_deliveries(bronze_member_file: DataFrame) -> int:
    """How many deliveries of the member file are no longer current."""
    return delivery_index(bronze_member_file).where(~F.col("is_current")).count()


def needs_full_refresh(in_bronze: int, published: int | None) -> bool:
    """published: the count the pipeline last published, or None before its first success."""
    return in_bronze > (published or 0)
