"""Staging helpers shared by every payer: blank to NULL, then typed columns.

A staging map is a list of (target, source, type). Codes and IDs stay strings (leading
zeros); dates, amounts and counts are typed. Lineage columns (_feed, _delivery_month, ...)
pass through unchanged.

Typing uses try_cast, so a value that does not parse becomes NULL instead of failing the
query (Spark runs in ANSI mode, as Databricks does). try_cast is also lenient: '2023-1-5'
parses as a date and '12.345' rounds to 12.35. Typing is therefore not a quality check;
the row-level expectations check the delivered text format of every typed column.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

STRING = "string"
DATE = "date"
INTEGER = "int"
AMOUNT = "decimal(18,2)"
QUANTITY = "decimal(18,3)"

StagingMap = Sequence[tuple[str, str, str]]  # (target column, source column, type)


def blank_to_null(column: Column) -> Column:
    """NULL for an empty or whitespace-only value; anything else unchanged."""
    return F.when(F.trim(column) == "", F.lit(None)).otherwise(column)


def typed(column: Column, dtype: str) -> Column:
    value = blank_to_null(column)
    return value if dtype == STRING else value.try_cast(dtype)


def stage(bronze: DataFrame, mapping: StagingMap) -> DataFrame:
    """Rename and type the mapped columns, then append bronze's lineage columns."""
    lineage = [c for c in bronze.columns if c.startswith("_")]
    return bronze.select(
        *[typed(F.col(source), dtype).alias(target) for target, source, dtype in mapping],
        *lineage,
    )
