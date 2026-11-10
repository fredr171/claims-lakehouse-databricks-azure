"""Staging helpers: blank to NULL, typing, lineage pass-through."""

from datetime import date
from decimal import Decimal

from claims_lakehouse.silver.common import AMOUNT, DATE, INTEGER, STRING, stage

MAPPING = [
    ("code", "CODE", STRING),
    ("start_date", "START_DT", DATE),
    ("amount", "AMT", AMOUNT),
    ("count", "CNT", INTEGER),
]


def staged_rows(spark, rows):
    bronze = spark.createDataFrame(
        rows, "CODE string, START_DT string, AMT string, CNT string, _checksum string"
    )
    return [r.asDict() for r in stage(bronze, MAPPING).collect()]


def test_values_are_renamed_typed_and_lineage_kept(spark):
    [row] = staged_rows(spark, [("007", "2023-01-31", "-748.13", "3", "abc")])
    assert row == {
        "code": "007",  # codes keep leading zeros
        "start_date": date(2023, 1, 31),
        "amount": Decimal("-748.13"),
        "count": 3,
        "_checksum": "abc",
    }


def test_blank_and_whitespace_become_null(spark):
    [row] = staged_rows(spark, [("", "  ", None, " ", "abc")])
    assert (row["code"], row["start_date"], row["amount"], row["count"]) == (None,) * 4


def test_whitespace_around_a_value_is_kept(spark):
    [row] = staged_rows(spark, [(" A ", None, None, None, "abc")])
    assert row["code"] == " A "


def test_unparseable_values_become_null_instead_of_failing(spark):
    """Spark runs in ANSI mode: a plain cast would fail the whole query."""
    [row] = staged_rows(spark, [("X", "2023-13-01", "12,50", "three", "abc")])
    assert (row["start_date"], row["amount"], row["count"]) == (None, None, None)
