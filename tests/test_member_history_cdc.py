"""AUTO CDC's SCD type 2 output, converted, equals the batch member history.

AUTO CDC only runs on Databricks, so a Python model of its SCD type 2 semantics stands in
for it here (tests/auto_cdc_model.py). The live run
checks the real AUTO CDC table against the answer keys.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from auto_cdc_model import auto_cdc_scd2

from claims_lakehouse.silver.member_history import (
    SEQUENCE_COLUMNS as SEQUENCE,
)
from claims_lakehouse.silver.member_history import (
    history_source,
    member_versions,
    versions_from_history,
)

KEY, TRACKED, END = "member_id", ("plan", "end_date"), "end_date"
# Archive time follows the list order (arrival order), which is not month order.
ARCHIVED = datetime(2026, 1, 1)

# (member, plan, end_date, delivery month): delivered rows, deliberately out of order.
ROWS = [
    ("M4", "B", None, "2023-05"),  # arrives first, but is M4's second version
    ("M1", "A", None, "2023-01"),
    ("M1", "A", None, "2023-02"),  # unchanged: extends version 1
    ("M1", "B", None, "2023-03"),  # update
    ("M1", "B", "2023-06-30", "2023-06"),  # termination
    ("M2", "A", None, "2023-04"),
    ("M2", "A", None, "2023-02"),  # arrives late but sorts first
    ("M3", "C", "2023-01-31", "2023-01"),  # terminated from the start: NEW, not TERMINATE
    ("M3", "C", None, "2023-03"),  # reinstated: an update
    ("M4", "A", None, "2023-02"),  # arrives last, but is M4's first version
]

SCHEMA = (
    "member_id string, plan string, end_date string, _delivery_month string, _archived_at timestamp"
)


@pytest.fixture
def staged(spark):
    return spark.createDataFrame(
        [
            (m, p, e, month, ARCHIVED + timedelta(minutes=i))
            for i, (m, p, e, month) in enumerate(ROWS)
        ],
        SCHEMA,
    )


def history_frame(spark, staged):
    source = history_source(staged, KEY, TRACKED)
    versions = auto_cdc_scd2(
        [r.asDict() for r in source.collect()],
        KEY,
        TRACKED,
        SEQUENCE,
        except_columns=["_archived_at"],
    )
    sequence = "struct<_delivery_month: string, _archived_at: timestamp>"
    return spark.createDataFrame(
        versions,
        f"member_id string, plan string, end_date string, _delivery_month string, "
        f"__START_AT {sequence}, __END_AT {sequence}",
    )


def rows(df):
    return sorted(tuple(r) for r in df.collect())


def test_converted_auto_cdc_history_equals_the_batch_reference(spark, staged):
    expected = member_versions(staged, KEY, TRACKED, END)
    actual = versions_from_history(history_frame(spark, staged), KEY, TRACKED, END)
    assert actual.dtypes == expected.dtypes  # names and types: the pipeline declares them
    assert rows(actual) == rows(expected)


def test_the_model_versions_as_expected(spark, staged):
    """Guard the stand-in itself: versions, types and spans for each member."""
    actual = versions_from_history(history_frame(spark, staged), KEY, TRACKED, END)
    got = {
        (r.member_id, r.version_number): (r.valid_from, r.valid_to, r.change_type,
                                          r.last_delivery_month)
        for r in actual.collect()
    }  # fmt: skip
    assert got == {
        ("M1", 1): ("2023-01", "2023-03", "NEW", "2023-02"),
        ("M1", 2): ("2023-03", "2023-06", "UPDATE", "2023-03"),
        ("M1", 3): ("2023-06", None, "TERMINATE", "2023-06"),
        ("M2", 1): ("2023-02", None, "NEW", "2023-04"),
        ("M3", 1): ("2023-01", "2023-03", "NEW", "2023-01"),
        ("M3", 2): ("2023-03", None, "UPDATE", "2023-03"),
        ("M4", 1): ("2023-02", "2023-05", "NEW", "2023-02"),
        ("M4", 2): ("2023-05", None, "UPDATE", "2023-05"),
    }
