"""The current-delivery index, on hand-built bronze rows.

Every scenario uses its own file type in one table, built once per module.
"""

from datetime import datetime

import pytest

from claims_lakehouse.silver.deliveries import current_rows, delivery_index

COLUMNS = (
    "ID string, _feed string, _file_type string, _delivery_month string, "
    "_checksum string, _source_file string, _archived_at timestamp"
)


def row(id_, scenario, month, checksum, archived_minute):
    archived_at = datetime(2026, 1, 1, 0, archived_minute)
    return (id_, "cclf", scenario, month, checksum, f"{scenario}/{month}/{checksum}", archived_at)


SCENARIOS = [
    # The latest archived delivery of a month is current (two re-sends).
    row("1", "RESENDS", "2023-01", "a", 1),
    row("2", "RESENDS", "2023-01", "a", 1),
    row("1", "RESENDS", "2023-01", "b", 5),
    row("1", "RESENDS", "2023-01", "c", 9),
    # Months are independent of arrival order.
    row("1", "MONTHS", "2023-02", "d", 1),  # February arrives first
    row("1", "MONTHS", "2023-01", "e", 2),
    row("1", "MONTHS", "2023-01", "f", 3),  # January re-sent after both
    # A tie on archive time is broken by checksum.
    row("1", "TIE", "2023-01", "h", 1),
    row("1", "TIE", "2023-01", "g", 1),
    # Rows for current_rows.
    row("old", "ROWS", "2023-01", "i", 1),
    row("new", "ROWS", "2023-01", "j", 2),
    row("feb", "ROWS", "2023-02", "k", 3),
]


@pytest.fixture(scope="module")
def bronze(spark):
    return spark.createDataFrame(SCENARIOS, COLUMNS).cache()


@pytest.fixture(scope="module")
def index(bronze):
    return {r._checksum: r for r in delivery_index(bronze).collect()}


def test_latest_archived_delivery_of_a_month_is_current(index):
    assert [index[c].delivery_version for c in "abc"] == [1, 2, 3]
    assert [index[c].is_current for c in "abc"] == [False, False, True]
    assert {index[c].delivery_count for c in "abc"} == {3}
    assert index["a"].row_count == 2


def test_months_are_independent_of_arrival_order(index):
    assert index["d"].is_current  # February is untouched by January's re-send
    assert not index["e"].is_current
    assert index["f"].is_current


def test_file_types_are_independent(index):
    """Same months and archive times in every scenario; each file type has its own current."""
    assert sum(r.is_current for r in index.values()) == 6


def test_tie_on_archive_time_is_broken_by_checksum(index):
    assert index["h"].is_current
    assert not index["g"].is_current


def test_current_rows_keeps_only_current_deliveries(bronze):
    current = current_rows(bronze.where("_file_type = 'ROWS'"))
    assert current.columns == bronze.columns
    assert sorted(r.ID for r in current.collect()) == ["feb", "new"]
