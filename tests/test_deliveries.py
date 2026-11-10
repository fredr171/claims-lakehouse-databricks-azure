"""The current-delivery index, on small hand-built bronze tables."""

from datetime import datetime

import pytest

from claims_lakehouse.silver.deliveries import current_rows, delivery_index

COLUMNS = (
    "ID string, _feed string, _file_type string, _delivery_month string, "
    "_checksum string, _source_file string, _archived_at timestamp"
)


def at(minute: int) -> datetime:
    return datetime(2026, 1, 1, 0, minute)


def row(id_, file_type, month, checksum, archived_minute):
    return (
        id_,
        "cclf",
        file_type,
        month,
        checksum * 64,
        f"{month}/{checksum}",
        at(archived_minute),
    )


@pytest.fixture
def make(spark):
    return lambda rows: spark.createDataFrame(rows, COLUMNS)


def index_by_checksum(bronze):
    return {r._checksum[0]: r for r in delivery_index(bronze).collect()}


def test_latest_archived_delivery_of_a_month_is_current(make):
    bronze = make(
        [
            row("1", "claims_header", "2023-01", "a", 1),
            row("2", "claims_header", "2023-01", "a", 1),
            row("1", "claims_header", "2023-01", "b", 5),  # corrected re-send
            row("1", "claims_header", "2023-01", "c", 9),  # and another
        ]
    )
    index = index_by_checksum(bronze)
    assert [index[c].delivery_version for c in "abc"] == [1, 2, 3]
    assert [index[c].is_current for c in "abc"] == [False, False, True]
    assert {index[c].delivery_count for c in "abc"} == {3}
    assert index["a"].row_count == 2


def test_months_are_independent_of_arrival_order(make):
    bronze = make(
        [
            row("1", "claims_header", "2023-02", "a", 1),  # February arrives first
            row("1", "claims_header", "2023-01", "b", 2),
            row("1", "claims_header", "2023-01", "c", 3),  # January re-sent after both
        ]
    )
    index = index_by_checksum(bronze)
    assert index["a"].is_current  # February is untouched by January's re-send
    assert not index["b"].is_current
    assert index["c"].is_current


def test_file_types_are_independent(make):
    bronze = make(
        [
            row("1", "claims_header", "2023-01", "a", 1),
            row("1", "claims_diagnosis", "2023-01", "b", 2),
            row("1", "claims_header", "2023-01", "c", 3),
        ]
    )
    index = index_by_checksum(bronze)
    assert index["b"].is_current  # the CCLF1 re-send does not supersede CCLF4
    assert not index["a"].is_current
    assert index["c"].is_current


def test_tie_on_archive_time_is_broken_by_checksum(make):
    bronze = make(
        [
            row("1", "claims_header", "2023-01", "b", 1),
            row("1", "claims_header", "2023-01", "a", 1),
        ]
    )
    index = index_by_checksum(bronze)
    assert index["b"].is_current
    assert not index["a"].is_current


def test_current_rows_keeps_only_current_deliveries(make):
    bronze = make(
        [
            row("old", "claims_header", "2023-01", "a", 1),
            row("new", "claims_header", "2023-01", "b", 2),
            row("feb", "claims_header", "2023-02", "c", 3),
        ]
    )
    current = current_rows(bronze)
    assert current.columns == bronze.columns
    assert sorted(r.ID for r in current.collect()) == ["feb", "new"]
