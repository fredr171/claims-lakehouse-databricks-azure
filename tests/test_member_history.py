"""Member history (SCD2 reference), on hand-built staged rows.

Every scenario is its own member in one table, built once per module.
"""

from datetime import datetime

import pytest

from claims_lakehouse.silver.member_history import current_members, member_versions

SCHEMA = (
    "member string, plan string, city string, coverage_end string, "
    "_delivery_month string, _archived_at timestamp"
)
TRACKED = ["plan", "city", "coverage_end"]


def at(minute: int) -> datetime:
    return datetime(2026, 1, 1, 0, minute)


SCENARIOS = [
    # Versions start only on a tracked change; NULL city on both rows is not a change.
    ("CHANGES", "A", None, None, "2023-01", at(1)),
    ("CHANGES", "A", None, None, "2023-02", at(2)),
    ("CHANGES", "B", None, None, "2023-03", at(3)),
    ("CHANGES", "B", None, "2023-06-30", "2023-06", at(4)),
    # February arrives (is archived) before January.
    ("LATE_JANUARY", "B", None, None, "2023-02", at(1)),
    ("LATE_JANUARY", "A", None, None, "2023-01", at(2)),
    # The first row is already ended (NEW, not TERMINATE); then coverage resumes.
    ("RETURNS", "A", None, "2023-12-31", "2023-01", at(1)),
    ("RETURNS", "A", None, None, "2024-06", at(2)),
]


@pytest.fixture(scope="module")
def versions(spark):
    staged = spark.createDataFrame(SCENARIOS, SCHEMA)
    result = member_versions(staged, "member", TRACKED, coverage_end="coverage_end").collect()
    by_member: dict[str, list[dict]] = {}
    for row in sorted(result, key=lambda r: (r.member, r.version_number)):
        by_member.setdefault(row.member, []).append(row.asDict())
    return by_member


def test_versions_start_only_on_a_tracked_change(versions):
    rows = versions["CHANGES"]
    assert [(r["valid_from"], r["valid_to"], r["change_type"]) for r in rows] == [
        ("2023-01", "2023-03", "NEW"),
        ("2023-03", "2023-06", "UPDATE"),
        ("2023-06", None, "TERMINATE"),
    ]
    assert [r["changed_columns"] for r in rows] == [[], ["plan"], ["coverage_end"]]
    assert [r["last_delivery_month"] for r in rows] == ["2023-02", "2023-03", "2023-06"]
    assert [r["is_current"] for r in rows] == [False, False, True]
    assert {r["first_delivery_month"] for r in rows} == {"2023-01"}


def test_versions_follow_delivery_month_not_arrival_order(versions):
    assert [(r["valid_from"], r["plan"], r["change_type"]) for r in versions["LATE_JANUARY"]] == [
        ("2023-01", "A", "NEW"),
        ("2023-02", "B", "UPDATE"),
    ]


def test_returning_after_termination_is_an_update(versions):
    rows = versions["RETURNS"]
    assert [r["change_type"] for r in rows] == ["NEW", "UPDATE"]
    assert rows[1]["changed_columns"] == ["coverage_end"]


def test_current_members_is_the_latest_version(spark):
    staged = spark.createDataFrame(SCENARIOS, SCHEMA)
    current = current_members(member_versions(staged, "member", TRACKED, "coverage_end"))
    assert sorted((r.member, r.valid_from) for r in current.collect()) == [
        ("CHANGES", "2023-06"),
        ("LATE_JANUARY", "2023-02"),
        ("RETURNS", "2024-06"),
    ]


def test_coverage_end_must_be_tracked(spark):
    staged = spark.createDataFrame([], SCHEMA)
    with pytest.raises(ValueError, match="must be a tracked column"):
        member_versions(staged, "member", ["plan"], coverage_end="coverage_end")
