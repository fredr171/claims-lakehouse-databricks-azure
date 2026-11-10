"""CCLF claim families, on hand-built claim versions.

Every scenario is its own family (control number) in one table, built once: one Spark
query for the whole module instead of one per test.
"""

from datetime import date
from decimal import Decimal

import pytest

from claims_lakehouse.silver.cclf.claim_families import claim_families, claim_versions

VERSIONS = (
    "part string, claim_control_number string, claim_id string, beneficiary_mbi string, "
    "adjustment_type_code string, claim_start_date date, claim_load_date date, "
    "delivery_month string, paid_amount decimal(18,2)"
)
KINDS = {"original": "0", "cancel": "1", "adjust": "2"}


def version(control, claim_id, kind, amount, month="2023-01", load_day=5, part="A"):
    year, mon = map(int, month.split("-"))
    load_date = date(year, mon, load_day)
    return (part, control, claim_id, "MBI1", KINDS[kind], date(2022, 12, 15), load_date, month,
            Decimal(amount))  # fmt: skip


SCENARIOS = [
    version("ORIGINAL_ONLY", "1", "original", "100.00"),
    version("CANCELLED", "1", "original", "100.00"),
    version("CANCELLED", "2", "cancel", "-100.00", "2023-02"),
    version("ADJUSTED", "1", "original", "100.00"),
    version("ADJUSTED", "2", "adjust", "80.00", "2023-02"),
    # A cancellation processed before a later adjustment still wins.
    version("CANCEL_AND_ADJUST", "1", "original", "100.00"),
    version("CANCEL_AND_ADJUST", "2", "cancel", "-100.00", "2023-02"),
    version("CANCEL_AND_ADJUST", "3", "adjust", "80.00", "2023-03"),
    # Two adjustments: the latest claim load date wins, not the higher claim_id.
    version("TWO_ADJUSTMENTS", "1", "original", "100.00"),
    version("TWO_ADJUSTMENTS", "9", "adjust", "90.00", "2023-02"),
    version("TWO_ADJUSTMENTS", "3", "adjust", "80.00", "2023-03"),
    version("NO_ORIGINAL", "2", "cancel", "-100.00", "2023-02"),
    version("BOTH_PARTS", "1", "original", "100.00", part="A"),
    version("BOTH_PARTS", "7", "original", "20.00", part="B"),
]


@pytest.fixture(scope="module")
def families(spark):
    result = claim_families(spark.createDataFrame(SCENARIOS, VERSIONS))
    return {r.claim_family_key: r.asDict() for r in result.collect()}


def test_original_only(families):
    family = families["A:ORIGINAL_ONLY"]
    assert family["family_status"] == "ORIGINAL"
    assert family["net_paid_amount"] == Decimal("100.00")
    assert family["action_type"] is None
    assert family["service_month"] == "2022-12"
    assert (family["version_count"], family["original_count"], family["action_count"]) == (1, 1, 0)


def test_cancellation_nets_to_zero(families):
    family = families["A:CANCELLED"]
    assert family["family_status"] == "CANCELLED"
    assert family["net_paid_amount"] == Decimal("0.00")
    assert (family["action_type"], family["action_claim_id"]) == ("CANCEL", "2")
    assert (family["original_delivery_month"], family["action_delivery_month"]) == (
        "2023-01",
        "2023-02",
    )


def test_adjustment_replaces_the_amount(families):
    family = families["A:ADJUSTED"]
    assert family["family_status"] == "ADJUSTED"
    assert family["net_paid_amount"] == Decimal("80.00")  # a replacement, not a delta


def test_cancellation_beats_a_later_adjustment(families):
    family = families["A:CANCEL_AND_ADJUST"]
    assert family["family_status"] == "CANCELLED"
    assert family["net_paid_amount"] == Decimal("0.00")
    assert family["action_count"] == 2


def test_latest_processed_adjustment_wins(families):
    family = families["A:TWO_ADJUSTMENTS"]
    assert (family["action_claim_id"], family["net_paid_amount"]) == ("3", Decimal("80.00"))


def test_action_without_its_original_still_forms_a_family(families):
    family = families["A:NO_ORIGINAL"]
    assert family["has_original"] is False
    assert family["original_claim_id"] is None
    assert family["family_status"] == "CANCELLED"
    assert family["service_month"] == "2022-12"  # from the earliest version


def test_parts_are_separate_families(families):
    assert families["A:BOTH_PARTS"]["net_paid_amount"] == Decimal("100.00")
    assert families["B:BOTH_PARTS"]["net_paid_amount"] == Decimal("20.00")


def test_part_b_lines_roll_up_to_one_version(spark):
    headers = spark.createDataFrame(
        [],
        "claim_control_number string, claim_id string, beneficiary_mbi string, "
        "adjustment_type_code string, claim_start_date date, claim_load_date date, "
        "_delivery_month string, payment_amount decimal(18,2)",
    )
    lines = spark.createDataFrame(
        [
            (
                "C9",
                "9",
                "MBI1",
                "0",
                date(2023, 1, 2),
                date(2023, 1, 20),
                "2023-01",
                Decimal("12.50"),
            ),
            (
                "C9",
                "9",
                "MBI1",
                "0",
                date(2023, 1, 3),
                date(2023, 1, 20),
                "2023-01",
                Decimal("7.25"),
            ),
        ],  # fmt: skip
        "claim_control_number string, claim_id string, beneficiary_mbi string, "
        "adjustment_type_code string, claim_start_date date, claim_load_date date, "
        "_delivery_month string, line_paid_amount decimal(18,2)",
    )
    [row] = claim_versions(headers, lines).collect()
    assert (row.part, row.claim_id, row.paid_amount) == ("B", "9", Decimal("19.75"))
    assert row.claim_start_date == date(2023, 1, 2)
