"""MA payer claim families, on hand-built claim versions.

Every scenario is its own family in one table, built once: one Spark query for the
whole module instead of one per test.
"""

from datetime import date
from decimal import Decimal

import pytest

from claims_lakehouse.silver.ma_payer.claim_families import claim_families, claim_versions

VERSIONS = (
    "claim_family_key string, claim_id string, claim_frequency_code string, "
    "delivery_month string, claim_status_code string, claim_form_type_code string, "
    "member_id string, beneficiary_mbi string, contract_id string, claim_start_date date, "
    "processed_date date, line_count long, paid_amount decimal(18,2), "
    "allowed_amount decimal(18,2), is_capitated boolean"
)
FREQUENCIES = {"original": "1", "replacement": "7", "reversal": "8"}


def version(family, claim_id, kind, paid, month="2023-01", status=None, capitated=False):
    year, mon = map(int, month.split("-"))
    status = status or ("R" if kind == "reversal" else "P")
    allowed = Decimal("50.00") if capitated else abs(Decimal(paid))
    return (family, claim_id, FREQUENCIES[kind], month, status, "P", "H000000001", "MBI1",
            "H9101", date(2022, 12, 15), date(year, mon, 10), 2, Decimal(paid), allowed,
            capitated)  # fmt: skip


SCENARIOS = [
    version("PAID", "PAID", "original", "100.00"),
    version("DENIED", "DENIED", "original", "0.00", status="D"),
    version("CAPITATED", "CAPITATED", "original", "0.00", capitated=True),
    # A void: the reversal repeats the original's claim_id.
    version("VOIDED", "VOIDED", "original", "100.00"),
    version("VOIDED", "VOIDED", "reversal", "-100.00", "2023-02"),
    # A replacement: a reversal of the original plus a new claim_id, in one delivery.
    version("REPLACED", "REPLACED", "original", "100.00"),
    version("REPLACED", "REPLACED", "reversal", "-100.00", "2023-02"),
    version("REPLACED", "R1", "replacement", "90.00", "2023-02"),
    # The replacement is replaced again: the active one wins.
    version("CHAIN", "CHAIN", "original", "100.00"),
    version("CHAIN", "CHAIN", "reversal", "-100.00", "2023-02"),
    version("CHAIN", "C1", "replacement", "90.00", "2023-02"),
    version("CHAIN", "C1", "reversal", "-90.00", "2023-03"),
    version("CHAIN", "C2", "replacement", "80.00", "2023-03"),
    # The only replacement is reversed later: nothing is active.
    version("CHAIN_VOID", "CHAIN_VOID", "original", "100.00"),
    version("CHAIN_VOID", "CHAIN_VOID", "reversal", "-100.00", "2023-02"),
    version("CHAIN_VOID", "V1", "replacement", "90.00", "2023-02"),
    version("CHAIN_VOID", "V1", "reversal", "-90.00", "2023-03"),
    # An active replacement beats a later reversed one, whatever the order.
    version("ACTIVE_FIRST", "ACTIVE_FIRST", "original", "100.00"),
    version("ACTIVE_FIRST", "ACTIVE_FIRST", "reversal", "-100.00", "2023-02"),
    version("ACTIVE_FIRST", "A2", "replacement", "70.00", "2023-03"),
    version("ACTIVE_FIRST", "A2", "reversal", "-70.00", "2023-03"),
    version("ACTIVE_FIRST", "A1", "replacement", "60.00", "2023-02"),
    # A reversal whose original is not loaded nets to 0, not to a negative amount.
    version("NO_ORIGINAL", "NO_ORIGINAL", "reversal", "-100.00", "2023-02"),
]


@pytest.fixture(scope="module")
def families(spark):
    result = claim_families(spark.createDataFrame(SCENARIOS, VERSIONS))
    return {r.claim_family_key: r.asDict() for r in result.collect()}


def summary(family):
    return family["family_status"], family["net_paid_amount"], family["action_type"]


def test_original_only(families):
    family = families["PAID"]
    assert summary(family) == ("ORIGINAL", Decimal("100.00"), None)
    assert family["service_month"] == "2022-12"
    assert (family["version_count"], family["original_count"]) == (1, 1)


def test_denied_original(families):
    assert summary(families["DENIED"]) == ("DENIED", Decimal("0.00"), None)


def test_capitated_original_keeps_its_allowed_amount(families):
    family = families["CAPITATED"]
    assert summary(family) == ("ORIGINAL", Decimal("0.00"), None)
    assert family["is_capitated"] is True
    assert family["capitated_allowed_amount"] == Decimal("50.00")
    assert families["PAID"]["is_capitated"] is False
    assert families["PAID"]["capitated_allowed_amount"] is None


def test_void(families):
    family = families["VOIDED"]
    assert summary(family) == ("VOIDED", Decimal("0.00"), "VOID")
    assert family["action_delivery_month"] == "2023-02"
    assert family["replacement_claim_id"] is None


def test_replacement(families):
    family = families["REPLACED"]
    assert summary(family) == ("REPLACED", Decimal("90.00"), "REPLACE")
    assert (family["replacement_claim_id"], family["replacement_paid_amount"]) == (
        "R1",
        Decimal("90.00"),
    )
    assert (family["original_claim_id"], family["original_paid_amount"]) == (
        "REPLACED",
        Decimal("100.00"),
    )
    assert (family["reversal_count"], family["replacement_count"]) == (1, 1)


def test_replacement_of_a_replacement(families):
    family = families["CHAIN"]
    assert summary(family) == ("REPLACED", Decimal("80.00"), "REPLACE")
    assert family["replacement_claim_id"] == "C2"
    assert family["action_delivery_month"] == "2023-03"


def test_reversed_replacement_leaves_nothing_active(families):
    assert summary(families["CHAIN_VOID"]) == ("VOIDED", Decimal("0.00"), "REPLACE")


def test_active_replacement_wins_over_a_later_reversed_one(families):
    family = families["ACTIVE_FIRST"]
    assert (family["replacement_claim_id"], family["net_paid_amount"]) == ("A1", Decimal("60.00"))


def test_net_paid_equals_the_sum_of_versions_when_history_is_complete(families):
    complete = [f for f in families.values() if f["has_original"]]
    assert all(f["net_paid_amount"] == f["sum_of_version_paid"] for f in complete)


def test_reversal_without_its_original(families):
    family = families["NO_ORIGINAL"]
    assert family["has_original"] is False
    assert summary(family) == ("VOIDED", Decimal("0.00"), "VOID")
    assert family["sum_of_version_paid"] == Decimal("-100.00")
    assert family["service_month"] == "2022-12"  # from the earliest version


def test_lines_roll_up_to_versions_with_the_family_key(spark):
    lines = spark.createDataFrame(
        [
            (
                "O1",
                1,
                None,
                "1",
                "P",
                "P",
                "H1",
                "M1",
                "H9101",
                date(2023, 1, 2),
                date(2023, 1, 20),
                Decimal("12.50"),
                Decimal("20.00"),
                "N",
                "2023-01",
            ),
            (
                "O1",
                2,
                None,
                "1",
                "P",
                "P",
                "H1",
                "M1",
                "H9101",
                date(2023, 1, 3),
                date(2023, 1, 20),
                Decimal("7.25"),
                Decimal("10.00"),
                "Y",
                "2023-01",
            ),
            (
                "O1",
                1,
                "O1",
                "8",
                "R",
                "P",
                "H1",
                "M1",
                "H9101",
                date(2023, 1, 2),
                date(2023, 2, 20),
                Decimal("-12.50"),
                Decimal("-20.00"),
                "N",
                "2023-02",
            ),
            (
                "N1",
                1,
                "O1",
                "7",
                "P",
                "P",
                "H1",
                "M1",
                "H9101",
                date(2023, 1, 2),
                date(2023, 2, 20),
                Decimal("11.00"),
                Decimal("18.00"),
                "N",
                "2023-02",
            ),
        ],  # fmt: skip
        "claim_id string, line_number int, original_claim_id string, "
        "claim_frequency_code string, claim_status_code string, claim_form_type_code string, "
        "member_id string, beneficiary_mbi string, contract_id string, "
        "claim_start_date date, processed_date date, paid_amount decimal(18,2), "
        "allowed_amount decimal(18,2), capitated_flag string, _delivery_month string",
    )
    rows = {(r.claim_id, r.claim_frequency_code): r for r in claim_versions(lines).collect()}
    assert {r.claim_family_key for r in rows.values()} == {"O1"}
    original = rows[("O1", "1")]
    assert (original.line_count, original.paid_amount, original.allowed_amount) == (
        2,
        Decimal("19.75"),
        Decimal("30.00"),
    )
    assert original.claim_start_date == date(2023, 1, 2)
    assert original.is_capitated is True
    assert rows[("O1", "8")].delivery_month == "2023-02"
