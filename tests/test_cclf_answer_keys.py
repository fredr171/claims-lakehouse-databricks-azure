"""CCLF silver against the generator's answer keys (all 36 months, loaded in order).

The answer keys are written from the generator's own simulation, not by re-reading the
CSVs, so matching them exactly is an independent check of staging, member history and
claim families.
"""

import csv
import json
from collections import defaultdict
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.cclf.staging import (
    BENEFICIARY_DEMOGRAPHICS,
    BENEFICIARY_TRACKED_COLUMNS,
)
from claims_lakehouse.silver.quality import evaluate

pytestmark = pytest.mark.slow

# final_members.csv column -> staged column: every tracked attribute (age is not tracked).
MEMBER_COLUMNS = {
    source: target
    for target, source, _ in BENEFICIARY_DEMOGRAPHICS
    if target in ("beneficiary_mbi", *BENEFICIARY_TRACKED_COLUMNS)
}

# member_changes.csv reason -> (derived change type, the column that must have changed).
CHANGE_REASONS = {
    "NEW": ("NEW", None),
    "ADDRESS": ("UPDATE", "address_line_1"),
    "DUAL": ("UPDATE", "dual_status_code"),
    "PARTB": ("UPDATE", "part_b_enrollment_start_date"),
    "DEATH": ("TERMINATE", "death_date"),
    "FFS_END": ("TERMINATE", "coverage_end_date"),  # switched to Medicare Advantage
    "FFS_RETURN": ("UPDATE", "coverage_start_date"),  # back from Medicare Advantage
}


@pytest.fixture(scope="module")
def silver(spark, full_load):
    archive, _ = full_load
    bronze = {ft: archive.bronze(spark, ft).cache() for ft in cclf_tables.FILE_TYPES}
    tables = cclf_tables.build(bronze)
    for name in ("beneficiary_versions", "claim_families", "stg_beneficiary_demographics"):
        tables[name] = tables[name].cache()
    yield tables
    for df in [*bronze.values(), *tables.values()]:
        df.unpersist()


@pytest.fixture(scope="module")
def key(generated):
    def read(name):
        with (generated / "key" / name).open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    return read


def text(value) -> str:
    """A Spark value as the answer keys write it: ISO dates, 2-decimal amounts, '' for NULL."""
    return "" if value is None else str(value)


def test_current_members_match_final_members(silver, key):
    expected = {row["BENE_MBI_ID"]: row for row in key("final_members.csv")}
    current = silver["beneficiary_versions"].where("is_current").collect()
    assert len(current) == len(expected) == 426
    for row in current:
        want = expected[row.beneficiary_mbi]
        got = {source: text(row[target]) for source, target in MEMBER_COLUMNS.items()}
        assert got == {source: want[source] for source in MEMBER_COLUMNS}, row.beneficiary_mbi
        assert (row.first_delivery_month, row.last_delivery_month) == (
            want["FIRST_DELIVERY_MONTH"],
            want["LAST_DELIVERY_MONTH"],
        ), row.beneficiary_mbi


def test_every_member_change_is_a_version_with_the_right_type(silver, key):
    changes = key("member_changes.csv")
    versions = {
        (r.beneficiary_mbi, r.valid_from): (r.change_type, set(r.changed_columns))
        for r in silver["beneficiary_versions"].collect()
    }
    assert len(versions) == len(changes) == 931  # every delivered row is a real change
    for change in changes:
        change_type, changed = versions[(change["BENE_MBI_ID"], change["delivery_month"])]
        want_type, column = CHANGE_REASONS[change["change_type"]]
        assert change_type == want_type, change
        assert (not changed) if column is None else column in changed, change


def test_derived_change_type_agrees_with_the_generator_helper(silver):
    """RECORD_CHANGE_TYPE is a synthetic helper, never used; it must agree with what we derive."""
    staged = silver["stg_beneficiary_demographics"].select(
        "beneficiary_mbi", F.col("_delivery_month").alias("valid_from"), "record_change_type"
    )
    joined = staged.join(silver["beneficiary_versions"], ["beneficiary_mbi", "valid_from"], "left")
    disagreements = joined.where(
        F.col("change_type").isNull()
        | ((F.col("change_type") == "NEW") != (F.col("record_change_type") == "NEW"))
    )
    assert disagreements.count() == 0


def test_claim_actions_match(silver, key):
    expected = {(r["part"], r["CLM_CNTL_NUM"]): r for r in key("claim_actions.csv")}
    actions = silver["claim_families"].where("action_type is not null").collect()
    assert len(actions) == len(expected) == 104
    for row in actions:
        want = expected[(row.part, row.claim_control_number)]
        got = [
            row.action_type,
            row.beneficiary_mbi,
            row.original_claim_id,
            row.original_delivery_month,
            text(row.original_amount),
            row.action_claim_id,
            row.action_delivery_month,
            text(row.action_amount),
            text(row.net_paid_amount),
        ]
        assert got == [
            want["action_type"],
            want["BENE_MBI_ID"],
            want["original_clm_id"],
            want["original_load_month"],
            want["original_amount"],
            want["action_clm_id"],
            want["action_load_month"],
            want["action_amount"],
            want["final_amount"],
        ], row.claim_family_key


def test_net_paid_by_service_month_matches(silver, key):
    rows = (
        silver["claim_families"]
        .groupBy("service_month")
        .pivot("part", ["A", "B"])
        .agg(F.sum("net_paid_amount"))
        .collect()
    )
    got = {r.service_month: (r.A or Decimal("0.00"), r.B or Decimal("0.00")) for r in rows}
    expected = {
        r["service_month"]: (Decimal(r["part_a_net_paid"]), Decimal(r["part_b_net_paid"]))
        for r in key("net_paid_by_service_month.csv")
    }
    assert got == expected


def test_net_change_by_load_month_matches(silver, key):
    """Originals book in their own month; an adjustment books its delta and a cancellation
    reverses the original, both in the action's month. Sums to the same grand total."""
    booked: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for f in silver["claim_families"].collect():
        if f.has_original:
            booked[(f.original_delivery_month, f.part)] += f.original_amount
        if f.action_type == "ADJUST":
            booked[(f.action_delivery_month, f.part)] += f.action_amount - f.original_amount
        elif f.action_type == "CANCEL":
            booked[(f.action_delivery_month, f.part)] -= f.original_amount
    expected = {}
    for r in key("net_paid_by_load_month.csv"):
        expected[(r["load_month"], "A")] = Decimal(r["part_a_net_change"])
        expected[(r["load_month"], "B")] = Decimal(r["part_b_net_change"])
    assert {k: v for k, v in booked.items() if v} == {k: v for k, v in expected.items() if v}


def test_totals_and_counts_match_the_summary(silver, generated):
    summary = json.loads((generated / "key" / "summary.json").read_text(encoding="utf-8"))
    families = silver["claim_families"]
    totals = {
        r.part: r.total
        for r in families.groupBy("part").agg(F.sum("net_paid_amount").alias("total")).collect()
    }
    assert (text(totals["A"]), text(totals["B"])) == (
        summary["part_a_total_net_paid"],
        summary["part_b_total_net_paid"],
    )
    originals = {
        r.part: r["count"] for r in families.where("has_original").groupBy("part").count().collect()
    }
    assert (originals["A"], originals["B"]) == (
        summary["part_a_original_claims"],
        summary["part_b_original_claims"],
    )
    actions = {r.action_type: r["count"] for r in families.groupBy("action_type").count().collect()}
    assert (actions["CANCEL"], actions["ADJUST"]) == (
        summary["cancellations"],
        summary["adjustments"],
    )
    members = silver["beneficiary_versions"].where("is_current")
    assert members.count() == summary["beneficiaries"]
    assert members.where("death_date is not null").count() == summary["deaths"]


def test_clean_deliveries_pass_every_expectation(silver):
    """Every row expectation and gate table passes on the generated data (the quality
    tests show each one failing on broken data)."""
    for table, expectations in cclf_tables.EXPECTATIONS.items():
        _, results = evaluate(silver[table], expectations)
        assert [r for r in results if not r.passed] == [], table
