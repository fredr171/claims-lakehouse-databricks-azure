"""MA payer silver against the generator's answer keys (all 36 months, loaded in order).

The answer keys are written from the generator's own simulation, not by re-reading the
CSVs, so matching them exactly is an independent check of staging, member history and
claim families.
"""

import calendar
import csv
import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from claims_lakehouse.silver.humana import tables as humana_tables
from claims_lakehouse.silver.humana.amount_rules import amount_problem
from claims_lakehouse.silver.humana.staging import MEMBER_ELIGIBILITY, MEMBER_TRACKED_COLUMNS
from claims_lakehouse.silver.quality import evaluate

pytestmark = pytest.mark.slow

# final_members.csv column -> staged column: every tracked attribute. FILE_AS_OF_DT is not
# tracked; it is checked against the version's last delivery month instead.
MEMBER_COLUMNS = {
    source: target
    for target, source, _ in MEMBER_ELIGIBILITY
    if target in ("member_id", *MEMBER_TRACKED_COLUMNS)
}

# member_changes.csv reason -> (derived change type, columns of which one must have changed).
CHANGE_REASONS = {
    "NEW": ("NEW", ()),
    "ADDRESS": ("UPDATE", ("address_line_1",)),
    "PCP": ("UPDATE", ("pcp_npi",)),
    "DUAL_LIS": ("UPDATE", ("dual_flag", "low_income_subsidy_flag")),
    "DEATH": ("TERMINATE", ("death_date",)),
    "DISENROLL": ("TERMINATE", ("enrollment_end_date",)),  # to FFS or another plan
}
FORMS = {"I": "institutional", "P": "professional"}


@pytest.fixture(scope="module")
def silver(spark, full_load):
    archive, _ = full_load
    bronze = {ft: archive.bronze(spark, ft).cache() for ft in humana_tables.FILE_TYPES}
    tables = humana_tables.build(bronze)
    for name in ("member_versions", "claim_versions", "claim_families", "stg_medical_claim_lines"):
        tables[name] = tables[name].cache()
    yield tables
    for df in [*bronze.values(), *tables.values()]:
        df.unpersist()


@pytest.fixture(scope="module")
def key(generated):
    def read(name):
        with (generated / "key" / "humana" / name).open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    return read


@pytest.fixture(scope="module")
def summary(generated):
    return json.loads((generated / "key" / "humana" / "summary.json").read_text(encoding="utf-8"))


def text(value) -> str:
    """A Spark value as the answer keys write it: ISO dates, '' for NULL."""
    return "" if value is None else str(value)


def month_end(month: str) -> str:
    year, mon = map(int, month.split("-"))
    return f"{month}-{calendar.monthrange(year, mon)[1]:02d}"


def amount(value) -> Decimal:
    return Decimal(value) if value else Decimal("0.00")


# --- Members ------------------------------------------------------------------------------


def test_current_members_match_final_members(silver, key):
    expected = {row["MEMBER_ID"]: row for row in key("final_members.csv")}
    current = silver["member_versions"].where("is_current").collect()
    assert len(current) == len(expected)
    for row in current:
        want = expected[row.member_id]
        got = {source: text(row[target]) for source, target in MEMBER_COLUMNS.items()}
        assert got == {source: want[source] for source in MEMBER_COLUMNS}, row.member_id
        assert (row.first_delivery_month, row.last_delivery_month) == (
            want["FIRST_DELIVERY_MONTH"],
            want["LAST_DELIVERY_MONTH"],
        ), row.member_id
        assert month_end(row.last_delivery_month) == want["FILE_AS_OF_DT"], row.member_id


def test_every_member_change_is_a_version_with_the_right_type(silver, key):
    changes = key("member_changes.csv")
    versions = {
        (r.member_id, r.valid_from): (r.change_type, set(r.changed_columns))
        for r in silver["member_versions"].collect()
    }
    assert len(versions) == len(changes)  # every delivered row is a real change
    for change in changes:
        change_type, changed = versions[(change["MEMBER_ID"], change["delivery_month"])]
        want_type, columns = CHANGE_REASONS[change["change_type"]]
        assert change_type == want_type, change
        assert (not changed) if not columns else changed & set(columns), change


def test_derived_change_type_agrees_with_the_generator_helper(silver):
    """RECORD_CHANGE_TYPE (NEW/UPDATE/TERM) is a synthetic helper, never used; it must
    agree with what we derive."""
    helper_to_derived = F.create_map(
        *[F.lit(x) for x in ("NEW", "NEW", "UPDATE", "UPDATE", "TERM", "TERMINATE")]
    )
    staged = silver["stg_member_eligibility"].select(
        "member_id", F.col("_delivery_month").alias("valid_from"), "record_change_type"
    )
    joined = staged.join(silver["member_versions"], ["member_id", "valid_from"], "left")
    disagreements = joined.where(
        F.col("change_type").isNull()
        | (F.col("change_type") != helper_to_derived[F.col("record_change_type")])
    )
    assert disagreements.count() == 0


# --- Claims -------------------------------------------------------------------------------


def test_claim_actions_match(silver, key):
    expected = {r["original_claim_id"]: r for r in key("claim_actions.csv")}
    actions = silver["claim_families"].where("action_type is not null").collect()
    assert len(actions) == len(expected)
    for row in actions:
        want = expected[row.claim_family_key]
        got = [
            row.action_type,
            row.original_claim_id,
            row.member_id,
            row.claim_form_type_code,
            row.original_delivery_month,
            text(row.original_paid_amount),
            row.action_delivery_month,
            text(row.replacement_claim_id),
            text(row.replacement_paid_amount),
            text(row.net_paid_amount),
        ]
        assert got == [
            want["action_type"],
            want["original_claim_id"],
            want["MEMBER_ID"],
            want["CLAIM_FORM_TYPE"],
            want["original_processed_month"],  # processed in its delivery month
            want["original_paid"],
            want["action_processed_month"],
            want["replacement_claim_id"],
            want["replacement_paid"],
            want["final_paid"],
        ], row.claim_family_key


def test_net_paid_by_service_month_matches(silver, key):
    rows = (
        silver["claim_families"]
        .groupBy("service_month")
        .pivot("claim_form_type_code", list(FORMS))
        .agg(F.sum("net_paid_amount"))
        .collect()
    )
    got = {r.service_month: (amount(r.I), amount(r.P)) for r in rows}
    expected = {
        r["service_month"]: (
            Decimal(r["institutional_net_paid"]),
            Decimal(r["professional_net_paid"]),
        )
        for r in key("net_paid_by_service_month.csv")
    }
    assert got == expected


def test_net_paid_is_also_the_sum_of_every_version(silver):
    """Reversals negate their original, so the status rule and a plain sum agree."""
    families = silver["claim_families"]
    assert families.where("not has_original").count() == 0
    assert families.where("net_paid_amount != sum_of_version_paid").count() == 0


def test_paid_by_processed_month_matches(silver, key):
    rows = (
        silver["claim_versions"]
        .groupBy("delivery_month")
        .pivot("claim_form_type_code", list(FORMS))
        .agg(F.sum("paid_amount"))
        .collect()
    )
    got = {r.delivery_month: (amount(r.I), amount(r.P)) for r in rows}
    expected = {
        r["processed_month"]: (Decimal(r["institutional_paid"]), Decimal(r["professional_paid"]))
        for r in key("net_paid_by_processed_month.csv")
    }
    assert got == expected


def test_capitated_encounters_by_month_match(silver, key):
    rows = (
        silver["claim_families"]
        .where("is_capitated")
        .groupBy("service_month")
        .agg(
            F.count(F.lit(1)).alias("claims"),
            F.sum("original_line_count").alias("lines"),
            F.sum("capitated_allowed_amount").alias("allowed"),
        )
        .collect()
    )
    got = {r.service_month: (r.claims, r.lines, r.allowed) for r in rows}
    expected = {
        r["service_month"]: (
            int(r["capitated_claims"]),
            int(r["capitated_lines"]),
            Decimal(r["capitated_allowed_amt"]),
        )
        for r in key("capitated_encounters_by_month.csv")
    }
    assert got == expected


def test_totals_and_counts_match_the_summary(silver, summary):
    families = silver["claim_families"]
    totals = {
        r.claim_form_type_code: r.total
        for r in families.groupBy("claim_form_type_code")
        .agg(F.sum("net_paid_amount").alias("total"))
        .collect()
    }
    assert (text(totals["I"]), text(totals["P"])) == (
        summary["institutional_total_net_paid"],
        summary["professional_total_net_paid"],
    )
    originals = {
        r.claim_form_type_code: r["count"]
        for r in families.where("has_original").groupBy("claim_form_type_code").count().collect()
    }
    assert (originals["I"], originals["P"]) == (
        summary["institutional_original_claims"],
        summary["professional_original_claims"],
    )
    statuses = {
        r.family_status: r["count"] for r in families.groupBy("family_status").count().collect()
    }
    assert statuses["REPLACED"] == summary["replacements"]
    assert statuses["VOIDED"] == summary["voids"]
    assert statuses["DENIED"] == summary["denied"]
    assert families.where("is_capitated").count() == summary["capitated_claims"]
    members = silver["member_versions"].where("is_current")
    assert members.count() == summary["members"]
    assert members.where("death_date is not null").count() == summary["deaths"]


def test_generated_lines_break_no_amount_rule(silver):
    """The rules pass on clean data (test_humana_amount_rules shows each one failing)."""
    lines = silver["stg_medical_claim_lines"]
    problems = lines.select(amount_problem().alias("problem")).where("problem is not null")
    assert problems.groupBy("problem").count().collect() == []


def test_member_id_and_mbi_are_one_to_one(silver):
    """MEMBER_ID and MBI stay 1:1, so gold can key people by MBI across payers."""
    members = silver["member_versions"]
    assert (
        members.groupBy("member_id")
        .agg(F.countDistinct("beneficiary_mbi").alias("n"))
        .where("n > 1")
        .count()
        == 0
    )
    assert (
        members.groupBy("beneficiary_mbi")
        .agg(F.countDistinct("member_id").alias("n"))
        .where("n > 1")
        .count()
        == 0
    )


def test_clean_deliveries_pass_every_expectation(silver):
    """Every row expectation and gate table passes on the generated data (the quality
    tests show each one failing on broken data)."""
    for table, expectations in humana_tables.EXPECTATIONS.items():
        _, results = evaluate(silver[table], expectations)
        assert [r for r in results if not r.passed] == [], table
