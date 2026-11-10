"""Stop vs warn gating across four job runs (months 2023-01 to 2023-04, both payers).

run 1  01 clean; 02 with an odd CCLF claim end date      CCLF warned, Humana passed
run 2  03 with a broken Humana claims extract             Humana stopped, CCLF refreshed
run 3  04 clean Humana; 04 with a bad CCLF action code    both stopped (Humana still,
                                                          blocked by 03)
run 4  corrected re-sends of Humana 03 and CCLF 04        both recover; the result
                                                          equals a clean load of the
                                                          same current files
"""

from decimal import Decimal

import pytest
from harness import LocalArchive, edited_copy, generated_file
from lakehouse import LocalLakehouse, snapshot

from claims_lakehouse.silver.quality import Action

pytestmark = pytest.mark.slow

CMS, HUMANA = "cms_ffs", "humana_ma"


def odd_claim_end_date(rows):
    rows[0]["CLM_THRU_DT"] = rows[0]["CLM_THRU_DT"].replace("-", "/")  # a WARN format check
    return rows


def unknown_action_code(rows):
    rows[0]["CLM_ADJSMT_TYPE_CD"] = "X"  # a FAIL row expectation
    return rows


def paid_above_allowed(rows):
    """A broken extract: well over 1% (and at least 5) of the month's lines break a rule."""
    paid_lines = [r for r in rows if r["CLAIM_STATUS"] == "P" and r["CAPITATED_FLAG"] == "N"]
    for r in paid_lines[:6]:
        r["PAID_AMT"] = f"{Decimal(r['ALLOWED_AMT']) + 1:.2f}"
    return rows


@pytest.fixture(scope="module")
def run(spark, config, generated, tmp_path_factory):
    """Runs the four job runs once; returns each run's outcomes and published gold."""
    root = tmp_path_factory.mktemp("gating")
    archive = LocalArchive(root, config)
    lakehouse = LocalLakehouse(spark, config, archive)

    def file(file_type, month):
        return generated_file(config, generated, file_type, month)

    warned = edited_copy(file("claims_header", "2023-02"), root / "warn", odd_claim_end_date)
    broken_humana = edited_copy(
        file("medical_claim_lines", "2023-03"), root / "broken", paid_above_allowed
    )
    broken_cclf = edited_copy(
        file("claims_header", "2023-04"), root / "broken", unknown_action_code
    )

    runs = []

    def job_run():
        outcomes = lakehouse.update()
        gold = snapshot(lakehouse.gold)
        columns, dim_payer = gold.tables["dim_payer"]
        payer, last = columns.index("payer_id"), columns.index("last_delivery_month")
        runs.append(
            {
                "outcomes": outcomes,
                "gold": gold,
                "humana_gold": gold.payer(HUMANA),
                "humana_silver": snapshot(lakehouse.published[HUMANA]),
                "windows": {row[payer]: row[last] for row in dim_payer},
            }
        )

    archive.deliver_month(generated, "2023-01")
    archive.deliver_month(generated, "2023-02", replace={warned.name: warned})
    job_run()
    archive.deliver_month(generated, "2023-03", replace={broken_humana.name: broken_humana})
    job_run()
    archive.deliver_month(generated, "2023-04", replace={broken_cclf.name: broken_cclf})
    job_run()
    archive.deliver_generated(generated, "humana", "medical_claim_lines", "2023-03")
    archive.deliver_generated(generated, "cclf", "claims_header", "2023-04")
    job_run()

    # The reference: a clean load of the files that are current after run 4.
    reference = LocalArchive(root / "reference", config)
    reference.deliver_month(generated, "2023-01")
    reference.deliver_month(generated, "2023-02", replace={warned.name: warned})
    reference.deliver_month(generated, "2023-03")
    reference.deliver_month(generated, "2023-04")
    clean = LocalLakehouse(spark, config, reference)
    clean.update()
    return runs, snapshot(clean.gold), snapshot(clean.published[HUMANA]), lakehouse


def statuses(run_):
    return {p: o.status for p, o in run_["outcomes"].items()}


def test_a_warn_does_not_block(run):
    runs, *_ = run
    assert statuses(runs[0]) == {CMS: "warned", HUMANA: "passed"}
    [problem] = runs[0]["outcomes"][CMS].problems
    assert (problem.table, problem.expectation, problem.action, problem.failed_rows) == (
        "claims_header_current",
        "CLM_THRU_DT_format",
        Action.WARN,
        1,
    )
    assert runs[0]["windows"] == {CMS: _month(2), HUMANA: _month(2)}


def test_a_stop_holds_back_its_payer_and_the_other_payer_still_refreshes(run):
    runs, *_ = run
    assert statuses(runs[1]) == {CMS: "warned", HUMANA: "stopped"}
    assert runs[1]["outcomes"][HUMANA].stopping == {"rules_mostly_hold"}
    # Humana's silver and its slice of gold are exactly what run 1 published.
    assert runs[1]["humana_silver"] == runs[0]["humana_silver"]
    assert runs[1]["humana_gold"] == runs[0]["humana_gold"]
    # CCLF moved on to 2023-03.
    assert runs[1]["windows"] == {CMS: _month(3), HUMANA: _month(2)}


def test_a_stop_keeps_blocking_later_months_until_a_resend(run):
    runs, *_ = run
    assert statuses(runs[2]) == {CMS: "stopped", HUMANA: "stopped"}
    assert runs[2]["outcomes"][HUMANA].stopping == {"rules_mostly_hold"}  # still 2023-03
    assert runs[2]["outcomes"][CMS].stopping == {"CLM_ADJSMT_TYPE_CD_domain"}
    assert runs[2]["gold"] == runs[1]["gold"]  # nothing new is published anywhere


def test_corrected_resends_recover_to_a_clean_load(run):
    runs, clean_gold, clean_humana, _ = run
    assert statuses(runs[3]) == {CMS: "warned", HUMANA: "passed"}  # the 02 warn is current
    assert runs[3]["windows"] == {CMS: _month(4), HUMANA: _month(4)}
    assert runs[3]["humana_silver"] == clean_humana
    assert runs[3]["gold"] == clean_gold


def test_problems_carry_only_identifiers_and_counts(run):
    runs, *_ = run
    for run_ in runs:
        for outcome in run_["outcomes"].values():
            for problem in outcome.problems:
                assert isinstance(problem.failed_rows, int)
                assert {type(problem.table), type(problem.expectation)} == {str}


def _month(n):
    from datetime import date

    return date(2023, n, 1)
