"""MA payer line amount rules: every rule fails on a line deliberately broken to break it."""

from decimal import Decimal

import pytest

from claims_lakehouse.silver.ma_payer.amount_rules import amount_problem

LINES = (
    "case string, claim_frequency_code string, claim_status_code string, "
    "capitated_flag string, billed_amount decimal(18,2), allowed_amount decimal(18,2), "
    "paid_amount decimal(18,2), copay_amount decimal(18,2), coinsurance_amount decimal(18,2), "
    "deductible_amount decimal(18,2)"
)


def line(case, billed, allowed, paid, copay="0.00", coins="0.00", ded="0.00", freq="1",
         status="P", capitated="N"):  # fmt: skip
    amounts = [
        None if a is None else Decimal(a) for a in (billed, allowed, paid, copay, coins, ded)
    ]
    return (case, freq, status, capitated, *amounts)


CASES = [
    # (line, expected problem)
    (line("good", "200.00", "150.00", "120.00", copay="10.00", coins="20.00"), None),
    (line("good replacement", "200.00", "150.00", "150.00", freq="7"), None),
    (line("good denied", "200.00", "0.00", "0.00", status="D"), None),
    (line("good capitated", "200.00", "150.00", "0.00", copay="10.00", capitated="Y"), None),
    (line("good reversal", "-200.00", "-150.00", "-120.00", freq="8", status="R"), None),
    (line("good null cost share", "200.00", "150.00", "150.00", copay=None), None),
    (line("missing paid", "200.00", "150.00", None), "missing amount"),
    (
        line("positive reversal", "-200.00", "-150.00", "120.00", freq="8", status="R"),
        "reversal with a positive amount",
    ),
    (line("negative paid", "200.00", "150.00", "-1.00"), "negative amount"),
    (line("paid above allowed", "200.00", "150.00", "160.00"), "paid above allowed"),
    (line("allowed above billed", "100.00", "150.00", "150.00"), "allowed above billed"),
    (
        line("denied but paid", "200.00", "150.00", "150.00", status="D"),
        "denied line with allowed or paid",
    ),
    (
        line("cost share off", "200.00", "150.00", "120.00", copay="10.00"),
        "cost share is not allowed - paid",
    ),
]


@pytest.fixture(scope="module")
def problems(spark):
    lines = spark.createDataFrame([c for c, _ in CASES], LINES)
    return {
        r.case: r.problem for r in lines.select("case", amount_problem().alias("problem")).collect()
    }


@pytest.mark.parametrize(("case", "expected"), [(c[0], e) for c, e in CASES])
def test_amount_rule(problems, case, expected):
    assert problems[case] == expected
