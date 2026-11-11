"""Quality gates: the expectation evaluator, the row-expectation builders and gate tables."""

import pytest
from pyspark.sql import functions as F

from claims_lakehouse.silver.common import AMOUNT, DATE, INTEGER, QUANTITY
from claims_lakehouse.silver.quality import (
    KEYS_UNIQUE,
    RULE_BREAK_EXPECTATIONS,
    Action,
    Expectation,
    duplicate_key_check,
    evaluate,
    format_check,
    format_checks,
    one_of,
    present,
    rule_breaks_by_month,
)

# --- evaluate() follows Lakeflow's warn / drop / fail ---------------------------------------


NUMBER_EXPECTATIONS = [
    Expectation("warn_small", "n < 3", Action.WARN),
    Expectation("drop_odd", "n % 2 = 0", Action.DROP),
    Expectation("fail_small", "n < 3", Action.FAIL),
    Expectation("fail_huge", "n IS NULL OR n < 100", Action.FAIL),
]


@pytest.fixture(scope="module")
def evaluated(spark):
    """One evaluate() over 1, 2, 3, 4 and NULL; kept rows collected once."""
    numbers = spark.createDataFrame([(1,), (2,), (3,), (4,), (None,)], "n int")
    kept, results = evaluate(numbers, NUMBER_EXPECTATIONS)
    return sorted(r.n for r in kept.collect()), {r.name: r for r in results}


def test_warn_keeps_rows_and_counts_them(evaluated):
    _, results = evaluated
    warn = results["warn_small"]
    assert (warn.failed_rows, warn.passed, warn.stops) == (3, False, False)  # 3, 4 and NULL


def test_drop_removes_failing_rows_and_only_those(evaluated):
    kept, results = evaluated
    assert kept == [2, 4]  # a NULL result counts as failing; WARN and FAIL remove nothing
    assert (results["drop_odd"].failed_rows, results["drop_odd"].stops) == (3, False)


def test_fail_stops_only_when_a_row_fails(evaluated):
    _, results = evaluated
    assert results["fail_small"].stops
    assert not results["fail_huge"].stops and results["fail_huge"].passed


def test_expectation_names_must_be_unique(spark):
    e = Expectation("x", "true", Action.WARN)
    with pytest.raises(ValueError, match="duplicate expectation names"):
        evaluate(spark.range(1), [e, e])


# --- Row-expectation builders ------------------------------------------------------------------

# (type, delivered text, passes): blank is allowed; anything try_cast would bend is not.
FORMAT_CASES = [
    (DATE, "2023-01-05", True),
    (DATE, "", True),
    (DATE, None, True),
    (DATE, "2023-1-5", False),
    (DATE, "2023-02-30", False),
    (DATE, "01/05/2023", False),
    (DATE, " 2023-01-05", False),
    (DATE, "2023-01-05 00:00:00", False),
    (AMOUNT, "12.34", True),
    (AMOUNT, "-12.34", True),
    (AMOUNT, "0.00", True),
    (AMOUNT, "12.345", False),
    (AMOUNT, "12.3", False),
    (AMOUNT, "12", False),
    (AMOUNT, "1,000.00", False),
    (AMOUNT, "$5.00", False),
    (AMOUNT, "1e3", False),
    (INTEGER, "7", True),
    (INTEGER, "007", True),
    (INTEGER, "1.0", False),
    (INTEGER, "x", False),
    (INTEGER, "99999999999", False),
    (QUANTITY, "1", True),
    (QUANTITY, "1.5", True),
    (QUANTITY, "1.250", True),
    (QUANTITY, "1.2345", False),
    (QUANTITY, "1.", False),
]


@pytest.fixture(scope="module")
def format_results(spark):
    rows = [(i, value) for i, (_, value, _) in enumerate(FORMAT_CASES)]
    df = spark.createDataFrame(rows, "case int, V string")
    checks = {
        dtype: F.coalesce(F.expr(format_check("V", dtype).constraint), F.lit(False))
        for dtype in (DATE, AMOUNT, INTEGER, QUANTITY)
    }
    result = df.select("case", *[c.alias(dtype) for dtype, c in checks.items()]).collect()
    return {r.case: r.asDict() for r in result}


@pytest.mark.parametrize(("index", "case"), list(enumerate(FORMAT_CASES)))
def test_format_check(format_results, index, case):
    dtype, _, passes = case
    assert format_results[index][dtype] is passes


def test_present_and_one_of(spark):
    df = spark.createDataFrame([("A",), ("",), ("  ",), (None,), ("Z",)], "V string")
    checks = [present("V"), one_of("V", ["A", "B"]), one_of("V", ["A"], allow_blank=True)]
    rows = df.select(
        *[
            F.coalesce(F.expr(e.constraint), F.lit(False)).alias(e.name + str(i))
            for i, e in enumerate(checks)
        ]  # fmt: skip
    ).collect()
    assert [tuple(r) for r in rows] == [
        (True, True, True),
        (False, False, True),
        (False, False, True),
        (False, False, True),
        (True, False, False),
    ]
    assert [e.action for e in checks] == [Action.FAIL, Action.WARN, Action.WARN]


def test_format_checks_cover_typed_columns_only():
    mapping = [("a", "A", "string"), ("b", "B", DATE), ("c", "C", AMOUNT)]
    checks = format_checks(mapping, stop_columns=["C"])
    assert [(e.name, e.action) for e in checks] == [
        ("B_format", Action.WARN),
        ("C_format", Action.FAIL),
    ]
    with pytest.raises(ValueError, match="not in the staging map"):
        format_checks(mapping, stop_columns=["Z"])


# --- Gate tables ---------------------------------------------------------------------------------


def test_duplicate_keys_within_a_file_or_across_files(spark):
    current = spark.createDataFrame(
        [("2023-01", "K1"), ("2023-01", "K1"), ("2023-01", "K2"), ("2023-02", "K2")],
        "_delivery_month string, KEY string",
    )
    within = {r._delivery_month: r for r in duplicate_key_check(current, ["KEY"]).collect()}
    assert (within["2023-01"].row_count, within["2023-01"].duplicate_key_rows) == (3, 2)
    assert within["2023-02"].duplicate_key_rows == 0

    across = duplicate_key_check(current, ["KEY"], across_months=True).cache()
    counts = {r._delivery_month: r.duplicate_key_rows for r in across.collect()}
    assert counts == {"2023-01": 3, "2023-02": 1}
    _, [result] = evaluate(across, [KEYS_UNIQUE])
    assert result.stops and result.failed_rows == 2  # both months fail


# month -> (lines, broken lines, warns, stops)
RULE_BREAK_MONTHS = {
    "2023-01": (100, 0, False, False),
    "2023-02": (10, 4, True, False),  # 40%, but fewer than 5 lines: an odd small file
    "2023-03": (500, 5, True, False),  # exactly 1%: not more than 1%
    "2023-04": (400, 5, True, True),  # 1.25% and 5 lines
    "2023-05": (100, 50, True, True),  # a broken extract
}


@pytest.fixture(scope="module")
def rule_breaks(spark):
    rows = [
        (month, "broken" if i < broken else None)
        for month, (lines, broken, _, _) in RULE_BREAK_MONTHS.items()
        for i in range(lines)
    ]
    lines = spark.createDataFrame(rows, "_delivery_month string, problem string")
    return rule_breaks_by_month(lines, F.col("problem")).cache()


def test_rule_breaks_stop_above_one_percent_and_five_lines(rule_breaks):
    got = {
        r._delivery_month: (r.line_count, r.broken_lines, r.is_stopped)
        for r in rule_breaks.collect()
    }
    assert got == {
        m: (lines, broken, stops) for m, (lines, broken, _, stops) in RULE_BREAK_MONTHS.items()
    }


def test_rule_break_expectations_warn_then_stop(rule_breaks):
    _, [warn, stop] = evaluate(rule_breaks, RULE_BREAK_EXPECTATIONS)
    assert (warn.action, warn.failed_rows) == (Action.WARN, 4)
    assert (stop.action, stop.failed_rows, stop.stops) == (Action.FAIL, 2, True)
    _, results = evaluate(
        rule_breaks.where("_delivery_month <= '2023-03'"), RULE_BREAK_EXPECTATIONS
    )
    assert not any(r.stops for r in results)
