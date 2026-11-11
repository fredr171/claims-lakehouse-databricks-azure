"""Quality gates for silver: row-level expectations and per-month gate tables.

Expectations are (name, SQL constraint, action), exactly what Lakeflow Declarative
Pipelines take (@dp.expect / expect_or_drop / expect_or_fail), so the phase 3 pipeline
attaches the same definitions. evaluate() is the local reference for their behavior:
    WARN  the row is kept and counted
    DROP  the row is removed and counted
    FAIL  the update stops: the table and everything downstream keep their last good state
Constraints are written to never evaluate to NULL (blank checks use IS NULL OR ...);
evaluate() counts a NULL result as a failure, the conservative reading.

Row expectations run on each file's current-delivery rows, as delivered text, before
staging types them. That is where formats are checked: try_cast is lenient ('2023-1-5'
is a date, '12.345' rounds to 12.35), so a typed value cannot show what was delivered.
The file's structure (header, field counts, non-empty keys) is already guaranteed by
file-level validation.

Some rules are not row-level: one odd line only warns, but many broken lines in a month
mean a broken extract, and duplicate keys only show across rows. Those are gate tables,
one row per delivery month, with WARN and FAIL expectations on their counts. A gate table
reads every current delivery, so a bad month keeps failing every later update until a
corrected re-send replaces it.

Naming: <SOURCE_COLUMN>_<check>, e.g. CLM_FROM_DT_format, CLAIM_FREQ_CD_domain.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from claims_lakehouse.silver.common import AMOUNT, DATE, INTEGER, QUANTITY, STRING, StagingMap

# --- Expectations -------------------------------------------------------------------------


class Action(Enum):
    WARN = "warn"
    DROP = "drop"
    FAIL = "fail"


@dataclass(frozen=True)
class Expectation:
    name: str
    constraint: str  # a Spark SQL boolean expression
    action: Action


@dataclass(frozen=True)
class ExpectationResult:
    name: str
    action: Action
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    @property
    def stops(self) -> bool:
        return self.action is Action.FAIL and not self.passed


def _holds(expectation: Expectation) -> Column:
    return F.coalesce(F.expr(expectation.constraint), F.lit(False))


def evaluate(
    df: DataFrame, expectations: Sequence[Expectation]
) -> tuple[DataFrame, list[ExpectationResult]]:
    """Counts each expectation's failing rows (one Spark job); returns the kept rows.

    Kept rows are df minus rows failing a DROP expectation. When any FAIL expectation has
    a failing row, the caller must not publish anything (see ExpectationResult.stops).
    """
    names = [e.name for e in expectations]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate expectation names: {sorted(names)}")
    if not expectations:
        return df, []
    counts = df.agg(
        *[F.sum((~_holds(e)).cast("long")).alias(f"_{i}") for i, e in enumerate(expectations)]
    ).first()
    results = [
        ExpectationResult(e.name, e.action, counts[f"_{i}"] or 0)
        for i, e in enumerate(expectations)
    ]
    kept = df
    for e in expectations:
        if e.action is Action.DROP:
            kept = kept.where(_holds(e))
    return kept, results


# --- Row expectation builders ---------------------------------------------------------------

_FORMATS = {
    # type -> (regex the delivered text must match, also checked with try_cast)
    DATE: "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    AMOUNT: "^-?[0-9]{1,16}[.][0-9]{2}$",
    INTEGER: "^-?[0-9]{1,9}$",
    QUANTITY: "^-?[0-9]{1,15}([.][0-9]{1,3})?$",
}


def _blank(column: str) -> str:
    return f"({column} IS NULL OR trim({column}) = '')"


def format_check(column: str, dtype: str, action: Action = Action.WARN) -> Expectation:
    """Blank, or the exact text format of the type (and a real value: no 2023-02-30)."""
    regex = _FORMATS[dtype]
    return Expectation(
        f"{column}_format",
        f"{_blank(column)} OR ({column} RLIKE '{regex}' "
        f"AND try_cast({column} AS {dtype}) IS NOT NULL)",
        action,
    )


def present(column: str, action: Action = Action.FAIL) -> Expectation:
    return Expectation(f"{column}_present", f"NOT {_blank(column)}", action)


def one_of(
    column: str, values: Iterable[str], action: Action = Action.WARN, allow_blank: bool = False
) -> Expectation:
    listed = ", ".join(f"'{v}'" for v in values)
    check = f"coalesce({column} IN ({listed}), false)"
    return Expectation(
        f"{column}_domain", f"{_blank(column)} OR {check}" if allow_blank else check, action
    )


def format_checks(mapping: StagingMap, stop_columns: Iterable[str] = ()) -> list[Expectation]:
    """A format check for every typed column of a staging map; FAIL for stop_columns."""
    stop = set(stop_columns)
    sources = {source for _, source, _ in mapping}
    if unknown := stop - sources:
        raise ValueError(f"stop columns not in the staging map: {sorted(unknown)}")
    return [
        format_check(source, dtype, Action.FAIL if source in stop else Action.WARN)
        for _, source, dtype in mapping
        if dtype != STRING
    ]


# --- Gate tables ------------------------------------------------------------------------------

MONTH = "_delivery_month"


def duplicate_key_check(
    current: DataFrame, key: Sequence[str], across_months: bool = False
) -> DataFrame:
    """One row per delivery month: rows whose key is shared with another current row.

    across_months: a key must be unique over every current delivery (claim versions),
    not just within its file (members, delivered again whenever they change).
    """
    scope = [*key] if across_months else [*key, MONTH]
    shared = F.count(F.lit(1)).over(Window.partitionBy(*scope)) > 1
    return (
        current.select(MONTH, shared.cast("long").alias("_shared"))
        .groupBy(MONTH)
        .agg(F.count(F.lit(1)).alias("row_count"), F.sum("_shared").alias("duplicate_key_rows"))
    )


KEYS_UNIQUE = Expectation("keys_unique", "duplicate_key_rows = 0", Action.FAIL)

# A month stops when more than this share of its lines break a rule, and at least
# RULE_BREAK_MIN_LINES do (so one odd line in a tiny file does not stop a payer).
RULE_BREAK_STOP_RATIO = 0.01
RULE_BREAK_MIN_LINES = 5


def rule_breaks_by_month(lines: DataFrame, problem: Column) -> DataFrame:
    """One row per delivery month: lines breaking a rule (problem not NULL), and whether
    that is enough to stop."""
    broken = problem.isNotNull().cast("long")
    by_month = lines.groupBy(MONTH).agg(
        F.count(F.lit(1)).alias("line_count"), F.sum(broken).alias("broken_lines")
    )
    ratio = F.col("broken_lines") / F.col("line_count")
    return by_month.withColumn("broken_ratio", ratio).withColumn(
        "is_stopped",
        (ratio > RULE_BREAK_STOP_RATIO) & (F.col("broken_lines") >= RULE_BREAK_MIN_LINES),
    )


RULE_BREAK_EXPECTATIONS = [
    Expectation("rules_hold", "broken_lines = 0", Action.WARN),
    Expectation("rules_mostly_hold", "NOT is_stopped", Action.FAIL),
]
