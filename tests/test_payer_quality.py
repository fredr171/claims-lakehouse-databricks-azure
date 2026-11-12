"""Every payer expectation passes on a real delivered row and fails on one broken to break it.

The broken row for each expectation is derived from its name, so a new expectation that
no rule below can break fails test_every_expectation_has_a_broken_row.
"""

import csv
import re

import pytest
from pyspark.sql import functions as F

from claims_lakehouse.silver.cclf import quality as cclf_quality
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.cclf.staging import STAGING as CCLF_STAGING
from claims_lakehouse.silver.common import AMOUNT, DATE, INTEGER, QUANTITY
from claims_lakehouse.silver.ma_payer import quality as ma_payer_quality
from claims_lakehouse.silver.ma_payer import tables as ma_payer_tables
from claims_lakehouse.silver.ma_payer.staging import STAGING as MA_PAYER_STAGING

ROW_EXPECTATIONS = {**cclf_quality.ROW_EXPECTATIONS, **ma_payer_quality.ROW_EXPECTATIONS}
TYPES = {
    source: dtype
    for mapping in (*CCLF_STAGING.values(), *MA_PAYER_STAGING.values())
    for _, source, dtype in mapping
}
BAD_TEXT = {DATE: "2023-1-5", AMOUNT: "12.345", INTEGER: "1.5", QUANTITY: "1.2345"}
FOLDERS = {"cclf": "cms", "ma_payer": "ma_payer"}

# Expectations whose broken row is not derived from the name.
SPECIAL = {"ORIG_CLAIM_ID_links_action": {"CLAIM_FREQ_CD": "7", "ORIG_CLAIM_ID": ""}}


def broken_row(name: str) -> dict[str, str] | None:
    if name in SPECIAL:
        return SPECIAL[name]
    match = re.fullmatch(r"(.+)_(format|present|domain)", name)
    if match is None:
        return None
    column, check = match.groups()
    return {
        column: {"format": BAD_TEXT.get(TYPES.get(column)), "present": "", "domain": "X"}[check]
    }


def test_every_expectation_has_a_broken_row():
    for file_type, expectations in ROW_EXPECTATIONS.items():
        for e in expectations:
            override = broken_row(e.name)
            assert override and None not in override.values(), (file_type, e.name)


def test_tables_carry_the_row_expectations():
    assert cclf_tables.EXPECTATIONS["claims_header_current"] == ROW_EXPECTATIONS["claims_header"]
    assert set(ma_payer_tables.EXPECTATIONS) >= {
        "member_eligibility_current",
        "medical_claim_lines_key_check",
        "medical_claim_line_amount_check",
    }


def delivered_row(config, generated, file_type: str) -> dict[str, str | None]:
    """A real row from the 2023-02 delivery (an original claim), empty fields as NULL."""
    ft = config.file_types[file_type]
    folder = generated / FOLDERS[ft.feed.name] / "2023-02"
    [path] = [p for p in folder.iterdir() if re.match(ft.file_name_regex, p.name)]
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    row = next(r for r in rows if r.get("CLAIM_FREQ_CD", "1") == "1")
    return {k: (v if v != "" else None) for k, v in row.items()}


@pytest.fixture(scope="module")
def outcomes(spark, config, generated):
    """file type -> case -> names of the expectations its row fails.

    Every file type's rows go in one wide table (a column a file type lacks is NULL), and
    every expectation is one column: a single Spark query for the module.
    """
    rows = []
    for file_type, expectations in ROW_EXPECTATIONS.items():
        base = delivered_row(config, generated, file_type)
        rows.append((file_type, "delivered", base))
        rows += [(file_type, e.name, base | broken_row(e.name)) for e in expectations]
    columns = sorted({c for _, _, row in rows for c in row})
    df = spark.createDataFrame(
        [(ft, case, *[row.get(c) for c in columns]) for ft, case, row in rows],
        ", ".join(["file_type string", "case string", *[f"{c} string" for c in columns]]),
    )
    checks = [(ft, e) for ft, expectations in ROW_EXPECTATIONS.items() for e in expectations]
    holds = [
        F.coalesce(F.expr(e.constraint), F.lit(False)).alias(f"_{i}")
        for i, (_, e) in enumerate(checks)
    ]
    result: dict[str, dict[str, set[str]]] = {ft: {} for ft in ROW_EXPECTATIONS}
    for r in df.select("file_type", "case", *holds).collect():
        result[r.file_type][r.case] = {
            e.name for i, (ft, e) in enumerate(checks) if ft == r.file_type and not r[f"_{i}"]
        }
    return result


@pytest.mark.parametrize("file_type", list(ROW_EXPECTATIONS))
def test_a_delivered_row_passes_every_expectation(outcomes, file_type):
    assert outcomes[file_type]["delivered"] == set()


@pytest.mark.parametrize(
    ("file_type", "name"),
    [(ft, e.name) for ft, expectations in ROW_EXPECTATIONS.items() for e in expectations],
)
def test_expectation_fails_on_its_broken_row(outcomes, file_type, name):
    assert name in outcomes[file_type][name]
