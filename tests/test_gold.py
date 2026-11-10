"""Gold marts on hand-built conformed views, under a registry with every payer lifecycle.

Registry: alpha (active), beta (retired in 2023-03), gamma (registered, no data yet).
Feed fd has conformed views but no registry entry (offboarded): it must vanish from gold.
Built once for the module; each mart is collected once.
"""

import dataclasses
from datetime import date
from decimal import Decimal

import pytest

from claims_lakehouse.config import Payer
from claims_lakehouse.gold.build import build_gold
from claims_lakehouse.gold.contract import CONTRACT, ContractError, check_contract
from claims_lakehouse.gold.marts import read_sql, registry_relation, render

PAYERS = (
    Payer("alpha", "Alpha O'Brien \\ Co", "Medicare Advantage", "fa", "2023-01", None),
    Payer("beta", "Beta", "Medicare FFS", "fb", "2023-01", "2023-03"),
    Payer("gamma", "Gamma", "Medicare Advantage", "fc", "2023-06", None),
)


def schema(kind):
    return ", ".join(f"{name} {dtype}" for name, dtype in CONTRACT[kind])


def d(text):
    return date.fromisoformat(text) if text else None


def span(payer, mbi, start, end, last, state="WI", member=None):
    return (payer, member or f"{payer}-{mbi}", mbi, d(start), d(end), f"{payer}-plan", 1950,
            "F", state, False, False, d(last))  # fmt: skip


def claim(payer, claim_id, mbi, month, status, amount):
    return (payer, claim_id, f"{payer}-{mbi}", mbi, "PROFESSIONAL", status, status,
            d(f"{month}-05"), d(f"{month}-01"), d("2023-01-01"), d("2023-01-01"),
            Decimal(amount), False)  # fmt: skip


def deliveries(payer, months):
    return [(payer, "eligibility", d(f"{m}-01"), f"sha-{payer}-{m}") for m in months]


COVERAGE = {
    "fa": [
        span("alpha", "M1", "2022-06-15", None, "2023-04-01"),  # open: bounded by the window
        span("alpha", "M2", "2023-01-31", "2023-02-01", "2023-02-01"),  # any day of a month
        span("alpha", "M2", "2023-02-01", "2023-02-28", "2023-02-01"),  # overlap counts once
        span("alpha", "M3", "2023-06-01", None, "2023-04-01"),  # after the window: no months
        span("alpha", "SWITCH", "2023-02-01", None, "2023-04-01", state="WI"),
    ],
    "fb": [
        span("beta", "SWITCH", "2020-01-01", "2023-01-31", "2023-01-01", state="IL"),
        span("beta", "RETIRED", "2020-01-01", None, "2023-03-01"),
    ],
    "fd": [span("delta", "GONE", "2023-01-01", None, "2023-01-01")],
}
CLAIMS = {
    "fa": [
        claim("alpha", "C1", "M1", "2023-02", "PAID", "100.00"),
        claim("alpha", "C2", "M1", "2023-02", "VOIDED", "0.00"),
        claim("alpha", "C3", "M2", "2022-11", "PAID", "50.00"),  # before the window
    ],
    "fb": [claim("beta", "C4", "RETIRED", "2023-01", "ADJUSTED", "30.00")],
    "fd": [claim("delta", "C5", "GONE", "2023-01", "PAID", "9.00")],
}
DELIVERIES = {
    "fa": deliveries("alpha", ["2023-01", "2023-02", "2023-03", "2023-04"]),
    "fb": deliveries("beta", ["2023-01", "2023-02", "2023-03"]),
    "fd": deliveries("delta", ["2023-01"]),
}


@pytest.fixture(scope="module")
def registry_config(config):
    return dataclasses.replace(config, payers=PAYERS)


@pytest.fixture(scope="module")
def conformed(spark):
    return {
        feed: {
            "coverage": spark.createDataFrame(COVERAGE[feed], schema("coverage")),
            "claims": spark.createDataFrame(CLAIMS[feed], schema("claims")),
            "deliveries": spark.createDataFrame(DELIVERIES[feed], schema("deliveries")),
        }
        for feed in COVERAGE
    }


@pytest.fixture(scope="module")
def gold(spark, registry_config, conformed):
    marts = build_gold(spark, registry_config, conformed)
    return {name: [r.asDict() for r in df.collect()] for name, df in marts.items()}


def by(rows, *keys):
    return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r for r in rows}


# --- dim_payer: the registry is the only source of payers ------------------------------------


def test_dim_payer_follows_the_registry(gold):
    payers = by(gold["dim_payer"], "payer_id")
    assert set(payers) == {"alpha", "beta", "gamma"}  # delta is offboarded
    assert {p: r["payer_status"] for p, r in payers.items()} == {
        "alpha": "active",
        "beta": "retired",
        "gamma": "awaiting_data",
    }
    assert (payers["alpha"]["first_delivery_month"], payers["alpha"]["last_delivery_month"]) == (
        date(2023, 1, 1),
        date(2023, 4, 1),
    )
    assert payers["alpha"]["current_file_count"] == 4
    assert payers["gamma"]["current_file_count"] == 0
    assert payers["beta"]["retired_month"] == date(2023, 3, 1)


def test_registry_literals_are_escaped(gold):
    assert by(gold["dim_payer"], "payer_id")["alpha"]["payer_name"] == "Alpha O'Brien \\ Co"


# --- dim_member: one row per person across payers -----------------------------------------------


def test_dim_member_is_one_row_per_mbi_across_payers(gold):
    members = by(gold["dim_member"], "member_key")
    assert set(members) == {"M1", "M2", "M3", "SWITCH", "RETIRED"}  # not GONE (offboarded)
    switch = members["SWITCH"]
    assert (switch["payer_count"], switch["is_multi_payer"], switch["coverage_span_count"]) == (
        2,
        True,
        2,
    )
    assert switch["current_payer_id"] == "alpha"
    assert switch["state_code"] == "WI"  # from the most recently delivered span
    assert switch["first_coverage_start_date"] == date(2020, 1, 1)


def test_open_span_at_a_retired_payer_is_not_current_coverage(gold):
    assert by(gold["dim_member"], "member_key")["RETIRED"]["current_payer_id"] is None


# --- fct_member_months: any day of a month, inside the payer's delivery window ------------------


def test_member_months(gold):
    months = sorted((r["payer_id"], r["member_key"], r["month"].isoformat()[:7])
                    for r in gold["fct_member_months"])  # fmt: skip
    assert months == sorted(
        [("alpha", "M1", m) for m in ("2023-01", "2023-02", "2023-03", "2023-04")]
        + [("alpha", "M2", "2023-01"), ("alpha", "M2", "2023-02")]
        + [("alpha", "SWITCH", m) for m in ("2023-02", "2023-03", "2023-04")]
        + [("beta", "SWITCH", "2023-01")]
        + [("beta", "RETIRED", m) for m in ("2023-01", "2023-02", "2023-03")]
    )


# --- fct_claims and agg_payer_monthly -------------------------------------------------------------


def test_fct_claims_has_registered_payers_only(gold):
    assert sorted(r["claim_key"] for r in gold["fct_claims"]) == [
        "alpha:C1",
        "alpha:C2",
        "alpha:C3",
        "beta:C4",
    ]


def test_pmpm_is_net_paid_by_service_month_per_member_month(gold):
    agg = by(gold["agg_payer_monthly"], "payer_id", "month")
    feb = agg[("alpha", date(2023, 2, 1))]
    assert (feb["member_months"], feb["claim_count"], feb["net_paid_amount"]) == (
        3,
        1,  # the voided claim is not counted
        Decimal("100.00"),
    )
    assert feb["pmpm_net_paid"] == Decimal("33.33")
    before = agg[("alpha", date(2022, 11, 1))]  # a service month before the window
    assert (before["member_months"], before["pmpm_net_paid"]) == (0, None)
    assert agg[("alpha", date(2023, 3, 1))]["pmpm_net_paid"] == Decimal("0.00")


def test_marts_reconcile(gold):
    assert gold["check_marts_reconcile"] == []


def test_reconcile_check_fails_when_the_summary_loses_a_row(spark, gold):
    lost = "payer_id = 'alpha' AND month = DATE'2023-02-01'"
    tampered = spark.sql(f"SELECT * FROM gold_agg_payer_monthly WHERE NOT ({lost})")
    tampered.createOrReplaceTempView("tampered_agg")
    values = {"agg_payer_monthly": "tampered_agg"} | {
        m: f"gold_{m}" for m in ("fct_claims", "fct_member_months")
    }
    rows = spark.sql(render(read_sql("claims_lakehouse.gold", "check_marts_reconcile"), values))
    assert [r.payer_id for r in rows.collect()] == ["alpha"]


# --- The conformed contract -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda df: df.drop("plan_code"), "missing plan_code"),
        (
            lambda df: df.withColumn("birth_year", df.birth_year.cast("string")),
            "birth_year is string",
        ),
        (lambda df: df.withColumn("first_name", df.sex), "unexpected first_name"),
        (lambda df: df.select(*reversed(df.columns)), "columns out of order"),
    ],
)
def test_contract_rejects_a_view_that_differs(conformed, change, message):
    view = conformed["fa"]["coverage"]
    check_contract(view, "coverage", "alpha")
    with pytest.raises(ContractError, match=message):
        check_contract(change(view), "coverage", "alpha")


def test_gold_stops_on_a_broken_conformed_view(spark, registry_config, conformed):
    broken = {
        **conformed,
        "fb": {**conformed["fb"], "claims": conformed["fb"]["claims"].drop("is_capitated")},
    }
    with pytest.raises(ContractError, match="beta conformed_claims: missing is_capitated"):
        build_gold(spark, registry_config, broken)


def test_render_refuses_a_missing_placeholder():
    with pytest.raises(KeyError, match="claims"):
        render(read_sql("claims_lakehouse.gold", "fct_claims"), {})


def test_registry_types_every_column_even_when_all_null(spark):
    """No payer retired yet: retired_month is still a date column, not NULL-typed."""
    registry = spark.sql(f"SELECT * FROM {registry_relation(PAYERS[:1])}")
    assert dict(registry.dtypes)["retired_month"] == "date"
    assert dict(registry.dtypes)["onboarded_month"] == "date"
