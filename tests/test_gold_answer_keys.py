"""Gold against both payers' answer keys (all 36 months, both feeds, loaded in order).

Silver -> each payer's conformed views -> the marts, exactly as the job will run them.
Member months are checked for every month against an independent count from the delivered
eligibility files (the generator's own coverage-span rule), not just the last month. The
job's own check (check_gold) must pass here and report each kind of broken gold.
"""

import csv
import json
import re
import shutil
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from claims_lakehouse.checks.answer_keys import (
    SWITCHERS,
    check_gold,
    expected_member_months,
    missing_months,
    write_member_month_keys,
)
from claims_lakehouse.gold.build import build_conformed, build_gold
from claims_lakehouse.gold.marts import MARTS
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.humana import tables as humana_tables

pytestmark = pytest.mark.slow

PAYERS = {"cclf": "cms_ffs", "humana": "humana_ma"}
KEY_FILES = {"cms_ffs": "key", "humana_ma": "key/humana"}
# Columns that must never reach gold (they stay in silver, behind column masks).
PROTECTED = {"first_name", "middle_name", "middle_initial", "last_name", "address_line_1",
             "city", "birth_date", "zip5_code", "zip9_code"}  # fmt: skip


@pytest.fixture(scope="module")
def gold(spark, config, full_load):
    archive, _ = full_load
    builders = {"cclf": cclf_tables, "humana": humana_tables}
    bronze = {
        ft: archive.bronze(spark, ft).cache() for b in builders.values() for ft in b.FILE_TYPES
    }
    conformed = {}
    for feed, builder in builders.items():
        silver = builder.build({ft: bronze[ft] for ft in builder.FILE_TYPES})
        views = build_conformed(spark, feed, PAYERS[feed], silver)
        conformed[feed] = {kind: df.cache() for kind, df in views.items()}
    marts = build_gold(spark, config, conformed)
    yield marts
    for df in [*bronze.values(), *[v for views in conformed.values() for v in views.values()],
               *marts.values()]:  # fmt: skip
        df.unpersist()


def read_key(generated, payer_id, name):
    with (generated / KEY_FILES[payer_id] / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_registry_payers_are_active_over_the_whole_window(gold):
    rows = {r.payer_id: r for r in gold["dim_payer"].collect()}
    assert set(rows) == set(PAYERS.values())
    for r in rows.values():
        assert (r.payer_status, r.first_delivery_month, r.last_delivery_month) == (
            "active",
            date(2023, 1, 1),
            date(2025, 12, 1),
        )
    assert rows["cms_ffs"].current_file_count == 36 * 4
    assert rows["humana_ma"].current_file_count == 36 * 2


@pytest.mark.parametrize("payer_id", list(PAYERS.values()))
def test_net_paid_by_service_month_matches_the_answer_key(gold, generated, payer_id):
    expected = {
        r["service_month"]: Decimal(r["total_net_paid"])
        for r in read_key(generated, payer_id, "net_paid_by_service_month.csv")
    }
    rows = (
        gold["fct_claims"]
        .where(F.col("payer_id") == payer_id)
        .groupBy(F.date_format("service_month", "yyyy-MM").alias("month"))
        .agg(F.sum("net_paid_amount").alias("net_paid"))
        .collect()
    )
    assert {r.month: r.net_paid for r in rows} == expected
    [total] = (
        gold["agg_payer_monthly"]
        .where(F.col("payer_id") == payer_id)
        .agg(F.sum("net_paid_amount"))
        .first()
    )
    assert total == sum(expected.values())


def test_claim_statuses_are_conformed_across_payers(gold, generated):
    """CANCELLED (CCLF) and VOIDED (Humana) are one status in gold, as are ADJUSTED and
    REPLACED; the counts come from both payers' summaries."""
    cms = json.loads((generated / "key" / "summary.json").read_text(encoding="utf-8"))
    hum = json.loads((generated / "key" / "humana" / "summary.json").read_text(encoding="utf-8"))
    counts = {
        r.claim_status: r["count"]
        for r in gold["fct_claims"].groupBy("claim_status").count().collect()
    }
    assert counts["VOIDED"] == cms["cancellations"] + hum["voids"]
    assert counts["ADJUSTED"] == cms["adjustments"] + hum["replacements"]
    assert counts["DENIED"] == hum["denied"]
    assert sum(counts.values()) == gold["fct_claims"].count()


def test_dim_member_matches_the_crossover_answer_key(gold, generated):
    """The payer switchers are exactly the multi-payer members, each with one more coverage
    span than switches, currently covered by the payer of their last switch."""
    switches: dict[str, list[dict]] = defaultdict(list)
    for row in sorted(read_key(generated, "cms_ffs", "crossover_members.csv"),
                      key=lambda r: r["effective_date"]):  # fmt: skip
        switches[row["BENE_MBI_ID"]].append(row)
    expected = {
        mbi: (len(rows) + 1, "humana_ma" if rows[-1]["direction"] == "FFS_TO_MA" else "cms_ffs")
        for mbi, rows in switches.items()
    }
    multi = gold["dim_member"].where("is_multi_payer").collect()
    assert {r.member_key: (r.coverage_span_count, r.current_payer_id) for r in multi} == expected
    assert len(expected) == 17
    members = gold["dim_member"]
    assert members.count() == members.select("member_key").distinct().count()


# --- Member months, every month, against an independent count -------------------------------


@pytest.mark.parametrize("payer_id", list(PAYERS.values()))
def test_member_months_match_the_delivered_coverage_every_month(gold, config, generated, payer_id):
    expected = expected_member_months(config, generated, payer_id)
    rows = (
        gold["fct_member_months"]
        .where(F.col("payer_id") == payer_id)
        .groupBy(F.date_format("month", "yyyy-MM").alias("month"))
        .count()
        .collect()
    )
    assert {r.month: r["count"] for r in rows} == expected
    assert len(expected) == 36 and min(expected.values()) > 0


# --- Gold-wide rules --------------------------------------------------------------------------


def test_marts_reconcile(gold):
    assert gold["check_marts_reconcile"].collect() == []


def test_no_names_addresses_or_birth_dates_in_gold(gold):
    for name in MARTS:
        assert not PROTECTED & set(gold[name].columns), name


# --- The job's answer-key check (claims_lakehouse.checks.answer_keys) ------------------------


@pytest.fixture(scope="module")
def keys(config, generated, tmp_path_factory):
    """The generator's answer keys plus member_months.csv, as uploaded to the volume."""
    keys = tmp_path_factory.mktemp("answer_keys")
    shutil.copytree(generated / "key", keys, dirs_exist_ok=True)
    write_member_month_keys(config, generated, keys)
    return keys


def test_the_job_check_passes_on_a_correct_load(gold, keys):
    assert missing_months(gold, keys) == {}
    assert check_gold(gold, keys) == []


def test_the_job_check_waits_until_every_month_is_delivered(gold, keys):
    """Two months short for one payer: the check reports them missing (and compares nothing)."""
    short = gold["dim_payer"].withColumn(
        "last_delivery_month",
        F.when(F.col("payer_id") == "humana_ma", F.lit("2025-10-01").cast("date")).otherwise(
            F.col("last_delivery_month")
        ),
    )
    assert missing_months({**gold, "dim_payer": short}, keys) == {"humana_ma": 2}


def _broken(gold, name, df):
    return {**gold, name: df}


BREAKS = {
    "net_paid_by_service_month": lambda g: _broken(
        g, "fct_claims",
        g["fct_claims"].withColumn("net_paid_amount", F.col("net_paid_amount") + F.lit(1))),
    "member_months": lambda g: _broken(
        g, "fct_member_months",
        g["fct_member_months"].where(F.date_format("month", "yyyy-MM") != "2024-06")),
    "claim_statuses": lambda g: _broken(
        g, "fct_claims", g["fct_claims"].where(F.col("claim_status") != "DENIED")),
    "payer_switchers": lambda g: _broken(
        g, "dim_member", g["dim_member"].withColumn("current_payer_id", F.lit("cms_ffs"))),
    "pmpm_net_paid_total": lambda g: _broken(
        g, "agg_payer_monthly", g["agg_payer_monthly"].limit(1)),
}  # fmt: skip


@pytest.mark.parametrize("check", list(BREAKS))
def test_the_job_check_reports_broken_gold(gold, keys, check):
    problems = check_gold(BREAKS[check](gold), keys)
    assert check in {p.check for p in problems}
    for problem in problems:  # counts only, never data values
        assert not re.search(r"\b[0-9][A-Z0-9]{10}\b|\d+\.\d{2}", str(problem)), problem


# --- saved queries (queries/) ------------------------------------------------------------

QUERIES = Path(__file__).resolve().parents[1] / "queries"


def run_saved_query(spark, gold, name):
    """A saved query against the local gold: the catalog and schema become temp views."""
    for mart, df in gold.items():
        df.createOrReplaceTempView(mart)
    sql = (QUERIES / name).read_text(encoding="utf-8")
    return spark.sql(sql.replace("claims_lakehouse_dev.gold.", "")).collect()


@pytest.mark.parametrize("name", sorted(p.name for p in QUERIES.glob("*.sql")))
def test_every_saved_query_runs_on_gold(spark, gold, name):
    assert run_saved_query(spark, gold, name)


def test_the_saved_queries_agree_with_the_answer_keys(spark, gold):
    assert len(run_saved_query(spark, gold, "payer_switchers.sql")) == SWITCHERS
    by_month = run_saved_query(spark, gold, "member_months_by_payer.sql")
    pmpm = run_saved_query(spark, gold, "pmpm_by_payer_month.sql")
    assert {(r.payer_id, r.month, r.member_months) for r in by_month} == {
        (r.payer_id, r.month, r.member_months) for r in pmpm
    }
