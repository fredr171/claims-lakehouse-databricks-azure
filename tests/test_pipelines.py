"""The silver pipeline file builds what the local build builds, and gates what it publishes.

src/pipelines/silver.py runs on Databricks; tests/pipeline_runner.py runs it here with a
stand-in for `pyspark.pipelines` and a model of AUTO CDC. Published tables must equal
the local build of the same bronze, which the answer-key tests already check.
"""

from __future__ import annotations

import pytest
from harness import LocalArchive, edited_copy, generated_file, months
from lakehouse import snapshot
from pipeline_runner import load_pipeline, run_pipeline, view_name
from pyspark.sql import functions as F

from claims_lakehouse.bronze.lineage import bronze_table_name
from claims_lakehouse.gold.build import build_conformed
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.humana import tables as humana_tables
from claims_lakehouse.silver.member_history import SEQUENCE_COLUMNS
from claims_lakehouse.silver.quality import Action

CATALOG = "cat"
TABLES = {"cclf": cclf_tables, "humana": humana_tables}
PAYERS = {"cclf": "cms_ffs", "humana": "humana_ma"}
LOADED_MONTHS = months("2023-01", "2023-04")


@pytest.fixture(scope="module")
def archive(config, generated, tmp_path_factory):
    """Four months, then a corrected re-send of each payer's 2023-02 member file with one
    member left out, so bronze holds a superseded delivery that silver must ignore."""
    root = tmp_path_factory.mktemp("pipelines")
    archive = LocalArchive(root, config)
    for month in LOADED_MONTHS:
        archive.deliver_month(generated, month)
    for feed, module in TABLES.items():
        file_type = module.MEMBER_HISTORY.file_type
        original = generated_file(config, generated, file_type, "2023-02")
        resend = edited_copy(original, root / "resend" / feed, lambda rows: rows[1:])
        assert archive.deliver_file(resend, archive.landing_path(feed, resend.name))
    return archive


@pytest.fixture(scope="module", params=["cclf", "humana"])
def payer(request, spark, config, archive):
    """(feed, the pipeline's definitions, what it built, the local build)."""
    feed = request.param
    module = TABLES[feed]
    bronze = {}
    for file_type in module.FILE_TYPES:
        bronze[file_type] = archive.bronze(spark, file_type).localCheckpoint(eager=True)
        name = f"{CATALOG}.bronze.{bronze_table_name(config.file_types[file_type])}"
        bronze[file_type].createOrReplaceTempView(view_name(name))

    local = module.build(bronze)
    local |= {
        f"conformed_{kind}": df
        for kind, df in build_conformed(spark, feed, PAYERS[feed], local).items()
    }
    defs = load_pipeline(
        spark,
        "silver.py",
        {"claims_lakehouse.feed": feed, "claims_lakehouse.catalog": CATALOG},
    )
    built = run_pipeline(spark, defs)
    yield feed, defs, built, local
    for name in [*built, *(d.name for d in defs.datasets)]:
        spark.catalog.dropTempView(name)


def published(feed):
    module = TABLES[feed]
    names = [t.name for t in module.TABLES if t.published]
    return [*names, "conformed_coverage", "conformed_claims", "conformed_deliveries"]


def test_every_listed_table_is_defined_with_its_expectations(payer):
    feed, defs, _, _ = payer
    datasets = {d.name: d for d in defs.datasets}
    module = TABLES[feed]
    for table in module.TABLES:
        if table.name == module.MEMBER_HISTORY.table:
            continue  # defined from AUTO CDC, checked below
        expected = {
            action.value: {e.name: e.constraint for e in table.expectations if e.action is action}
            for action in Action
        }
        expected = {action: rules for action, rules in expected.items() if rules}
        assert datasets[table.name].expectations == expected, table.name


def test_member_history_is_auto_cdc_from_current_deliveries(payer):
    feed, defs, _, _ = payer
    history = TABLES[feed].MEMBER_HISTORY
    [flow] = defs.flows
    assert flow["keys"] == [history.key]
    assert flow["stored_as_scd_type"] == 2
    assert flow["track_history_column_list"] == list(history.tracked)
    assert flow["target"] in defs.streaming_tables
    assert str(flow["sequence_by"]) == str(F.struct(*SEQUENCE_COLUMNS))
    assert flow["except_column_list"] == ["_archived_at"]


def test_published_tables_equal_the_local_build(payer):
    feed, _, built, local = payer
    names = published(feed)
    assert snapshot({n: built[n] for n in names}) == snapshot({n: local[n] for n in names})


def test_everything_listed_is_built(payer):
    feed, defs, built, _ = payer
    assert {d.name for d in defs.datasets} <= set(built)


def test_without_the_gate_nothing_is_published(spark, payer):
    """Every published table depends on _gate: withhold it and none can be built, while
    the tables that feed the gate still are."""
    feed, defs, built, _ = payer
    for name in built:
        spark.catalog.dropTempView(name)
    ungated = run_pipeline(spark, defs, skip=frozenset({"_gate"}))
    assert not set(published(feed)) & set(ungated)
    gate_inputs = {t.name for t in TABLES[feed].TABLES if t.gates}
    assert gate_inputs <= set(ungated)
    for name, df in built.items():  # restore for the other tests
        df.createOrReplaceTempView(name)
