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
from claims_lakehouse.jobs.supersession import needs_full_refresh, superseded_deliveries
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.humana import tables as humana_tables
from claims_lakehouse.silver.masks import PROTECTED, mask_function_statements, masked_schema
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


def test_the_published_supersession_count_is_what_the_job_compares(spark, config, payer):
    """The pipeline's <history>_superseded and the job's count from bronze must agree, or
    the job would full-refresh on every run (or never)."""
    feed, _, built, _ = payer
    history = TABLES[feed].MEMBER_HISTORY
    name = f"{CATALOG}.bronze.{bronze_table_name(config.file_types[history.file_type])}"
    in_bronze = superseded_deliveries(spark.read.table(view_name(name)))
    published = built[f"{history.table}_superseded"].first()["superseded"]
    assert in_bronze == published == 1  # the fixture's re-send
    assert not needs_full_refresh(in_bronze, published)


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


# --- Column masks ---------------------------------------------------------------------------

EXPECTED_MASKED = {
    "cclf": {"beneficiary_demographics_current", "stg_beneficiary_demographics",
             "beneficiary_versions", "beneficiary_versions_history"},
    "humana": {"member_eligibility_current", "stg_member_eligibility",
               "member_versions", "member_versions_history"},
}  # fmt: skip


def declared_schemas(defs):
    return {d.name: d.schema for d in defs.datasets} | dict(defs.streaming_tables)


def test_exactly_the_tables_holding_member_pii_declare_masks(payer):
    feed, defs, _, _ = payer
    masked = {name for name, ddl in declared_schemas(defs).items() if ddl and "MASK" in ddl}
    assert masked == EXPECTED_MASKED[feed]


def test_each_declared_schema_is_the_built_table_with_masks(payer):
    """The declared schema is derived from empty inputs; it must be exactly the schema of
    the table built from real data, or Databricks would reject the update."""
    feed, defs, built, _ = payer
    protected = TABLES[feed].PROTECTED_COLUMNS
    for name, ddl in declared_schemas(defs).items():
        if ddl is not None:
            assert ddl == masked_schema(built[name].schema, protected, CATALOG), name


def test_every_protected_column_is_masked_with_its_type(payer):
    feed, defs, _, _ = payer
    stg = declared_schemas(defs)[f"stg_{TABLES[feed].MEMBER_HISTORY.file_type}"]
    columns = dict(part.split(" ", 1) for part in stg.split(", "))
    for name in PROTECTED:
        if f"`{name}`" in columns:
            fn = "mask_pii_date" if name == "birth_date" else "mask_pii_string"
            assert columns[f"`{name}`"].endswith(f"MASK {CATALOG}.ops.{fn}"), name
    unmasked = [c for c, d in columns.items() if "MASK" not in d]
    assert "`beneficiary_mbi`" in unmasked  # identifiers are not masked


def test_bronze_masks_the_delivered_member_columns(spark, config, archive):
    defs = load_pipeline(
        spark,
        "bronze.py",
        {"claims_lakehouse.archive_url": "abfss://archive@x/", "claims_lakehouse.catalog": CATALOG},
    )
    schemas = {d.name: d.schema for d in defs.datasets}
    for file_type in config.file_types.values():
        name = bronze_table_name(file_type)
        module = next((m for m in TABLES.values() if m.MEMBER_HISTORY.file_type ==
                       file_type.file_type), None)  # fmt: skip
        if module is None:
            assert schemas[name] is None, name
            continue
        actual = archive.bronze(spark, file_type.file_type).schema
        assert schemas[name] == masked_schema(actual, module.PROTECTED_COLUMNS, CATALOG)
        assert "MASK" in schemas[name]


def test_the_mask_functions_cover_text_and_dates():
    statements = mask_function_statements(CATALOG, "pii_readers")
    assert [s.split("(")[0] for s in statements] == [
        f"CREATE OR REPLACE FUNCTION {CATALOG}.ops.mask_pii_string",
        f"CREATE OR REPLACE FUNCTION {CATALOG}.ops.mask_pii_date",
    ]
    assert all("is_member('pii_readers')" in s for s in statements)


# Written out by hand, independently of masks.PROTECTED: what must never be readable.
MUST_BE_MASKED = {
    "cclf": {"first_name", "middle_name", "last_name", "address_line_1", "city", "zip5_code",
             "zip9_code", "birth_date", "BENE_1ST_NAME", "BENE_MIDL_NAME", "BENE_LAST_NAME",
             "BENE_LINE_1_ADR", "GEO_ZIP_PLC_NAME", "GEO_ZIP5_CD", "BENE_ZIP_CD", "BENE_DOB"},
    "humana": {"first_name", "middle_initial", "last_name", "address_line_1", "city",
               "zip5_code", "birth_date", "FIRST_NAME", "MIDDLE_INITIAL", "LAST_NAME",
               "ADDRESS_LINE_1", "CITY", "ZIP", "DOB"},
}  # fmt: skip


def test_the_masked_columns_are_exactly_the_members_pii(payer):
    feed, defs, _, _ = payer
    masked = set()
    for ddl in declared_schemas(defs).values():
        for part in (ddl or "").split(", "):
            if " MASK " in part:
                masked.add(part.split("`")[1])
    # staged names (staging, history) and delivered names (*_current); nothing else
    assert masked == MUST_BE_MASKED[feed]
