"""Silver pipeline for one payer: the payer's TABLES, its gate, conformed views and AUTO CDC.

Both payer pipelines run this file; the configuration picks the feed. Every table comes
from the payer's TABLES list (claims_lakehouse.silver.<feed>.tables), the same list the
local tests build, with its expectations attached:
    WARN -> expect_all, DROP -> expect_all_or_drop, FAIL -> expect_all_or_fail

Gating: _gate depends on every table carrying a FAIL expectation, and every published
table (member history, claim families, the conformed views) joins _gate. A failed FAIL
expectation therefore stops the update before anything the payer publishes is refreshed,
and gold keeps reading the payer's last good tables.

Member history is AUTO CDC (SCD type 2), fed from current deliveries only, sequenced by
delivery month then archive time. A corrected re-send cannot be retracted incrementally,
so the job runs a full refresh of this pipeline when a re-send supersedes a member file
(jobs/prepare_payer.py, using <history>_superseded below).

Column masks: a table that holds members' names, addresses or birth dates declares its full
schema with a MASK on each (claims_lakehouse.silver.masks). The schemas come from the same
build functions run on empty bronze, so they cannot drift from what the tables produce.

Pipeline configuration:
    claims_lakehouse.feed      cclf | ma_payer
    claims_lakehouse.catalog   the Unity Catalog catalog (bronze is <catalog>.bronze)
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from claims_lakehouse.bronze.lineage import bronze_table_name
from claims_lakehouse.config import load_config
from claims_lakehouse.gold.contract import KINDS
from claims_lakehouse.gold.marts import read_sql, render
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.deliveries import FILE_KEY
from claims_lakehouse.silver.ma_payer import tables as ma_payer_tables
from claims_lakehouse.silver.masks import empty_bronze, masked_schema, table_schemas
from claims_lakehouse.silver.member_history import (
    SEQUENCE_COLUMNS,
    history_source,
    versions_from_history,
)
from claims_lakehouse.silver.quality import Action
from claims_lakehouse.silver.spec import SilverTable, bronze_file_type

FEED = spark.conf.get("claims_lakehouse.feed")  # noqa: F821 (pipeline global)
CATALOG = spark.conf.get("claims_lakehouse.catalog")  # noqa: F821
CONFIG = load_config()
PAYER = next(p for p in CONFIG.payers if p.feed == FEED)
TABLES_MODULE = {"cclf": cclf_tables, "ma_payer": ma_payer_tables}[FEED]
HISTORY = TABLES_MODULE.MEMBER_HISTORY

GATE = "_gate"
GATE_KEY = "__gate_key"
HISTORY_TABLE = f"{HISTORY.table}_history"  # the AUTO CDC target
HISTORY_SOURCE = f"{HISTORY.table}_source"
SUPERSEDED = f"{HISTORY.table}_superseded"
EXCEPT_COLUMNS = ["_archived_at"]  # sequencing only; not stored in the history table
stage_member_file = next(t for t in TABLES_MODULE.TABLES if t.name == HISTORY.source).build

EMPTY_BRONZE = {
    ft: empty_bronze(spark, CONFIG.file_types[ft])  # noqa: F821
    for ft in TABLES_MODULE.FILE_TYPES
}
SCHEMAS = table_schemas(TABLES_MODULE.TABLES, EMPTY_BRONZE)


def schema(table_schema: StructType) -> str | None:
    return masked_schema(table_schema, TABLES_MODULE.PROTECTED_COLUMNS, CATALOG)


def history_table_schema() -> StructType:
    """AUTO CDC's SCD2 target: the source's columns (except EXCEPT_COLUMNS), then
    __START_AT and __END_AT, structs of the sequence columns."""
    source = history_source(
        stage_member_file(EMPTY_BRONZE[HISTORY.file_type]), HISTORY.key, HISTORY.tracked
    ).schema
    sequence = StructType([source[c] for c in SEQUENCE_COLUMNS])
    stored = StructType([f for f in source.fields if f.name not in EXCEPT_COLUMNS])
    return stored.add("__START_AT", sequence).add("__END_AT", sequence)


def bronze_table(file_type: str) -> str:
    return f"{CATALOG}.bronze.{bronze_table_name(CONFIG.file_types[file_type])}"


def read(name: str) -> DataFrame:
    """A silver table of this pipeline, or bronze(...) for a bronze table."""
    file_type = bronze_file_type(name)
    return spark.read.table(bronze_table(file_type) if file_type else name)  # noqa: F821


def gated(df: DataFrame) -> DataFrame:
    """df, refreshed only after every gate table passed. An equi-join on a constant key,
    so it also works on a streaming DataFrame (a stream-static inner join)."""
    gate = read(GATE).select(F.lit(1).alias(GATE_KEY))
    return df.withColumn(GATE_KEY, F.lit(1)).join(gate, GATE_KEY).drop(GATE_KEY)


def with_expectations(table: SilverTable, function):
    """Attach the table's expectations, grouped by action."""
    by_action = {
        Action.WARN: dp.expect_all,
        Action.DROP: dp.expect_all_or_drop,
        Action.FAIL: dp.expect_all_or_fail,
    }
    for action, decorator in by_action.items():
        constraints = {e.name: e.constraint for e in table.expectations if e.action is action}
        if constraints:
            function = decorator(constraints)(function)
    return function


def define_table(table: SilverTable) -> None:
    def build() -> DataFrame:
        df = table.build(*[read(name) for name in table.inputs])
        return gated(df) if table.published else df

    dp.materialized_view(
        name=table.name, comment=f"{PAYER.payer_id} silver", schema=schema(SCHEMAS[table.name])
    )(with_expectations(table, build))


for silver_table in TABLES_MODULE.TABLES:
    if silver_table.name != HISTORY.table:  # AUTO CDC builds it (below)
        define_table(silver_table)


@dp.materialized_view(name=GATE, comment="One row; refreshes only when every gate table passed")
def gate() -> DataFrame:
    counts = [read(t.name).groupBy().count() for t in TABLES_MODULE.TABLES if t.gates]
    union = counts[0]
    for count in counts[1:]:
        union = union.unionByName(count)
    return union.agg(F.count(F.lit(1)).alias("gate_tables"))


# --- Member history: AUTO CDC from current deliveries --------------------------------------


@dp.temporary_view(name=HISTORY_SOURCE)
def history_rows() -> DataFrame:
    """New bronze rows of the member file, from current deliveries only, staged."""
    deliveries = read(f"{HISTORY.file_type}_deliveries")
    current = deliveries.where("is_current").select(*FILE_KEY, "_checksum")
    rows = spark.readStream.table(bronze_table(HISTORY.file_type)).join(  # noqa: F821
        current, on=[*FILE_KEY, "_checksum"], how="left_semi"
    )
    return gated(history_source(stage_member_file(rows), HISTORY.key, HISTORY.tracked))


dp.create_streaming_table(
    name=HISTORY_TABLE,
    comment=f"{PAYER.payer_id} member history (SCD2)",
    schema=schema(history_table_schema()),
)
dp.create_auto_cdc_flow(
    target=HISTORY_TABLE,
    source=HISTORY_SOURCE,
    keys=[HISTORY.key],
    sequence_by=F.struct(*SEQUENCE_COLUMNS),
    except_column_list=EXCEPT_COLUMNS,
    stored_as_scd_type=2,
    track_history_column_list=list(HISTORY.tracked),
)


@dp.materialized_view(
    name=HISTORY.table,
    comment=f"{PAYER.payer_id} member versions",
    schema=schema(SCHEMAS[HISTORY.table]),
)
def member_versions() -> DataFrame:
    history = read(HISTORY_TABLE)
    return gated(versions_from_history(history, HISTORY.key, HISTORY.tracked, HISTORY.coverage_end))


@dp.materialized_view(name=SUPERSEDED, comment="Superseded member-file deliveries history saw")
def superseded() -> DataFrame:
    """How many member-file deliveries were superseded when history was last built. The
    job compares it with bronze to decide on a full refresh."""
    deliveries = read(f"{HISTORY.file_type}_deliveries")
    count = deliveries.agg(F.sum((~F.col("is_current")).cast("long")).alias("superseded"))
    history_rows_ = read(HISTORY_TABLE).agg(F.count(F.lit(1)).alias("history_rows"))
    return gated(count.crossJoin(history_rows_))


# --- Conformed views: what gold reads -------------------------------------------------------


def define_conformed(kind: str) -> None:
    sql = render(read_sql(f"claims_lakehouse.silver.{FEED}", f"conformed_{kind}"),
                 {"payer_id": PAYER.payer_id})  # fmt: skip

    @dp.materialized_view(name=f"conformed_{kind}", comment=f"{PAYER.payer_id} conformed {kind}")
    def conformed() -> DataFrame:
        return gated(spark.sql(sql))  # noqa: F821


for conformed_kind in KINDS:
    define_conformed(conformed_kind)
