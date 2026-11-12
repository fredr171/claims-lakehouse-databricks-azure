"""Unity Catalog column masks on members' names, addresses and birth dates.

The member files carry synthetic names, addresses, ZIP codes and birth dates. Every table
that holds one of those columns, under its delivered name (bronze, *_current) or its staged
name (stg_*, member history), declares it with a mask. A reader outside the PII readers
group sees '[masked]' for text and NULL for a date. The identity that runs the pipelines
and the job is in the group, so silver and gold are computed from real values. Gold never
selects these columns (only birth year reaches it, through the conformed views).

Lakeflow takes a mask only as part of a table's full schema (a DDL string), so the schema
is derived from the same build code the table runs (see table_schemas), never written by
hand.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StringType, StructType

from claims_lakehouse.bronze.lineage import bronze_schema, with_lineage
from claims_lakehouse.config import FileType
from claims_lakehouse.silver.spec import SilverTable, build_tables

# Staged names of the protected columns; their delivered names come from each payer's
# staging list (staged name, delivered name, type).
PROTECTED = (
    "first_name",
    "middle_name",
    "middle_initial",
    "last_name",
    "address_line_1",
    "city",
    "zip5_code",
    "zip9_code",
    "birth_date",
)

MASKED_TEXT = "[masked]"
MASK_FUNCTIONS = {"string": "mask_pii_string", "date": "mask_pii_date"}
MASK_SCHEMA = "ops"


def protected_names(staging: Iterable[tuple[str, str, object]]) -> frozenset[str]:
    """Staged and delivered names of the protected columns in a payer's member file."""
    return frozenset(
        name
        for staged, delivered, _ in staging
        if staged in PROTECTED
        for name in (staged, delivered)
    )


def mask_function(catalog: str, data_type: DataType) -> str:
    key = data_type.simpleString()
    if key not in MASK_FUNCTIONS:
        raise ValueError(f"no mask function for a {key} column")
    return f"{catalog}.{MASK_SCHEMA}.{MASK_FUNCTIONS[key]}"


def mask_function_statements(catalog: str, readers_group: str) -> list[str]:
    """CREATE OR REPLACE the mask functions (run before any pipeline that uses them)."""
    allowed = f"is_account_group_member('{readers_group}') OR is_member('{readers_group}')"
    masked = {"string": f"'{MASKED_TEXT}'", "date": "CAST(NULL AS DATE)"}
    return [
        f"CREATE OR REPLACE FUNCTION {catalog}.{MASK_SCHEMA}.{name}(value {sql_type.upper()})\n"
        f"RETURNS {sql_type.upper()}\n"
        f"COMMENT 'Column mask: the value for {readers_group}, otherwise masked'\n"
        f"RETURN CASE WHEN {allowed} THEN value ELSE {masked[sql_type]} END"
        for sql_type, name in MASK_FUNCTIONS.items()
    ]


def masked_schema(schema: StructType, protected: frozenset[str], catalog: str) -> str | None:
    """The table's schema as DDL with a MASK on each protected column, or None when the
    table holds none (it then needs no declared schema)."""
    if not any(f.name in protected for f in schema.fields):
        return None
    columns = []
    for field in schema.fields:
        ddl = f"`{field.name}` {field.dataType.simpleString()}"
        if field.name in protected:
            ddl += f" MASK {mask_function(catalog, field.dataType)}"
        columns.append(ddl)
    return ", ".join(columns)


# --- Schemas without data: the build code run on empty inputs -----------------------------


def empty_bronze(spark: SparkSession, file_type: FileType) -> DataFrame:
    """A zero-row DataFrame with exactly the bronze table's columns and types."""
    rows = spark.createDataFrame(
        [], bronze_schema(file_type).add("__file", StringType()).add("__time", "timestamp")
    )
    return with_lineage(rows, file_type, F.col("__file"), F.col("__time"))


def table_schemas(
    tables: Sequence[SilverTable], empty_bronze_tables: Mapping[str, DataFrame]
) -> dict[str, StructType]:
    """Table name -> schema, from the tables' own build functions over empty bronze."""
    return {name: df.schema for name, df in build_tables(tables, empty_bronze_tables).items()}
