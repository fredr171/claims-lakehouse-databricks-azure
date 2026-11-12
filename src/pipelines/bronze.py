"""Bronze pipeline: Auto Loader from the archive, one streaming table per file type.

The archive holds only validated files (the validation Function wrote them), at
checksum-keyed paths, so Auto Loader sees each delivery, including a corrected re-send,
as a new file. Every column is read as a string; lineage comes from _metadata. The CSV
options, schema and lineage expression are the ones the local tests use
(claims_lakehouse.bronze.lineage).

The member files' names, addresses and birth dates are declared with Unity Catalog column
masks (claims_lakehouse.silver.masks), so the table's schema is given in full for them.

Pipeline configuration:
    claims_lakehouse.archive_url   abfss://archive@<account>.dfs.core.windows.net/
    claims_lakehouse.catalog       the Unity Catalog catalog (holds the mask functions)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from claims_lakehouse.bronze.lineage import (
    CSV_OPTIONS,
    bronze_schema,
    bronze_table_name,
    with_lineage,
)
from claims_lakehouse.config import FileType, load_config
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.humana import tables as humana_tables
from claims_lakehouse.silver.masks import empty_bronze, masked_schema

ARCHIVE_URL = spark.conf.get("claims_lakehouse.archive_url")  # noqa: F821 (pipeline global)
CATALOG = spark.conf.get("claims_lakehouse.catalog")  # noqa: F821
CONFIG = load_config()
PROTECTED = {
    module.MEMBER_HISTORY.file_type: module.PROTECTED_COLUMNS
    for module in (cclf_tables, humana_tables)
}

# A path outside the archive layout has no month or checksum: never load it.
LINEAGE_EXPECTATIONS = {
    "delivery_month_from_path": "_delivery_month IS NOT NULL",
    "checksum_from_path": "_checksum IS NOT NULL",
}


def define_bronze_table(file_type: FileType) -> None:
    source = f"{ARCHIVE_URL}{file_type.feed.prefix}{file_type.file_type}/"
    protected = PROTECTED.get(file_type.file_type, frozenset())
    schema = masked_schema(empty_bronze(spark, file_type).schema, protected, CATALOG)  # noqa: F821

    @dp.table(
        name=bronze_table_name(file_type),
        comment=f"Every validated delivery of {file_type.file_type} ({file_type.feed.name}), "
        "as delivered text, append-only",
        schema=schema,
    )
    @dp.expect_all_or_fail(LINEAGE_EXPECTATIONS)
    def bronze():
        rows = (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .options(**CSV_OPTIONS)
            .schema(bronze_schema(file_type))
            .load(source)
        )
        return with_lineage(
            rows,
            file_type,
            F.col("_metadata.file_path"),
            F.col("_metadata.file_modification_time"),
        )


for ft in CONFIG.file_types.values():
    define_bronze_table(ft)
