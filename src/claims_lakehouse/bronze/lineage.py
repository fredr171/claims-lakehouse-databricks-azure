"""Bronze: how archived CSVs are read, and the lineage columns added to every row.

One bronze table per file type, append-only, every delivered column as a string (codes and
IDs keep their leading zeros). Auto Loader (phase 3) and the local test harness both use
CSV_OPTIONS, bronze_schema and with_lineage from here, so they cannot drift apart.

Spark CSV reads an empty field as NULL, quoted or not; no reader option keeps it as ''.
Everything else (whitespace, leading zeros, quoted text) stays exactly as delivered.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from claims_lakehouse.config import FileType

# Must agree with validation (claims_lakehouse.validation), which already rejected anything
# these options could read differently from Python's csv module: line breaks inside
# fields (multiLine off), a BOM, ragged rows. escape='"' gives RFC 4180 doubled quotes;
# Spark's default escape is a backslash. FAILFAST: a row that still fails to parse stops
# the load rather than becoming NULLs.
CSV_OPTIONS: dict[str, str] = {
    "header": "true",
    "multiLine": "false",
    "escape": '"',
    "mode": "FAILFAST",
    "encoding": "UTF-8",
}

# Lineage columns, in output order after the delivered columns.
LINEAGE_COLUMNS = (
    "_feed",
    "_file_type",
    "_delivery_month",  # YYYY-MM, from the archive path (delivery_month=...)
    "_checksum",  # SHA-256 of the delivered file, from the archive path (checksum=...)
    "_source_file",  # full archive path of the file the row came from
    "_archived_at",  # when the file reached the archive: orders deliveries
    "_ingested_at",  # when the row was loaded into bronze
)

# Archive layout (claims_lakehouse.validation.outcome), matched at the end of the path so
# any storage URL prefix (abfss://archive@<account>..., file:/...) works.
ARCHIVE_PATH_PATTERN = r"/delivery_month=(\d{4}-(?:0[1-9]|1[0-2]))/checksum=([0-9a-f]{64})/[^/]+$"


def bronze_table_name(file_type: FileType) -> str:
    """e.g. cclf_claims_header."""
    return f"{file_type.feed.name}_{file_type.file_type}"


def bronze_schema(file_type: FileType) -> StructType:
    """The delivered columns, all strings. Validation has already matched the header."""
    return StructType([StructField(name, StringType()) for name in file_type.expected_columns])


def with_lineage(
    df: DataFrame,
    file_type: FileType,
    source_file: Column,
    archived_at: Column,
    ingested_at: Column | None = None,
) -> DataFrame:
    """Add the lineage columns. Auto Loader passes _metadata.file_path and
    _metadata.file_modification_time; a path that is not in the archive layout gives NULL
    month and checksum, which bronze's expectations reject."""
    delivery_month = F.nullif(F.regexp_extract(source_file, ARCHIVE_PATH_PATTERN, 1), F.lit(""))
    checksum = F.nullif(F.regexp_extract(source_file, ARCHIVE_PATH_PATTERN, 2), F.lit(""))
    return df.select(
        *file_type.expected_columns,
        F.lit(file_type.feed.name).alias("_feed"),
        F.lit(file_type.file_type).alias("_file_type"),
        delivery_month.alias("_delivery_month"),
        checksum.alias("_checksum"),
        source_file.alias("_source_file"),
        archived_at.cast("timestamp").alias("_archived_at"),
        (ingested_at if ingested_at is not None else F.current_timestamp()).alias("_ingested_at"),
    )
