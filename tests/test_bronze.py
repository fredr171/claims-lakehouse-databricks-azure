"""Bronze: lineage from archive paths, and the harness loading files the way Auto Loader will."""

import csv
from datetime import timedelta

import pytest
from harness import ARCHIVE_URL, CLOCK_START, DeliveryRejected, LocalArchive, months
from pyspark.sql import functions as F
from validation_fixtures import FILE_NAME, FIXTURE_DIR, HEADER, ROWS

from claims_lakehouse.bronze.lineage import LINEAGE_COLUMNS, bronze_table_name, with_lineage
from claims_lakehouse.silver.deliveries import current_rows, delivery_index

LANDING = f"cms/cclf/claims_diagnosis/{FILE_NAME}"
SHA = "ab" * 32


def fixture(case: str):
    return FIXTURE_DIR / case / FILE_NAME


# --- Lineage from the archive path -------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "month", "checksum"),
    [
        (
            f"abfss://archive@acct.dfs.core.windows.net/cms/cclf/claims_header/"
            f"delivery_month=2023-01/checksum={SHA}/CCLF1_202301.csv",
            "2023-01",
            SHA,
        ),
        (
            f"file:///C:/tmp/archive/ma_payer/ma/member_eligibility/"
            f"delivery_month=2025-12/checksum={SHA}/MAP_ELIG_202512.csv",
            "2025-12",
            SHA,
        ),
        # Not the archive layout: bronze's expectations reject the NULLs.
        (
            "abfss://landing@acct.dfs.core.windows.net/cms/cclf/claims_header/CCLF1_202301.csv",
            None,
            None,
        ),
        (f"/x/delivery_month=2023-13/checksum={SHA}/CCLF1_202301.csv", None, None),
        (f"/x/delivery_month=2023-01/checksum={SHA.upper()}/CCLF1_202301.csv", None, None),
        (f"/x/delivery_month=2023-01/checksum={SHA}/extra/CCLF1_202301.csv", None, None),
    ],
)
def test_lineage_from_archive_path(spark, config, path, month, checksum):
    ft = config.file_types["claims_header"]
    df = spark.range(1).select(
        *[F.lit("x").alias(c) for c in ft.expected_columns], F.lit(path).alias("path")
    )
    row = with_lineage(df, ft, F.col("path"), F.lit(CLOCK_START)).first()
    assert (row._delivery_month, row._checksum) == (month, checksum)
    assert (row._feed, row._file_type, row._source_file) == ("cclf", "claims_header", path)
    assert row._ingested_at is not None


def test_bronze_table_name(config):
    assert (
        bronze_table_name(config.file_types["medical_claim_lines"])
        == "ma_payer_medical_claim_lines"
    )


# --- Loading archived files --------------------------------------------------------------


def test_bronze_keeps_delivered_strings_and_adds_lineage(spark, config, tmp_path):
    archive = LocalArchive(tmp_path, config)
    result = archive.deliver_file(fixture("valid"), LANDING)
    bronze = archive.bronze(spark, "claims_diagnosis")

    assert bronze.columns == [*HEADER.split(","), *LINEAGE_COLUMNS]
    assert all(dtype == "string" for _, dtype in bronze.dtypes[: len(HEADER.split(","))])
    rows = bronze.orderBy("CLM_VAL_SQNC_NUM", "CUR_CLM_UNIQ_ID").collect()
    assert len(rows) == result.row_count == 3
    first = rows[0]
    assert first.CLM_DGNS_CD == "E785"
    assert first.CLM_POA_IND is None  # Spark CSV reads an empty field as NULL
    assert first._delivery_month == "2023-01"
    assert first._checksum == result.checksum
    assert first._source_file == ARCHIVE_URL + result.archive_path
    # Compare as epoch seconds: PySpark returns timestamps as naive local times.
    archived_epoch = bronze.select(F.col("_archived_at").cast("long")).first()[0]
    assert archived_epoch == (CLOCK_START + timedelta(seconds=1)).timestamp()


def test_quoting_whitespace_and_leading_zeros_survive(spark, config, tmp_path):
    columns = HEADER.split(",")
    values = dict(zip(columns, ROWS[0].split(","), strict=True))
    values |= {
        "PRVDR_OSCAR_NUM": "007",  # leading zeros
        "CLM_DGNS_CD": '"E7""85"',  # RFC 4180 doubled quote
        "BENE_HIC_NUM": "91\\5549764A",  # a backslash is not an escape
        "CLM_POA_IND": '"  Y "',  # whitespace inside quotes
    }
    path = tmp_path / FILE_NAME
    path.write_text(f"{HEADER}\n{','.join(values.values())}\n", encoding="utf-8")

    archive = LocalArchive(tmp_path, config)
    archive.deliver_file(path, LANDING)
    row = archive.bronze(spark, "claims_diagnosis").first()
    assert row.PRVDR_OSCAR_NUM == "007"
    assert row.CLM_DGNS_CD == 'E7"85'
    assert row.BENE_HIC_NUM == "91\\5549764A"
    assert row.CLM_POA_IND == "  Y "


def test_crlf_file_loads_the_same_values(spark, config, tmp_path):
    def values(case):
        archive = LocalArchive(tmp_path / case, config)
        archive.deliver_file(fixture(case), LANDING)
        bronze = archive.bronze(spark, "claims_diagnosis")
        return sorted(tuple(r) for r in bronze.select(*HEADER.split(",")).collect())

    assert values("valid_crlf") == values("valid")


def test_rejected_file_never_reaches_the_archive(config, tmp_path):
    archive = LocalArchive(tmp_path, config)
    with pytest.raises(DeliveryRejected, match="no_bom"):
        archive.deliver_file(fixture("bom_utf8"), LANDING)
    assert archive.files == []


def test_identical_redelivery_is_skipped(spark, config, tmp_path):
    archive = LocalArchive(tmp_path, config)
    assert archive.deliver_file(fixture("valid"), LANDING) is not None
    assert archive.deliver_file(fixture("valid"), LANDING) is None
    assert archive.bronze(spark, "claims_diagnosis").count() == 3


def test_corrected_resend_is_appended_and_becomes_current(spark, config, tmp_path):
    archive = LocalArchive(tmp_path, config)
    original = archive.deliver_file(fixture("valid"), LANDING)
    resend = archive.deliver_file(fixture("valid_quoted"), LANDING)  # same name, new bytes
    bronze = archive.bronze(spark, "claims_diagnosis").cache()

    index = {r._checksum: r for r in delivery_index(bronze).collect()}
    # Append-only: both deliveries stay in bronze.
    assert (index[original.checksum].row_count, index[resend.checksum].row_count) == (3, 3)
    assert index[original.checksum].delivery_version == 1
    assert not index[original.checksum].is_current
    assert index[resend.checksum].delivery_version == 2
    assert index[resend.checksum].is_current
    current = current_rows(bronze)
    assert current.columns == bronze.columns
    current_checksums = [r._checksum for r in current.collect()]
    assert current_checksums == [resend.checksum] * 3
    bronze.unpersist()


# --- The full generated dataset ----------------------------------------------------------


@pytest.mark.slow
def test_every_generated_file_is_accepted(config, full_load):
    _, results = full_load
    assert len(results) == 36 * len(config.file_types)
    assert all(r is not None and r.accepted for r in results)


@pytest.mark.slow
@pytest.mark.parametrize(
    "file_type",
    [
        "beneficiary_demographics",
        "claims_header",
        "claims_diagnosis",
        "part_b_physician_lines",
        "member_eligibility",
        "medical_claim_lines",
    ],
)
def test_full_load_bronze_matches_the_delivered_files(spark, config, full_load, file_type):
    """Spark (with bronze's options) reads exactly what validation's csv module read."""
    archive, results = full_load
    bronze = archive.bronze(spark, file_type).cache()
    ft = config.file_types[file_type]

    expected = []
    for archived in (f for f in archive.files if f.file_type == file_type):
        with archived.local_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))[1:]
        expected.extend(tuple(v if v != "" else None for v in row) for row in rows)
    actual = [tuple(r) for r in bronze.select(*ft.expected_columns).collect()]
    assert sorted(actual, key=repr) == sorted(expected, key=repr)
    assert len(actual) == sum(r.row_count for r in results if r.file_type == ft)

    index = delivery_index(bronze).collect()
    assert len(index) == 36
    assert all(r.is_current and r.delivery_version == 1 for r in index)
    assert sorted(r._delivery_month for r in index) == months()
    bronze.unpersist()
