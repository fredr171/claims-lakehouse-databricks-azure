"""A local stand-in for landing -> validation Function -> archive -> Auto Loader -> bronze.

    archive = LocalArchive(tmp_path, config)
    archive.deliver_month(generated, "2023-01")      # validate + archive every file
    archive.deliver_file(path, landing_path)         # one file (e.g. a corrected re-send)
    bronze = archive.bronze(spark, "claims_header")  # the bronze table as Auto Loader builds it

What is real: validation (validate_file), the archive layout and checksum-keyed paths,
duplicate skipping, Spark's CSV parser with bronze's CSV_OPTIONS, the lineage expression,
and the delivery order (_archived_at comes from a clock that ticks once per archived file).

What differs: Auto Loader reads files from storage and takes lineage from _metadata. Here
Python reads the archived files (local Spark on Windows cannot read the filesystem without
Hadoop's native binaries) and splits them into lines; Spark's from_csv then parses each
line with the same CSV parser and options, in the JVM. Validation has rejected line breaks
inside fields, so one line is always one row. tests/test_bronze.py checks every parsed
field of the 36-month dataset against Python's csv module.
"""

from __future__ import annotations

import csv
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from claims_lakehouse.bronze.lineage import CSV_OPTIONS, bronze_schema, with_lineage
from claims_lakehouse.config import LakehouseConfig
from claims_lakehouse.validation.validate import ValidationResult, validate_file

ARCHIVE_URL = "abfss://archive@stlocalharness.dfs.core.windows.net/"
CLOCK_START = datetime(2026, 1, 1, tzinfo=UTC)
FIRST_MONTH, LAST_MONTH = "2023-01", "2025-12"  # what the generators are run for

# bronze's CSV_OPTIONS that apply to parsing one line (header, multiLine and encoding are
# about reading a file, which Python does here).
_LINE_OPTIONS = {k: v for k, v in CSV_OPTIONS.items() if k in ("escape", "mode")}


def months(first: str = FIRST_MONTH, last: str = LAST_MONTH) -> list[str]:
    """['2023-01', '2023-02', ...] inclusive."""
    year, month = map(int, first.split("-"))
    result = []
    while f"{year:04d}-{month:02d}" <= last:
        result.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return result


@dataclass(frozen=True)
class ArchivedFile:
    file_type: str
    archive_path: str  # relative to the archive container
    local_path: Path
    archived_at: datetime


class DeliveryRejected(AssertionError):
    pass


class LocalArchive:
    def __init__(self, root: Path, config: LakehouseConfig) -> None:
        self.root = root / "archive"
        self.config = config
        self.files: list[ArchivedFile] = []
        self._clock = CLOCK_START

    # --- Deliveries ----------------------------------------------------------------------

    def deliver_file(self, local_file: Path, landing_path: str) -> ValidationResult | None:
        """Validate and archive one file, as the Function will.

        Returns None if an identical file (same checksum) is already archived: duplicates
        are skipped. Raises DeliveryRejected if validation rejects it.
        """
        result = validate_file(landing_path, local_file, self.config)
        if result is None:
            raise ValueError(f"{landing_path} is outside every feed")
        if not result.accepted:
            raise DeliveryRejected(f"{landing_path}: failed {result.failed_checks}")
        target = self.root / result.archive_path
        if target.exists():
            return None
        target.parent.mkdir(parents=True)
        shutil.copyfile(local_file, target)
        self._clock += timedelta(seconds=1)
        self.files.append(
            ArchivedFile(result.file_type.file_type, result.archive_path, target, self._clock)
        )
        return result

    def deliver_month(
        self,
        generated: Path,
        month: str,
        feeds: tuple[str, ...] = ("cclf", "humana"),
        replace: dict[str, Path] | None = None,
    ) -> list[ValidationResult]:
        """Deliver every generated file of a month for the given feeds.

        replace: file name -> a local file delivered instead (an edited copy).
        """
        replace = replace or {}
        results = []
        for feed in feeds:
            for local_file in sorted((generated / _GENERATED_DIRS[feed] / month).glob("*.csv")):
                source = replace.get(local_file.name, local_file)
                results.append(self.deliver_file(source, self.landing_path(feed, local_file.name)))
        return results

    def deliver_generated(self, generated: Path, feed: str, file_type: str, month: str):
        """Deliver one generated file."""
        path = generated_file(self.config, generated, file_type, month)
        return self.deliver_file(path, self.landing_path(feed, path.name))

    def landing_path(self, feed_name: str, file_name: str) -> str:
        """Where a file of this name lands: the file type whose name pattern matches."""
        matches = [
            ft for ft in self.config.file_types_for_feed(feed_name) if ft.delivery_month(file_name)
        ]
        if len(matches) != 1:
            raise ValueError(f"{file_name}: expected one file type in {feed_name}, got {matches}")
        return f"{matches[0].feed.prefix}{matches[0].file_type}/{file_name}"

    # --- Bronze --------------------------------------------------------------------------

    def bronze(self, spark: SparkSession, file_type: str) -> DataFrame:
        """Every archived delivery of a file type, as Auto Loader would load it."""
        ft = self.config.file_types[file_type]
        files = [f for f in self.files if f.file_type == file_type]

        lines = []
        for archived in files:
            text = archived.local_path.read_text(encoding="utf-8")
            # Split on LF / CRLF only, as Spark does (str.splitlines also splits on U+2028).
            data_lines = re.split(r"\r?\n", text.removesuffix("\n").removesuffix("\r"))[1:]
            source = ARCHIVE_URL + archived.archive_path
            lines.extend((source, archived.archived_at, line) for line in data_lines)

        raw = spark.createDataFrame(lines, "source_file string, archived_at timestamp, line string")
        parsed = raw.select(
            "source_file",
            "archived_at",
            F.from_csv("line", bronze_schema(ft).simpleString(), _LINE_OPTIONS).alias("row"),
        ).select("source_file", "archived_at", "row.*")
        return with_lineage(parsed, ft, F.col("source_file"), F.col("archived_at"))


_GENERATED_DIRS = {"cclf": "cms", "humana": "humana"}


def generated_file(config: LakehouseConfig, generated: Path, file_type: str, month: str) -> Path:
    """The generated file of a file type for a delivery month."""
    ft = config.file_types[file_type]
    folder = generated / _GENERATED_DIRS[ft.feed.name] / month
    [path] = [p for p in folder.iterdir() if re.match(ft.file_name_regex, p.name)]
    return path


def edited_copy(source: Path, target_dir: Path, edit) -> Path:
    """A copy of a delivered CSV with edit(rows) applied (rows: list of dicts), same name.

    The copy is a different file (new checksum): delivered under the same name it is a
    corrected re-send, or a broken delivery.
    """
    with source.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header, rows = reader.fieldnames, list(reader)
    rows = edit(rows)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / source.name
    with target.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return target
