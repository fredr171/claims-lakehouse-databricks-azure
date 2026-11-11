"""Validate one delivered file: run every check that can give a meaningful answer.

All independent checks run, so one report lists every problem with the file. A check
whose input depends on an earlier failure (the CSV structure of a file that does not
decode, say) is reported as skipped rather than as a second failure with the same cause.

This module never touches storage. The validation Azure Function downloads the blob to a
local file, calls validate_file, and copies the file to the archive or quarantine path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from claims_lakehouse.config import Feed, FileType, LakehouseConfig
from claims_lakehouse.validation import checks
from claims_lakehouse.validation.checks import CheckResult, CheckStatus
from claims_lakehouse.validation.csv_inspect import profile_csv
from claims_lakehouse.validation.outcome import (
    archive_path,
    error_report_path,
    quarantine_path,
    sha256_file,
)

# Every check, in the order it is reported.
CHECK_NAMES = (
    "landing_path",
    "known_file_type",
    "file_name",
    "file_size",
    "no_bom",
    "encoding",
    "csv_parseable",
    "no_embedded_line_breaks",
    "header",
    "field_counts",
    "has_rows",
    "key_columns",
)


@dataclass(frozen=True)
class ValidationResult:
    landing_path: str
    feed: Feed
    file_type: FileType | None  # None if the landing folder is not a known file type
    delivery_month: str | None  # None if the file name does not match
    size_bytes: int
    checksum: str
    row_count: int | None  # None unless the whole file parsed
    checks: tuple[CheckResult, ...]

    @property
    def accepted(self) -> bool:
        return all(c.status is CheckStatus.PASSED for c in self.checks)

    @property
    def failed_checks(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.checks if c.status is CheckStatus.FAILED)

    @property
    def archive_path(self) -> str:
        if not self.accepted:
            raise ValueError("a rejected file has no archive path")
        assert self.file_type is not None and self.delivery_month is not None
        file_name = self.landing_path.rsplit("/", 1)[-1]
        return archive_path(
            self.feed.prefix,
            self.file_type.file_type,
            self.delivery_month,
            self.checksum,
            file_name,
        )

    @property
    def quarantine_path(self) -> str:
        return quarantine_path(self.landing_path, self.checksum)

    @property
    def error_report_path(self) -> str:
        return error_report_path(self.landing_path, self.checksum)

    def report(self) -> dict[str, Any]:
        """JSON-safe summary: identifiers, counts and check results, never data values."""
        return {
            "landing_path": self.landing_path,
            "feed": self.feed.name,
            "file_type": self.file_type.file_type if self.file_type else None,
            "delivery_month": self.delivery_month,
            "checksum_sha256": self.checksum,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count,
            "outcome": "accepted" if self.accepted else "rejected",
            "failed_checks": list(self.failed_checks),
            "checks": [c.to_dict() for c in self.checks],
        }


def validate_file(
    landing_path: str, local_file: Path, config: LakehouseConfig
) -> ValidationResult | None:
    """Validate a file delivered to landing_path, read from local_file.

    Returns None for a path outside every feed: it is not a delivery and is left alone.
    Anything under a feed prefix gets a result, so nothing is silently left in landing.
    """
    feed = config.feed_for_path(landing_path)
    if feed is None:
        return None

    size = local_file.stat().st_size
    results: list[CheckResult] = []

    def finish(
        file_type: FileType | None = None, month: str | None = None, rows: int | None = None
    ) -> ValidationResult:
        return ValidationResult(
            landing_path=landing_path,
            feed=feed,
            file_type=file_type,
            delivery_month=month,
            size_bytes=size,
            checksum=sha256_file(local_file),
            row_count=rows,
            checks=tuple(results),
        )

    def skip_rest(because: str) -> None:
        done = {c.name for c in results}
        results.extend(CheckResult.skipped(n, because) for n in CHECK_NAMES if n not in done)

    def run(*new: CheckResult) -> bool:
        results.extend(new)
        return all(c.status is CheckStatus.PASSED for c in new)

    # Where it landed: without a known file type there are no rules to check against.
    landing = config.parse_landing_path(landing_path)
    if not run(checks.check_landing_path(landing is not None, feed.prefix)):
        skip_rest("landing_path")
        return finish()
    known = [ft.file_type for ft in config.file_types_for_feed(feed.name)]
    if not run(checks.check_known_file_type(landing.file_type, feed.name, known)):
        skip_rest("known_file_type")
        return finish()
    file_type = config.file_type_for(landing)
    month = file_type.delivery_month(landing.file_name)

    # The file: the name is independent of the content; an empty or oversized file is not read.
    run(checks.check_file_name(landing.file_name, file_type.file_name_regex))
    if not run(checks.check_file_size(size, file_type.rules.max_file_bytes)):
        skip_rest("file_size")
        return finish(file_type, month)

    with local_file.open("rb") as binary:
        profile = profile_csv(binary, file_type.rules.encoding, file_type.key_columns)
    run(checks.check_no_bom(profile))
    if not run(checks.check_encoding(profile, file_type.rules.encoding)):
        skip_rest("encoding")
        return finish(file_type, month)
    if not run(checks.check_csv_parseable(profile)):
        skip_rest("csv_parseable")
        return finish(file_type, month)

    # Structure: complete facts from a full parse.
    run(checks.check_no_embedded_line_breaks(profile))
    header_ok = run(checks.check_header(profile.header, file_type.expected_columns))
    run(checks.check_field_counts(profile), checks.check_has_rows(profile))
    if header_ok:
        run(checks.check_key_columns(profile, file_type.key_columns))
    else:
        skip_rest("header")  # key positions come from the header
    return finish(file_type, month, profile.row_count)
