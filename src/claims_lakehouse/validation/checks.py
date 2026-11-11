"""File-level validation checks. Each is a small function returning a CheckResult.

Checks report bad data, never raise for it. Row-level data quality (code formats, dates,
amounts) is out of scope here; that is the job of the silver expectations.

Details carry identifiers and counts only (line numbers, column names, sizes), never field
values, because they end up in the quarantine error report and in GitHub issues.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from claims_lakehouse.validation.csv_inspect import CsvProfile

MAX_LISTED_HEADER_CHANGES = 3


class CheckStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"  # a check it depends on failed, so its answer would mean nothing


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: CheckStatus
    details: str = ""

    @classmethod
    def passed(cls, name: str, details: str = "") -> CheckResult:
        return cls(name, CheckStatus.PASSED, details)

    @classmethod
    def failed(cls, name: str, details: str) -> CheckResult:
        return cls(name, CheckStatus.FAILED, details)

    @classmethod
    def skipped(cls, name: str, because: str) -> CheckResult:
        return cls(name, CheckStatus.SKIPPED, f"skipped: {because} failed")

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "status": self.status.value, "details": self.details}


# --- Where the file landed ---------------------------------------------------------------


def check_landing_path(is_delivery_path: bool, feed_prefix: str) -> CheckResult:
    name = "landing_path"
    if not is_delivery_path:
        return CheckResult.failed(name, f"expected {feed_prefix}<file_type>/<file_name>")
    return CheckResult.passed(name)


def check_known_file_type(folder: str, feed_name: str, known: Sequence[str]) -> CheckResult:
    name = "known_file_type"
    if folder not in known:
        return CheckResult.failed(
            name, f"no file type {folder!r} in feed {feed_name!r}; expected one of {sorted(known)}"
        )
    return CheckResult.passed(name, folder)


# --- The file itself ---------------------------------------------------------------------


def check_file_name(file_name: str, pattern: re.Pattern[str]) -> CheckResult:
    name = "file_name"
    if pattern.fullmatch(file_name):
        return CheckResult.passed(name, file_name)
    return CheckResult.failed(name, f"{file_name!r} does not match {pattern.pattern}")


def check_file_size(size_bytes: int, max_bytes: int) -> CheckResult:
    name = "file_size"
    if size_bytes == 0:
        return CheckResult.failed(name, "file is empty (0 bytes)")
    if size_bytes > max_bytes:
        return CheckResult.failed(name, f"{size_bytes} bytes exceeds the limit of {max_bytes}")
    return CheckResult.passed(name, f"{size_bytes} bytes")


def check_no_bom(profile: CsvProfile) -> CheckResult:
    """A byte order mark would become part of the first column name in Spark."""
    name = "no_bom"
    if profile.bom:
        return CheckResult.failed(name, f"file starts with a {profile.bom} byte order mark")
    return CheckResult.passed(name)


def check_encoding(profile: CsvProfile, encoding: str) -> CheckResult:
    name = "encoding"
    if profile.decode_error:
        return CheckResult.failed(name, f"not valid {encoding} at {profile.decode_error}")
    return CheckResult.passed(name, encoding)


# --- CSV structure -----------------------------------------------------------------------


def check_csv_parseable(profile: CsvProfile) -> CheckResult:
    name = "csv_parseable"
    if profile.parse_error:
        return CheckResult.failed(name, f"malformed CSV at {profile.parse_error}")
    return CheckResult.passed(name)


def check_no_embedded_line_breaks(profile: CsvProfile) -> CheckResult:
    """Auto Loader reads one record per line, so a quoted line break would split a row."""
    name = "no_embedded_line_breaks"
    if profile.multiline_rows:
        return CheckResult.failed(
            name,
            f"{profile.multiline_rows} record(s) with a line break inside a field; "
            f"e.g. {', '.join(profile.multiline_examples)}",
        )
    return CheckResult.passed(name)


def check_header(header: Sequence[str] | None, expected: Sequence[str]) -> CheckResult:
    """Exactly the expected columns: same names, same order, same case.

    Unexpected names are listed only for small drift (a renamed or added column). A file
    without a header has a data row in its place, so a large mismatch reports counts only.
    """
    name = "header"
    if header is None:
        return CheckResult.failed(name, "no header row")
    header, expected = list(header), list(expected)
    if header == expected:
        return CheckResult.passed(name, f"{len(header)} columns")

    if [c.upper() for c in header] == [c.upper() for c in expected]:
        return CheckResult.failed(name, "column names differ from the expected ones only in case")

    missing = [c for c in expected if c not in header]
    unexpected = [c for c in header if c not in expected]
    if len(unexpected) > MAX_LISTED_HEADER_CHANGES:
        return CheckResult.failed(
            name,
            f"{len(unexpected)} of {len(header)} columns are not expected and "
            f"{len(missing)} of {len(expected)} expected columns are missing "
            "(is the header row missing?)",
        )

    problems = []
    if missing:
        problems.append(f"missing {missing}")
    if unexpected:
        problems.append(f"unexpected {unexpected}")
    if duplicated := sorted({c for c in header if header.count(c) > 1}):
        problems.append(f"duplicated {duplicated}")
    if not problems:
        position = next(
            i for i, (got, want) in enumerate(zip(header, expected, strict=True)) if got != want
        )
        problems.append(
            f"order differs at column {position + 1}: "
            f"got {header[position]!r}, expected {expected[position]!r}"
        )
    return CheckResult.failed(name, "; ".join(problems))


def check_field_counts(profile: CsvProfile) -> CheckResult:
    name = "field_counts"
    if profile.ragged_rows:
        width = len(profile.header or [])
        return CheckResult.failed(
            name,
            f"{profile.ragged_rows} row(s) without {width} fields; "
            f"e.g. {', '.join(profile.ragged_examples)}",
        )
    return CheckResult.passed(name)


def check_has_rows(profile: CsvProfile) -> CheckResult:
    name = "has_rows"
    if profile.row_count == 0:
        return CheckResult.failed(name, "header but no data rows")
    return CheckResult.passed(name, f"{profile.row_count} rows")


def check_key_columns(profile: CsvProfile, key_columns: Sequence[str]) -> CheckResult:
    """Key columns are never empty or whitespace-only."""
    name = "key_columns"
    if profile.empty_keys:
        return CheckResult.failed(
            name,
            f"{profile.empty_keys} empty value(s) in key columns {list(key_columns)}; "
            f"e.g. {', '.join(profile.empty_key_examples)}",
        )
    return CheckResult.passed(name, ", ".join(key_columns))
