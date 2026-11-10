"""File-level validation: every check is shown failing on a deliberately broken file."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from validation_fixtures import DATA_VALUES, FILE_NAME, FIXTURE_DIR, FIXTURES

from claims_lakehouse.config import load_config
from claims_lakehouse.validation.checks import CheckStatus
from claims_lakehouse.validation.outcome import (
    archive_path,
    error_report_path,
    quarantine_path,
    sha256_file,
)
from claims_lakehouse.validation.validate import CHECK_NAMES, validate_file

LANDING = f"cms/cclf/claims_diagnosis/{FILE_NAME}"


@pytest.fixture(scope="module")
def config():
    return load_config()


def fixture(case: str) -> Path:
    return FIXTURE_DIR / case / FILE_NAME


def statuses(result) -> dict[str, CheckStatus]:
    return {c.name: c.status for c in result.checks}


def details(result, name: str) -> str:
    return next(c.details for c in result.checks if c.name == name)


@pytest.mark.parametrize("case", FIXTURES)
def test_fixture_files_are_byte_exact(case):
    assert fixture(case).read_bytes() == FIXTURES[case], "run: python tests/validation_fixtures.py"


# --- Valid files -------------------------------------------------------------------------


@pytest.mark.parametrize("case", ["valid", "valid_crlf", "valid_quoted"])
def test_valid_file_is_accepted(config, case):
    result = validate_file(LANDING, fixture(case), config)
    assert result.accepted, result.report()
    assert [c.name for c in result.checks] == list(CHECK_NAMES)
    assert result.row_count == 3
    assert result.delivery_month == "2023-01"
    assert result.checksum == hashlib.sha256(FIXTURES[case]).hexdigest()
    assert result.archive_path == (
        "cms/cclf/claims_diagnosis/delivery_month=2023-01/"
        f"checksum={result.checksum}/CCLF4_202301.csv"
    )


def test_resend_under_the_same_name_gets_a_new_archive_path(config):
    """Auto Loader skips paths it has seen, so a corrected re-send needs its own path."""
    original = validate_file(LANDING, fixture("valid"), config)
    resend = validate_file(LANDING, fixture("valid_crlf"), config)
    assert original.delivery_month == resend.delivery_month
    assert original.archive_path != resend.archive_path


# --- Each broken file fails exactly the checks it should ---------------------------------

BROKEN = {
    "bom_utf8": ({"no_bom"}, "file starts with a UTF-8 byte order mark"),
    "utf16": ({"no_bom", "encoding"}, "line 1, byte 3: NUL byte (is the file UTF-16?)"),
    "latin1": ({"encoding"}, "not valid utf-8 at line 3, byte 295: invalid continuation byte"),
    "truncated_utf8": ({"encoding"}, "byte 414: unexpected end of data"),
    "unclosed_quote": ({"csv_parseable"}, "malformed CSV at line 4: unexpected end of data"),
    "text_after_quote": ({"csv_parseable"}, "malformed CSV at line 3"),
    "embedded_line_break": ({"no_embedded_line_breaks"}, "e.g. record ending on line 4"),
    "ragged": (
        {"field_counts"},
        "2 row(s) without 12 fields; e.g. line 3: 11 fields, line 4: 13 fields",
    ),
    "empty_key": ({"key_columns"}, "e.g. line 3: CUR_CLM_UNIQ_ID, line 4: BENE_MBI_ID"),
    "header_renamed": ({"header"}, "missing ['CLM_DGNS_CD']; unexpected ['CLM_DGNS_CODE']"),
    "header_reordered": (
        {"header"},
        "order differs at column 1: got 'BENE_MBI_ID', expected 'CUR_CLM_UNIQ_ID'",
    ),
    "header_lowercase": ({"header"}, "differ from the expected ones only in case"),
    "header_missing_column": ({"header"}, "missing ['DGNS_PRCDR_ICD_IND']"),
    "header_extra_column": ({"header"}, "unexpected ['NEW_COL']"),
    "no_header": ({"header"}, "12 of 12 columns are not expected"),
    "header_only": ({"has_rows"}, "header but no data rows"),
    "empty": ({"file_size"}, "file is empty (0 bytes)"),
    "several_problems": ({"no_bom", "field_counts", "key_columns"}, ""),
}


@pytest.mark.parametrize(
    ("case", "failed", "message"),
    [(case, *expected) for case, expected in BROKEN.items()],
    ids=list(BROKEN),
)
def test_broken_file_fails_exactly_its_checks(config, case, failed, message):
    result = validate_file(LANDING, fixture(case), config)
    assert not result.accepted
    assert set(result.failed_checks) == failed
    assert [c.name for c in result.checks] == list(CHECK_NAMES)  # every check is reported
    if message:
        assert any(message in details(result, name) for name in failed), result.report()


def test_checks_after_an_unreadable_file_are_skipped_not_failed(config):
    result = validate_file(LANDING, fixture("latin1"), config)
    after = CHECK_NAMES[CHECK_NAMES.index("encoding") + 1 :]
    assert all(statuses(result)[name] is CheckStatus.SKIPPED for name in after)
    assert details(result, "header") == "skipped: encoding failed"
    assert result.row_count is None


def test_key_columns_are_skipped_when_the_header_is_wrong(config):
    result = validate_file(LANDING, fixture("header_renamed"), config)
    assert statuses(result)["key_columns"] is CheckStatus.SKIPPED
    assert statuses(result)["field_counts"] is CheckStatus.PASSED


def test_file_over_the_size_limit_is_not_read(config):
    ft = config.file_types["claims_diagnosis"]
    small = replace(ft, rules=replace(ft.rules, max_file_bytes=100))
    small_config = replace(config, file_types={**config.file_types, "claims_diagnosis": small})
    result = validate_file(LANDING, fixture("valid"), small_config)
    assert result.failed_checks == ("file_size",)
    assert "414 bytes exceeds the limit of 100" in details(result, "file_size")
    assert statuses(result)["encoding"] is CheckStatus.SKIPPED


# --- Where the file landed and what it is called -----------------------------------------


def test_path_outside_every_feed_is_not_a_delivery(config):
    assert validate_file(f"other/claims_diagnosis/{FILE_NAME}", fixture("valid"), config) is None


@pytest.mark.parametrize(
    ("path", "failed", "message"),
    [
        (f"cms/cclf/{FILE_NAME}", "landing_path", "expected cms/cclf/<file_type>/<file_name>"),
        (f"cms/cclf/claims_diagnosis/x/{FILE_NAME}", "landing_path", "<file_type>/<file_name>"),
        (f"cms/cclf/diagnosis/{FILE_NAME}", "known_file_type", "no file type 'diagnosis'"),
        (f"humana/ma/claims_diagnosis/{FILE_NAME}", "known_file_type", "in feed 'humana'"),
    ],
)
def test_unknown_location_fails_and_skips_the_rest(config, path, failed, message):
    result = validate_file(path, fixture("valid"), config)
    assert result.failed_checks == (failed,)
    assert message in details(result, failed)
    after = CHECK_NAMES[CHECK_NAMES.index(failed) + 1 :]
    assert all(statuses(result)[name] is CheckStatus.SKIPPED for name in after)
    folder = path.rsplit("/", 1)[0]
    assert result.quarantine_path == f"{folder}/checksum={result.checksum}/{FILE_NAME}"


@pytest.mark.parametrize(
    "file_name",
    ["CCLF4_2023-01.csv", "CCLF4_202313.csv", "CCLF1_202301.csv", "CCLF4_202301.txt"],
)
def test_bad_file_name_fails_only_the_name_check(config, file_name):
    result = validate_file(f"cms/cclf/claims_diagnosis/{file_name}", fixture("valid"), config)
    assert result.failed_checks == ("file_name",)
    assert result.delivery_month is None
    assert result.row_count == 3  # the content was still checked
    with pytest.raises(ValueError, match="no archive path"):
        _ = result.archive_path


# --- Reports never carry data ------------------------------------------------------------


@pytest.mark.parametrize("case", FIXTURES)
def test_report_has_identifiers_and_counts_but_no_data_values(config, case):
    report = validate_file(LANDING, fixture(case), config).report()
    text = json.dumps({k: v for k, v in report.items() if k != "checksum_sha256"})
    assert [value for value in DATA_VALUES if value in text] == []
    assert report["outcome"] == ("accepted" if case.startswith("valid") else "rejected")


# --- Archive and quarantine layout -------------------------------------------------------

SHA = "ab" * 32


def test_layout_paths():
    assert archive_path(
        "humana/ma/", "member_eligibility", "2025-12", SHA, "HUM_ELIG_202512.csv"
    ) == (f"humana/ma/member_eligibility/delivery_month=2025-12/checksum={SHA}/HUM_ELIG_202512.csv")
    assert quarantine_path(LANDING, SHA) == f"cms/cclf/claims_diagnosis/checksum={SHA}/{FILE_NAME}"
    assert error_report_path(LANDING, SHA) == quarantine_path(LANDING, SHA) + ".error.json"


@pytest.mark.parametrize(
    ("month", "checksum", "message"),
    [
        ("2025-13", SHA, "YYYY-MM"),
        ("202512", SHA, "YYYY-MM"),
        ("2025-12", SHA.upper(), "lowercase hex"),
        ("2025-12", SHA[:-1], "lowercase hex"),
    ],
)
def test_layout_rejects_bad_parts(month, checksum, message):
    with pytest.raises(ValueError, match=message):
        archive_path("cms/cclf/", "claims_header", month, checksum, "CCLF1_202512.csv")


def test_sha256_file(tmp_path):
    path = tmp_path / "x.csv"
    path.write_bytes(b"a,b\n1,2\n")
    assert sha256_file(path) == hashlib.sha256(b"a,b\n1,2\n").hexdigest()
