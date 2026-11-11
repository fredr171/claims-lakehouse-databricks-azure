"""Strict streaming inspection of a delivered CSV, collecting what the CSV checks need.

The rules are stricter than Python's csv module needs, because the file is later read by
Auto Loader (Spark CSV, one record per line). Anything the two readers could split
differently, such as a quoted field containing a line break, is recorded here so validation
can reject it before bronze ever sees it.

Examples name line numbers, byte offsets and column names only, never field values: the
error report and the GitHub issues built from it must not carry data.
"""

from __future__ import annotations

import codecs
import csv
import io
from dataclasses import dataclass, field
from typing import BinaryIO

BOMS = {
    codecs.BOM_UTF8: "UTF-8",
    codecs.BOM_UTF16_LE: "UTF-16 LE",
    codecs.BOM_UTF16_BE: "UTF-16 BE",
}
MAX_EXAMPLES = 5
_CHUNK_BYTES = 64 * 1024


@dataclass
class CsvProfile:
    """Facts about one file. A decode or parse error stops the inspection where it happened."""

    bom: str | None = None  # which byte order mark the file starts with, if any
    decode_error: str | None = None
    parse_error: str | None = None
    header: list[str] | None = None
    row_count: int = 0
    ragged_rows: int = 0
    ragged_examples: list[str] = field(default_factory=list)
    multiline_rows: int = 0
    multiline_examples: list[str] = field(default_factory=list)
    empty_keys: int = 0
    empty_key_examples: list[str] = field(default_factory=list)


def profile_csv(binary: BinaryIO, encoding: str, key_columns: tuple[str, ...]) -> CsvProfile:
    """Inspect a seekable binary stream: BOM, then encoding, then one strict CSV pass.

    A leading BOM is recorded and then skipped, so it is reported once (by its own check)
    instead of also breaking the header.
    """
    profile = CsvProfile()
    start = binary.read(3)
    bom = next((bom for bom in BOMS if start.startswith(bom)), b"")
    profile.bom = BOMS.get(bom)

    binary.seek(len(bom))
    profile.decode_error = _find_decode_error(binary, encoding, offset=len(bom))
    if profile.decode_error:
        return profile

    binary.seek(len(bom))
    text = io.TextIOWrapper(binary, encoding=encoding, errors="strict", newline="")
    try:
        _scan_rows(csv.reader(text, strict=True), key_columns, profile)
    finally:
        text.detach()  # leave the caller's stream open
    return profile


def _find_decode_error(binary: BinaryIO, encoding: str, offset: int) -> str | None:
    """Where the file first fails to decode (line and byte offset), or None.

    A NUL byte also fails: it is never valid in a text CSV, and it is how a UTF-16 file
    shows up when read as UTF-8 (its ASCII characters are all valid UTF-8 bytes).
    """
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    newlines = 0
    while True:
        chunk = binary.read(_CHUNK_BYTES)
        pending = len(decoder.getstate()[0])  # bytes held back from the previous chunk
        try:
            decoder.decode(chunk, final=not chunk)
        except UnicodeDecodeError as exc:
            # exc.object holds file bytes, so only the position is reported.
            in_chunk = max(exc.start - pending, 0)
            line = newlines + chunk[:in_chunk].count(b"\n") + 1
            return f"line {line}, byte {offset - pending + exc.start}: {exc.reason}"
        if (nul := chunk.find(b"\x00")) >= 0:
            line = newlines + chunk[:nul].count(b"\n") + 1
            return f"line {line}, byte {offset + nul}: NUL byte (is the file UTF-16?)"
        if not chunk:
            return None
        newlines += chunk.count(b"\n")
        offset += len(chunk)


def _scan_rows(reader, key_columns: tuple[str, ...], profile: CsvProfile) -> None:
    """Header, then every row: field counts, embedded line breaks, empty keys."""
    try:
        header = next(reader, None)
        if header is None:
            return
        profile.header = header
        width = len(header)
        key_indexes = {col: header.index(col) for col in key_columns if col in header}

        for row in reader:
            profile.row_count += 1
            line = reader.line_num  # physical line the record ends on
            if any("\n" in value or "\r" in value for value in row):
                profile.multiline_rows += 1
                _example(profile.multiline_examples, f"record ending on line {line}")
            if len(row) != width:
                profile.ragged_rows += 1
                _example(profile.ragged_examples, f"line {line}: {len(row)} fields")
                continue  # key positions are unknown in a ragged row
            for col, idx in key_indexes.items():
                if not row[idx].strip():
                    profile.empty_keys += 1
                    _example(profile.empty_key_examples, f"line {line}: {col}")
    except csv.Error as exc:
        profile.parse_error = f"line {reader.line_num}: {exc}"


def _example(examples: list[str], text: str) -> None:
    if len(examples) < MAX_EXAMPLES:
        examples.append(text)
