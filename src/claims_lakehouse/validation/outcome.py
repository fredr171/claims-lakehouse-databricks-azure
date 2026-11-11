"""Checksums and the archive and quarantine layouts.

Archive (container `archive`):
    <feed prefix><file_type>/delivery_month=<YYYY-MM>/checksum=<sha256>/<file_name>

The checksum folder is required for correctness, not tidiness: Auto Loader tracks files by
path and never reprocesses one it has seen, so a corrected re-send under the same file name
must land at a new path. It also makes a duplicate delivery detectable by path alone.

Quarantine (container `quarantine`): the landing folder, then the checksum, so a rejected
file is found next to where it was delivered even when its file type or month is unknown:
    <landing folder>/checksum=<sha256>/<file_name>
    <landing folder>/checksum=<sha256>/<file_name>.error.json
"""

from __future__ import annotations

import hashlib
import posixpath
import re
from pathlib import Path

_SHA256 = re.compile(r"[0-9a-f]{64}")
_MONTH = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
_CHUNK_BYTES = 1024 * 1024


def sha256_file(path: Path) -> str:
    """Hex SHA-256, read in chunks so memory stays flat."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def archive_path(
    feed_prefix: str, file_type: str, delivery_month: str, checksum: str, file_name: str
) -> str:
    if not _MONTH.fullmatch(delivery_month):
        raise ValueError(f"delivery_month must be YYYY-MM, got {delivery_month!r}")
    return (
        f"{feed_prefix}{file_type}/delivery_month={delivery_month}/"
        f"checksum={_checked(checksum)}/{file_name}"
    )


def quarantine_path(landing_path: str, checksum: str) -> str:
    folder, file_name = posixpath.split(landing_path)
    return posixpath.join(folder, f"checksum={_checked(checksum)}", file_name)


def error_report_path(landing_path: str, checksum: str) -> str:
    return quarantine_path(landing_path, checksum) + ".error.json"


def _checked(checksum: str) -> str:
    if not _SHA256.fullmatch(checksum):
        raise ValueError("checksum must be a lowercase hex SHA-256")
    return checksum
