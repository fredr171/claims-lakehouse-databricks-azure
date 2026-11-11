"""Handle one landing event: validate the file, then archive or quarantine it.

The validation Azure Function is a thin wrapper around handle_landing_event. Storage is
behind the BlobStore protocol, so this runs and is tested locally with no Azure access.

Event Grid delivers at least once, so handling is idempotent:
- archiving creates the blob only if the path is free; an existing path is a duplicate
  (paths are checksum-keyed, so the same path means the same bytes), and the check and
  the write are one atomic call, so two copies of an event cannot both write;
- quarantine writes overwrite, so a retried event rewrites the same report;
- the file is read only if it still has the ETag from the event. If a payer replaced it
  in landing before it was read, the newer upload has its own event, which handles it.

The landing file is left in place: landing holds files exactly as delivered.
"""

from __future__ import annotations

import base64
import binascii
import json
import tempfile
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from claims_lakehouse.config import LakehouseConfig
from claims_lakehouse.validation.validate import validate_file

LANDING = "landing"
ARCHIVE = "archive"
QUARANTINE = "quarantine"

_BLOB_CREATED = "Microsoft.Storage.BlobCreated"
_SUBJECT_PREFIX = "/blobServices/default/containers/"


class BlobStore(Protocol):
    def download(self, container: str, path: str, target: Path, etag: str | None) -> bool:
        """Write the blob to target. False if it is gone, or no longer has this ETag."""

    def create(self, container: str, path: str, source: Path) -> bool:
        """Upload source to a new blob. False, writing nothing, if the blob exists."""

    def put(self, container: str, path: str, source: Path | bytes) -> None:
        """Upload, replacing any existing blob."""


@dataclass(frozen=True)
class LandingEvent:
    path: str  # relative to the landing container
    etag: str | None


class Outcome(StrEnum):
    ARCHIVED = "archived"
    DUPLICATE = "duplicate"  # identical file already archived
    QUARANTINED = "quarantined"
    IGNORED = "ignored"  # outside every feed: not a delivery
    GONE = "gone"  # deleted or replaced in landing before it was read


@dataclass(frozen=True)
class LandingResult:
    landing_path: str
    outcome: Outcome
    target_path: str | None = None  # archive or quarantine path
    failed_checks: tuple[str, ...] = ()
    checksum: str | None = None

    def log_record(self) -> dict[str, Any]:
        """Identifiers and check names only, never data values."""
        return {
            "landing_path": self.landing_path,
            "outcome": self.outcome.value,
            "target_path": self.target_path,
            "failed_checks": list(self.failed_checks),
            "checksum_sha256": self.checksum,
        }


def parse_event(body: bytes | str) -> LandingEvent | None:
    """The landing file named by an Event Grid BlobCreated event (Event Grid schema).

    Accepts the event as JSON or base64-encoded JSON, since a queue message can carry
    either. Returns None for an event that is not a blob created in landing. A message
    that is not an event at all raises ValueError, so it ends in the poison queue.
    """
    text = body.decode("utf-8") if isinstance(body, bytes) else body
    try:
        event = json.loads(text)
    except json.JSONDecodeError:
        try:
            event = json.loads(base64.b64decode(text, validate=True))
        except (binascii.Error, ValueError) as e:
            raise ValueError("queue message is neither JSON nor base64 JSON") from e
    if isinstance(event, list) and len(event) == 1:
        [event] = event
    if not isinstance(event, dict) or "subject" not in event:
        raise ValueError("queue message is not an Event Grid event")

    if event.get("eventType") != _BLOB_CREATED:
        return None
    container, sep, path = event["subject"].removeprefix(_SUBJECT_PREFIX).partition("/blobs/")
    if not sep or container != LANDING or not path:
        return None
    etag = (event.get("data") or {}).get("eTag")
    return LandingEvent(path=path, etag=etag)


def handle_landing_event(
    event: LandingEvent, store: BlobStore, config: LakehouseConfig
) -> LandingResult:
    if config.feed_for_path(event.path) is None:
        return LandingResult(event.path, Outcome.IGNORED)

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "delivery"
        if not store.download(LANDING, event.path, local, event.etag):
            return LandingResult(event.path, Outcome.GONE)

        result = validate_file(event.path, local, config)
        assert result is not None  # the path is under a feed

        if result.accepted:
            target = result.archive_path
            if not store.create(ARCHIVE, target, local):
                return LandingResult(event.path, Outcome.DUPLICATE, target, (), result.checksum)
            return LandingResult(event.path, Outcome.ARCHIVED, target, (), result.checksum)

        store.put(QUARANTINE, result.quarantine_path, local)
        report = json.dumps(result.report(), indent=2).encode("utf-8")
        store.put(QUARANTINE, result.error_report_path, report)
        return LandingResult(
            event.path,
            Outcome.QUARANTINED,
            result.quarantine_path,
            result.failed_checks,
            result.checksum,
        )
