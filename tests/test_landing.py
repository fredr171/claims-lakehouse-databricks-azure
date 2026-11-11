"""The validation Function's handler: event parsing, archive, quarantine, idempotency."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from validation_fixtures import DATA_VALUES, FILE_NAME, FIXTURE_DIR

from claims_lakehouse.validation.landing import (
    ARCHIVE,
    LANDING,
    QUARANTINE,
    LandingEvent,
    Outcome,
    handle_landing_event,
    parse_event,
)

LANDING_PATH = f"cms/cclf/claims_diagnosis/{FILE_NAME}"
VALID = (FIXTURE_DIR / "valid" / FILE_NAME).read_bytes()
RAGGED = (FIXTURE_DIR / "ragged" / FILE_NAME).read_bytes()


class MemoryStore:
    """BlobStore in memory. Blobs carry an ETag that changes on every write."""

    def __init__(self) -> None:
        self.blobs: dict[tuple[str, str], bytes] = {}
        self.etags: dict[tuple[str, str], str] = {}
        self.writes: list[tuple[str, str]] = []
        self._version = 0

    def upload(self, container: str, path: str, data: bytes) -> str:
        self._version += 1
        self.blobs[container, path] = data
        self.etags[container, path] = f"0x{self._version:04X}"
        return self.etags[container, path]

    # BlobStore
    def download(self, container, path, target: Path, etag) -> bool:
        key = (container, path)
        if key not in self.blobs or (etag is not None and self.etags[key] != etag):
            return False
        target.write_bytes(self.blobs[key])
        return True

    def create(self, container, path, source: Path) -> bool:
        if (container, path) in self.blobs:
            return False
        self.writes.append((container, path))
        self.upload(container, path, source.read_bytes())
        return True

    def put(self, container, path, source) -> None:
        self.writes.append((container, path))
        data = source if isinstance(source, bytes) else source.read_bytes()
        self.upload(container, path, data)


def land(store: MemoryStore, data: bytes, path: str = LANDING_PATH) -> LandingEvent:
    return LandingEvent(path=path, etag=store.upload(LANDING, path, data))


def in_container(store: MemoryStore, container: str) -> dict[str, bytes]:
    return {p: b for (c, p), b in store.blobs.items() if c == container}


# --- Handling -----------------------------------------------------------------------------


def test_valid_file_is_archived_under_its_checksum(config):
    store = MemoryStore()
    result = handle_landing_event(land(store, VALID), store, config)

    assert result.outcome is Outcome.ARCHIVED
    assert result.target_path == (
        f"cms/cclf/claims_diagnosis/delivery_month=2023-01/checksum={result.checksum}/{FILE_NAME}"
    )
    assert in_container(store, ARCHIVE) == {result.target_path: VALID}
    assert in_container(store, QUARANTINE) == {}
    assert in_container(store, LANDING) == {LANDING_PATH: VALID}  # landing is left as delivered


def test_invalid_file_is_quarantined_with_an_error_report(config):
    store = MemoryStore()
    result = handle_landing_event(land(store, RAGGED), store, config)

    assert result.outcome is Outcome.QUARANTINED
    assert result.failed_checks == ("field_counts",)
    quarantined = in_container(store, QUARANTINE)
    assert quarantined[result.target_path] == RAGGED
    report = json.loads(quarantined[result.target_path + ".error.json"])
    assert report["outcome"] == "rejected"
    assert report["failed_checks"] == ["field_counts"]
    assert in_container(store, ARCHIVE) == {}


def test_error_report_contains_no_data_values(config):
    store = MemoryStore()
    result = handle_landing_event(land(store, RAGGED), store, config)
    report = in_container(store, QUARANTINE)[result.target_path + ".error.json"].decode()
    log = json.dumps(result.log_record())
    for value in DATA_VALUES:
        assert value not in report
        assert value not in log


def test_identical_redelivery_is_a_duplicate_and_writes_nothing(config):
    store = MemoryStore()
    first = handle_landing_event(land(store, VALID), store, config)
    store.writes.clear()

    again = handle_landing_event(land(store, VALID), store, config)
    assert again.outcome is Outcome.DUPLICATE
    assert again.target_path == first.target_path
    assert store.writes == []


def test_repeated_event_is_a_duplicate(config):
    """Event Grid delivers at least once: the same event twice archives once."""
    store = MemoryStore()
    event = land(store, VALID)
    assert handle_landing_event(event, store, config).outcome is Outcome.ARCHIVED
    assert handle_landing_event(event, store, config).outcome is Outcome.DUPLICATE
    assert len(in_container(store, ARCHIVE)) == 1


def test_corrected_resend_under_the_same_name_gets_a_new_archive_path(config):
    corrected = VALID.replace(b"E785", b"E786")
    assert corrected != VALID
    store = MemoryStore()
    original = handle_landing_event(land(store, VALID), store, config)
    resend = handle_landing_event(land(store, corrected), store, config)

    assert resend.outcome is Outcome.ARCHIVED
    assert resend.target_path != original.target_path
    assert in_container(store, ARCHIVE) == {
        original.target_path: VALID,
        resend.target_path: corrected,
    }


def test_retried_rejection_rewrites_the_same_quarantine_files(config):
    store = MemoryStore()
    event = land(store, RAGGED)
    first = handle_landing_event(event, store, config)
    second = handle_landing_event(event, store, config)
    assert first == second
    assert len(in_container(store, QUARANTINE)) == 2  # the file and its report


def test_file_replaced_in_landing_before_it_is_read_is_skipped(config):
    """The newer upload has its own event; the stale one must not archive the new bytes."""
    store = MemoryStore()
    stale = land(store, RAGGED)
    land(store, VALID)  # replaced under the same name

    assert handle_landing_event(stale, store, config).outcome is Outcome.GONE
    assert store.writes == []


def test_file_deleted_before_it_is_read_is_skipped(config):
    store = MemoryStore()
    event = land(store, VALID)
    del store.blobs[LANDING, LANDING_PATH]
    assert handle_landing_event(event, store, config).outcome is Outcome.GONE
    assert store.writes == []


def test_path_outside_every_feed_is_ignored_and_not_read(config):
    store = MemoryStore()
    event = land(store, VALID, path="scratch/notes.csv")
    store.download = None  # reading it would raise
    result = handle_landing_event(event, store, config)
    assert result.outcome is Outcome.IGNORED
    assert store.writes == []


def test_unknown_folder_under_a_feed_is_quarantined_not_ignored(config):
    """Nothing under a feed is silently left in landing."""
    store = MemoryStore()
    result = handle_landing_event(
        land(store, VALID, path=f"cms/cclf/other/{FILE_NAME}"), store, config
    )
    assert result.outcome is Outcome.QUARANTINED
    assert "known_file_type" in result.failed_checks


# --- Event parsing ------------------------------------------------------------------------


def blob_created(container: str = "landing", path: str = LANDING_PATH, **overrides) -> dict:
    event = {
        "id": "4f2a",
        "eventType": "Microsoft.Storage.BlobCreated",
        "subject": f"/blobServices/default/containers/{container}/blobs/{path}",
        "eventTime": "2026-11-11T12:00:00Z",
        "dataVersion": "",
        "data": {"api": "FlushWithClose", "eTag": "0x8DCAB12", "contentLength": 414},
    }
    event.update(overrides)
    return event


def test_parse_json_event():
    body = json.dumps(blob_created()).encode()
    assert parse_event(body) == LandingEvent(LANDING_PATH, "0x8DCAB12")


def test_parse_base64_event():
    body = base64.b64encode(json.dumps(blob_created()).encode())
    assert parse_event(body) == LandingEvent(LANDING_PATH, "0x8DCAB12")


def test_parse_event_without_etag():
    event = blob_created(data={"api": "PutBlob"})
    assert parse_event(json.dumps(event)) == LandingEvent(LANDING_PATH, None)


@pytest.mark.parametrize(
    "event",
    [
        blob_created(container="archive"),
        blob_created(eventType="Microsoft.Storage.BlobDeleted"),
        blob_created(subject="/blobServices/default/containers/landing"),
    ],
    ids=["other_container", "other_event_type", "no_blob"],
)
def test_events_that_are_not_landing_files_are_skipped(event):
    assert parse_event(json.dumps(event)) is None


@pytest.mark.parametrize("body", [b"not json", b"[]", b'{"hello": 1}', b"\xff\xfe"])
def test_message_that_is_not_an_event_raises(body):
    with pytest.raises(ValueError):
        parse_event(body)
