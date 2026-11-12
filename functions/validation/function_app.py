"""Validation Function: one landing event from the queue → archive or quarantine.

All the logic is in claims_lakehouse.validation.landing (tested locally). This file only
wires the queue trigger, the lake's blob storage and logging. A raised exception (a
storage error, say) makes the Functions host retry the message, then move it to the
poison queue; a rejected file is a normal outcome, not an exception.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import azure.functions as func
from blob_store import AzureBlobStore

from claims_lakehouse.config import load_config
from claims_lakehouse.validation.landing import handle_landing_event, parse_event

# The build copies config/ next to this file (see build.ps1).
CONFIG = load_config(Path(__file__).resolve().parent / "config")
STORE = AzureBlobStore(os.environ["LAKE_BLOB_ENDPOINT"])

app = func.FunctionApp()
log = logging.getLogger("validation")


@app.queue_trigger(
    arg_name="message", queue_name="%LANDING_EVENTS_QUEUE%", connection="AzureWebJobsStorage"
)
def validate_landing_file(message: func.QueueMessage) -> None:
    event = parse_event(message.get_body())
    if event is None:
        log.info("not a landing BlobCreated event; skipped")
        return
    result = handle_landing_event(event, STORE, CONFIG)
    # The feed lets the quarantine alert raise one notifier issue per feed.
    feed = CONFIG.feed_for_path(result.landing_path)
    record = result.log_record() | {"feed": feed.name if feed else None}
    log.info("validation %s", json.dumps(record))
