"""What the notifier acts on: the problems a source found, and what it checked.

A source is the lakehouse job (one report per run) or an Azure Monitor alert. Each problem
names the feed its failing check reads ("all" for a check that reads every feed), so one
payer's problem never opens or closes another payer's issue. A report also lists its
scopes: the checks it ran to completion. An open issue inside a scope whose problem is no
longer reported has recovered and is closed.

Problems carry identifiers and counts only, never data values: they become GitHub issues
in a public repository.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum

ALL_FEEDS = "all"


class Status(StrEnum):
    PASSED = "passed"
    WARNED = "warned"  # rows kept or dropped; nothing blocked
    STOPPED = "stopped"  # a stop expectation blocked a payer's published tables
    ERROR = "error"  # a task, a check or the platform failed


SEVERITY = [Status.PASSED, Status.WARNED, Status.STOPPED, Status.ERROR]


@dataclass(frozen=True)
class Problem:
    feed: str  # the feed the failing check reads, or ALL_FEEDS
    check: str  # stable, "/"-separated name: task/bronze, expectation/keys_unique, ...
    status: Status
    detail: str  # identifiers and counts only

    @property
    def path(self) -> str:
        return f"{self.feed}/{self.check}"


def covers(scope: str, path: str) -> bool:
    """A scope covers itself and every check below it: "cclf/expectation" covers
    "cclf/expectation/keys_unique"."""
    return path == scope or path.startswith(scope + "/")


@dataclass(frozen=True)
class Report:
    source: str  # "job" or "alert"
    reference: str  # job run ID or alert rule and time, quoted in issues
    problems: tuple[Problem, ...]
    scopes: tuple[str, ...]  # checks that ran to completion (feed/check prefixes)

    @property
    def status(self) -> Status:
        return max((p.status for p in self.problems), key=SEVERITY.index, default=Status.PASSED)

    def to_json(self) -> str:
        return json.dumps(
            {
                "source": self.source,
                "reference": self.reference,
                "status": self.status.value,
                "problems": [
                    {"feed": p.feed, "check": p.check, "status": p.status.value, "detail": p.detail}
                    for p in self.problems
                ],
                "scopes": list(self.scopes),
            }
        )

    @classmethod
    def from_json(cls, text: str | bytes) -> Report:
        data = json.loads(text)
        return cls(
            source=str(data["source"]),
            reference=str(data["reference"]),
            problems=tuple(
                Problem(str(p["feed"]), str(p["check"]), Status(p["status"]), str(p["detail"]))
                for p in data["problems"]
            ),
            scopes=tuple(str(s) for s in data["scopes"]),
        )
