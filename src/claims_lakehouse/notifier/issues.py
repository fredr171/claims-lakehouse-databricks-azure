"""Turn a report into GitHub issue changes: one issue per problem, a comment while it
persists, closed on recovery.

An issue is identified by a hidden key in its body: source, feed and check. The title can
be edited freely without losing the link.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from claims_lakehouse.notifier.report import Problem, Report, covers

LABEL = "notifier"
_KEY = re.compile(r"<!-- notifier-key: (\S+) -->")


@dataclass(frozen=True)
class Issue:
    number: int
    title: str
    body: str

    @property
    def key(self) -> str | None:
        found = _KEY.search(self.body or "")
        return found.group(1) if found else None


class IssueTracker(Protocol):
    def open_issues(self, label: str) -> list[Issue]: ...
    def create(self, title: str, body: str, labels: list[str]) -> int: ...
    def comment(self, number: int, body: str) -> None: ...
    def close(self, number: int, comment: str) -> None: ...


@dataclass(frozen=True)
class Action:
    kind: str  # opened, commented, closed
    key: str
    number: int


def issue_key(source: str, path: str) -> str:
    return f"{source}/{path}"


def _title(problem: Problem) -> str:
    return f"[{problem.status.value}] {problem.feed}: {problem.check}"


def _body(report: Report, problem: Problem, key: str) -> str:
    return (
        f"<!-- notifier-key: {key} -->\n"
        f"Opened by the notifier from {report.source} {report.reference}.\n\n"
        f"- **Feed:** `{problem.feed}`\n"
        f"- **Check:** `{problem.check}`\n"
        f"- **Status:** {problem.status.value}\n"
        f"- **Detail:** {problem.detail}\n\n"
        f"Each later {report.source} that still finds it adds a comment; the first one that "
        "passes this check closes the issue."
    )


def reconcile(report: Report, tracker: IssueTracker) -> list[Action]:
    """Apply a report to the open issues and return what changed."""
    open_issues = {i.key: i for i in tracker.open_issues(LABEL) if i.key}
    current = {issue_key(report.source, p.path): p for p in report.problems}
    actions: list[Action] = []

    for key, problem in current.items():
        if (issue := open_issues.get(key)) is not None:
            tracker.comment(
                issue.number,
                f"Still {problem.status.value} in {report.source} {report.reference}: "
                f"{problem.detail}",
            )
            actions.append(Action("commented", key, issue.number))
        else:
            number = tracker.create(
                _title(problem), _body(report, problem, key), [LABEL, problem.status.value]
            )
            actions.append(Action("opened", key, number))

    prefix = f"{report.source}/"
    for key, issue in open_issues.items():
        if key in current or not key.startswith(prefix):
            continue
        path = key.removeprefix(prefix)
        if any(covers(scope, path) for scope in report.scopes):
            tracker.close(
                issue.number, f"Recovered: {report.source} {report.reference} passed this check."
            )
            actions.append(Action("closed", key, issue.number))
    return actions
