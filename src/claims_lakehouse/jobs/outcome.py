"""Classify a finished job run, per feed, as a notifier report.

The job's last task (src/jobs/report_outcome.py) collects the task states, each payer's
pipeline expectation results and the answer-key task value, and this module turns them
into problems:

    warned   a warn or drop expectation failed rows in a payer's pipeline update
    stopped  a stop expectation failed: that payer's update failed, its tables keep their
             last good version
    error    any other failed task, or gold differing from the answer keys

Each problem is keyed by the feed its check reads; checks that read every feed (masks,
bronze, gold, the cross-payer answer keys) use "all". A check that ran to completion is
listed as a scope, so its issue closes once it passes again. A check that did not run
(say, a payer's silver after bronze failed) is neither a problem nor a scope: its issues
stay as they are.

Pipeline error text can quote rows ("Input data: ..."), so only expectation names are
taken from it, and only names made of word characters.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from claims_lakehouse.notifier.report import ALL_FEEDS, Problem, Report, Status

SHARED_TASKS = ("create_masks", "bronze")
SUCCESS = "SUCCESS"
# Tasks a run skipped: not run because of a condition or an upstream failure.
NOT_RUN = {None, "EXCLUDED", "UPSTREAM_FAILED", "UPSTREAM_CANCELED"}

_VIOLATION = "EXPECTATION_VIOLATION"
_VIOLATED = re.compile(r"[Vv]iolated expectations?: ((?:'[\w.\-]+'(?:,\s*)?)+)")
_NAME = re.compile(r"'([\w.\-]+)'")


@dataclass(frozen=True)
class PipelineResult:
    """One payer's pipeline update in this run."""

    expectation_failures: Mapping[str, int] = field(default_factory=dict)  # name -> rows
    stopped_by: tuple[str, ...] = ()  # stop expectations that failed the update


def violated_expectations(error_texts: Iterable[str]) -> tuple[str, ...]:
    """Names of the expectations that failed an update, from its error events. Empty when
    the update failed for another reason; ("unknown",) when the names cannot be read."""
    names: list[str] = []
    violation = False
    for text in error_texts:
        if _VIOLATION not in (text or ""):
            continue
        violation = True
        for listed in _VIOLATED.findall(text):
            names += [n for n in _NAME.findall(listed) if n not in names]
    if violation and not names:
        return ("unknown",)
    return tuple(names)


def _failed(state: str | None) -> bool:
    return state not in NOT_RUN and state != SUCCESS


def classify_run(
    run_id: int | str,
    states: Mapping[str, str | None],
    feeds: Iterable[str],
    pipelines: Mapping[str, PipelineResult],
    answer_keys: Mapping[str, Any] | None,
    payer_feeds: Mapping[str, str],
) -> Report:
    feeds = list(feeds)
    problems: list[Problem] = []
    scopes: list[str] = []

    def task_problem(feed: str, check: str, task: str) -> None:
        problems.append(Problem(feed, check, Status.ERROR, f"task {task} ended {states[task]}"))

    for task in SHARED_TASKS:
        if states.get(task) == SUCCESS:
            scopes.append(f"{ALL_FEEDS}/task/{task}")
        elif _failed(states.get(task)):
            task_problem(ALL_FEEDS, f"task/{task}", task)
    if any(states.get(t) != SUCCESS for t in SHARED_TASKS):
        return Report("job", f"run {run_id}", tuple(problems), tuple(scopes))

    for feed in feeds:
        prepare = f"prepare_{feed}"
        if _failed(states.get(prepare)):
            task_problem(feed, "task/prepare", prepare)
            continue
        if states.get(prepare) == SUCCESS:
            scopes.append(f"{feed}/task/prepare")

        ran = [
            t
            for t in (f"silver_{feed}_full_refresh", f"silver_{feed}")
            if states.get(t) not in NOT_RUN
        ]
        if not ran:
            continue
        silver = ran[0]
        result = pipelines.get(feed) or PipelineResult()
        if states[silver] == SUCCESS:
            scopes += [f"{feed}/task/silver", f"{feed}/expectation"]
            problems += [
                Problem(feed, f"expectation/{name}", Status.WARNED,
                        f"{rows} rows failed this expectation in {silver} (kept or dropped)")
                for name, rows in sorted(result.expectation_failures.items())
                if rows > 0
            ]  # fmt: skip
        elif result.stopped_by:
            scopes.append(f"{feed}/task/silver")
            problems += [
                Problem(feed, f"expectation/{name}", Status.STOPPED,
                        f"stop expectation failed in {silver}; this payer's tables keep their "
                        "last good update")
                for name in result.stopped_by
            ]  # fmt: skip
        else:
            task_problem(feed, "task/silver", silver)

    gold = states.get("gold")
    if gold == SUCCESS:
        scopes.append(f"{ALL_FEEDS}/task/gold")
    elif _failed(gold):
        task_problem(ALL_FEEDS, "task/gold", "gold")

    checked = states.get("answer_keys")
    mismatch = (answer_keys or {}).get("status") == "mismatch"
    if checked == SUCCESS or (_failed(checked) and mismatch):
        scopes += [f"{f}/answer_keys" for f in [ALL_FEEDS, *feeds]]
        for m in (answer_keys or {}).get("mismatches", []) if mismatch else []:
            feed = payer_feeds.get(m.get("payer_id") or "", ALL_FEEDS)
            problems.append(
                Problem(feed, f"answer_keys/{m['check']}", Status.ERROR, str(m["detail"]))
            )
    elif _failed(checked):
        task_problem(ALL_FEEDS, "task/answer_keys", "answer_keys")

    return Report("job", f"run {run_id}", tuple(problems), tuple(scopes))
