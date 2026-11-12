"""The notifier: classifying job runs, alerts, and the GitHub issue lifecycle."""

from __future__ import annotations

import io
import json

import pytest

from claims_lakehouse.jobs.outcome import PipelineResult, classify_run, violated_expectations
from claims_lakehouse.notifier.alerts import report_from_alert
from claims_lakehouse.notifier.github import GitHubIssues
from claims_lakehouse.notifier.issues import LABEL, Issue, reconcile
from claims_lakehouse.notifier.report import Problem, Report, Status, covers

FEEDS = ["cclf", "humana"]
PAYER_FEEDS = {"cms_ffs": "cclf", "humana_ma": "humana"}


class FakeTracker:
    """An in-memory issue tracker with GitHub's open/comment/close behaviour."""

    def __init__(self) -> None:
        self.issues: dict[int, dict] = {}

    def open_issues(self, label: str) -> list[Issue]:
        return [
            Issue(n, i["title"], i["body"])
            for n, i in self.issues.items()
            if i["open"] and label in i["labels"]
        ]

    def create(self, title: str, body: str, labels: list[str]) -> int:
        number = len(self.issues) + 1
        self.issues[number] = {
            "title": title, "body": body, "labels": labels, "open": True, "comments": []
        }  # fmt: skip
        return number

    def comment(self, number: int, body: str) -> None:
        self.issues[number]["comments"].append(body)

    def close(self, number: int, comment: str) -> None:
        self.comment(number, comment)
        self.issues[number]["open"] = False

    def open_titles(self) -> list[str]:
        return sorted(i["title"] for i in self.issues.values() if i["open"])


def states(**overrides: str) -> dict[str, str]:
    """Task states of a clean incremental run, with overrides."""
    base = {
        "create_masks": "SUCCESS", "bronze": "SUCCESS", "gold": "SUCCESS",
        "answer_keys": "SUCCESS",
    }  # fmt: skip
    for feed in FEEDS:
        base |= {
            f"prepare_{feed}": "SUCCESS",
            f"full_refresh_{feed}": "SUCCESS",
            f"silver_{feed}_full_refresh": "EXCLUDED",
            f"silver_{feed}": "SUCCESS",
        }
    return base | overrides


def run(run_id: int, task_states=None, pipelines=None, answer_keys=None) -> Report:
    return classify_run(
        run_id,
        task_states or states(),
        FEEDS,
        pipelines or {},
        answer_keys if answer_keys is not None else {"status": "match"},
        PAYER_FEEDS,
    )


STOP_ERROR = (
    "[EXPECTATION_VIOLATION.VERBOSITY_ALL] Flow 'cclf_header_gate' failed to meet the "
    "expectation. Violated expectations: 'keys_unique'. Input data: "
    '\'{"first_name":"\'ALICE\'","duplicate_key_rows":"3"}\''
)


# --- classifying a run -------------------------------------------------------------------


def test_a_clean_run_has_no_problems_and_covers_every_check():
    report = run(1)
    assert report.problems == ()
    assert report.status is Status.PASSED
    for scope in ["all/task/bronze", "cclf/expectation", "humana/task/silver", "all/answer_keys",
                  "cclf/answer_keys", "all/task/gold"]:  # fmt: skip
        assert scope in report.scopes


def test_a_failed_shared_task_is_an_error_for_all_feeds_and_checks_nothing_downstream():
    report = run(2, states(bronze="FAILED", prepare_cclf="UPSTREAM_FAILED"))
    assert [(p.feed, p.check, p.status) for p in report.problems] == [
        ("all", "task/bronze", Status.ERROR)
    ]
    # Nothing after bronze ran, so no payer issue may close.
    assert report.scopes == ("all/task/create_masks",)


def test_warn_expectation_failures_warn_only_their_feed():
    pipelines = {"humana": PipelineResult({"claim_amount_format": 4, "keys_present": 0})}
    report = run(3, pipelines=pipelines)
    assert [(p.feed, p.check, p.status) for p in report.problems] == [
        ("humana", "expectation/claim_amount_format", Status.WARNED)
    ]
    assert "4 rows" in report.problems[0].detail
    assert report.status is Status.WARNED


def test_a_stop_is_stopped_for_that_payer_and_the_other_payer_still_passes():
    task_states = states(silver_cclf="FAILED")
    pipelines = {"cclf": PipelineResult(stopped_by=violated_expectations([STOP_ERROR]))}
    report = run(4, task_states, pipelines)
    assert [(p.feed, p.check, p.status) for p in report.problems] == [
        ("cclf", "expectation/keys_unique", Status.STOPPED)
    ]
    assert "humana/expectation" in report.scopes
    assert "cclf/expectation" not in report.scopes  # the stopped update checked only part


def test_a_pipeline_failure_without_an_expectation_is_an_error():
    report = run(5, states(silver_humana_full_refresh="FAILED", silver_humana="EXCLUDED"))
    assert [(p.feed, p.check, p.status) for p in report.problems] == [
        ("humana", "task/silver", Status.ERROR)
    ]
    assert "silver_humana_full_refresh ended FAILED" in report.problems[0].detail


def test_answer_key_mismatches_are_keyed_by_the_payer_feed():
    answer_keys = {
        "status": "mismatch",
        "mismatches": [
            {"check": "member_months", "payer_id": "humana_ma", "detail": "3 months differ"},
            {"check": "switchers", "payer_id": None, "detail": "16 switchers, expected 17"},
        ],
    }
    report = run(6, states(answer_keys="FAILED"), answer_keys=answer_keys)
    assert [(p.feed, p.check) for p in report.problems] == [
        ("humana", "answer_keys/member_months"),
        ("all", "answer_keys/switchers"),
    ]


def test_an_incomplete_answer_key_check_passes():
    assert run(7, answer_keys={"status": "incomplete", "cms_ffs": 24}).problems == ()


def test_an_answer_key_task_that_crashed_is_an_error():
    report = run(8, states(answer_keys="FAILED"), answer_keys={})
    assert [p.check for p in report.problems] == ["task/answer_keys"]


def test_violated_expectations_reads_names_and_never_row_values():
    assert violated_expectations([STOP_ERROR]) == ("keys_unique",)
    two = "EXPECTATION_VIOLATION ... Violated expectations: 'a_ok', 'b_ok'. Input data: 'x'"
    assert violated_expectations([two]) == ("a_ok", "b_ok")
    assert violated_expectations(["EXPECTATION_VIOLATION, format changed"]) == ("unknown",)
    assert violated_expectations(["Out of memory"]) == ()
    assert "ALICE" not in json.dumps(
        run(
            4,
            states(silver_cclf="FAILED"),
            {"cclf": PipelineResult(stopped_by=violated_expectations([STOP_ERROR]))},
        ).to_json()
    )


def test_reports_round_trip_through_json():
    report = run(3, pipelines={"cclf": PipelineResult({"x_format": 2})})
    assert Report.from_json(report.to_json()) == report


def test_covers_matches_whole_path_segments():
    assert covers("cclf/expectation", "cclf/expectation/keys_unique")
    assert covers("cclf/expectation", "cclf/expectation")
    assert not covers("cclf/expectation", "cclf/expectations_extra")
    assert not covers("cclf", "cclf_v2/task/silver")


# --- the issue lifecycle -----------------------------------------------------------------


def test_a_stop_opens_one_issue_comments_while_it_persists_and_closes_on_recovery():
    tracker = FakeTracker()
    stopped = {"cclf": PipelineResult(stopped_by=("keys_unique",))}

    actions = reconcile(run(10, states(silver_cclf="FAILED"), stopped), tracker)
    assert [a.kind for a in actions] == ["opened"]
    assert tracker.open_titles() == ["[stopped] cclf: expectation/keys_unique"]
    issue = tracker.issues[1]
    assert "stopped" in issue["labels"] and LABEL in issue["labels"]

    actions = reconcile(run(11, states(silver_cclf="FAILED"), stopped), tracker)
    assert [a.kind for a in actions] == ["commented"]
    assert "run 11" in issue["comments"][-1]

    actions = reconcile(run(12), tracker)
    assert [a.kind for a in actions] == ["closed"]
    assert tracker.open_titles() == []
    assert "run 12" in issue["comments"][-1]


def test_a_run_that_did_not_reach_a_check_leaves_its_issue_open():
    tracker = FakeTracker()
    reconcile(run(20, pipelines={"humana": PipelineResult({"amount_format": 1})}), tracker)
    reconcile(run(21, states(bronze="FAILED")), tracker)
    assert tracker.open_titles() == [
        "[error] all: task/bronze",
        "[warned] humana: expectation/amount_format",
    ]
    reconcile(run(22), tracker)
    assert tracker.open_titles() == []


def test_one_payer_recovering_never_closes_the_other_payers_issue():
    tracker = FakeTracker()
    both = {"cclf": PipelineResult({"a_format": 1}), "humana": PipelineResult({"b_format": 1})}
    reconcile(run(30, pipelines=both), tracker)
    reconcile(run(31, pipelines={"humana": PipelineResult({"b_format": 2})}), tracker)
    assert tracker.open_titles() == ["[warned] humana: expectation/b_format"]


def test_alert_issues_and_job_issues_never_close_each_other():
    tracker = FakeTracker()
    reconcile(report_from_alert(alert("Fired", feed="cclf")), tracker)
    reconcile(run(40), tracker)  # the job checking cclf does not close the alert's issue
    assert tracker.open_titles() == ["[error] cclf: alert/files-quarantined"]
    reconcile(report_from_alert(alert("Resolved", feed="cclf")), tracker)
    assert tracker.open_titles() == []


def test_issues_without_a_key_are_left_alone():
    tracker = FakeTracker()
    tracker.create("A hand-written issue", "no key here", [LABEL])
    reconcile(run(50), tracker)
    assert tracker.open_titles() == ["A hand-written issue"]


# --- alerts ------------------------------------------------------------------------------


def alert(condition: str, feed: str | None = None, severity: str = "Sev2") -> dict:
    dimensions = [{"name": "feed", "value": feed}] if feed else []
    return {
        "schemaId": "azureMonitorCommonAlertSchema",
        "data": {
            "essentials": {
                "alertId": "/subscriptions/00000000-0000-0000-0000-000000000000/providers/"
                "Microsoft.AlertsManagement/alerts/11111111-1111-1111-1111-111111111111",
                "alertRule": "files-quarantined",
                "severity": severity,
                "monitorCondition": condition,
                "firedDateTime": "2026-11-11T19:00:00Z",
                "alertTargetIDs": ["/subscriptions/00000000-0000-0000-0000-000000000000/x"],
            },
            "alertContext": {
                "condition": {"allOf": [{"metricValue": 3, "dimensions": dimensions}]}
            },
        },
    }


def test_a_fired_alert_is_a_problem_for_its_feed_dimension():
    report = report_from_alert(json.dumps(alert("Fired", feed="humana")))
    assert report.problems == (
        Problem("humana", "alert/files-quarantined", Status.ERROR, "3 in the alert window"),
    )
    assert report.scopes == ()


def test_an_alert_without_a_feed_is_for_all_feeds_and_low_severity_warns():
    [problem] = report_from_alert(alert("Fired", severity="Sev3")).problems
    assert (problem.feed, problem.status) == ("all", Status.WARNED)


def test_a_resolved_alert_scopes_only_its_own_rule():
    report = report_from_alert(alert("Resolved", feed="cclf"))
    assert report.problems == ()
    assert report.scopes == ("cclf/alert/files-quarantined",)


def test_alert_reports_never_carry_azure_ids():
    text = report_from_alert(alert("Fired", feed="cclf")).to_json()
    assert "subscriptions" not in text and "0000" not in text


def test_other_payloads_are_rejected():
    with pytest.raises(ValueError):
        report_from_alert({"schemaId": "something-else"})


# --- the GitHub client -------------------------------------------------------------------


class FakeGitHub:
    def __init__(self, pages: list[list[dict]]) -> None:
        self.pages = pages
        self.requests: list[tuple[str, str, dict | None]] = []

    def __call__(self, request):
        body = json.loads(request.data) if request.data else None
        self.requests.append((request.get_method(), request.full_url, body))
        if request.get_method() == "GET":
            payload = self.pages.pop(0)
        elif request.full_url.endswith("/issues"):
            payload = {"number": 7}
        else:
            payload = {}
        return io.BytesIO(json.dumps(payload).encode())


def test_github_lists_every_page_of_open_issues_and_skips_pull_requests():
    first = [{"number": n, "title": "t", "body": "b"} for n in range(100)]
    second = [{"number": 100, "title": "t", "body": None},
              {"number": 101, "title": "pr", "body": "", "pull_request": {}}]  # fmt: skip
    fake = FakeGitHub([first, second])
    issues = GitHubIssues("owner/repo", "token", fake).open_issues(LABEL)
    assert [i.number for i in issues] == [*range(100), 100]
    assert all("labels=notifier" in url and "state=open" in url for _, url, _ in fake.requests)


def test_github_create_comment_and_close_call_the_issues_api():
    fake = FakeGitHub([])
    github = GitHubIssues("owner/repo", "token", fake)
    assert github.create("title", "body", [LABEL, "warned"]) == 7
    github.close(7, "Recovered")
    assert [(m, url.removeprefix("https://api.github.com/repos/owner/repo")) for m, url, _ in
            fake.requests] == [("POST", "/issues"), ("POST", "/issues/7/comments"),
                               ("PATCH", "/issues/7")]  # fmt: skip
    assert fake.requests[-1][2] == {"state": "closed", "state_reason": "completed"}


def test_a_repaired_task_counts_its_latest_attempt():
    from claims_lakehouse.jobs.outcome import latest_states

    attempts = [
        ("answer_keys", 100, "FAILED"),
        ("gold", 50, "SUCCESS"),
        ("answer_keys", 200, "SUCCESS"),
    ]
    assert latest_states(attempts) == {"answer_keys": "SUCCESS", "gold": "SUCCESS"}
    assert latest_states(reversed(attempts))["answer_keys"] == "SUCCESS"


def test_alert_issues_name_the_rule_once():
    tracker = FakeTracker()
    reconcile(report_from_alert(alert("Fired", feed="cclf")), tracker)
    body = tracker.issues[1]["body"]
    assert "from alert files-quarantined (2026-11-11T19:00:00Z)" in body
    assert "alert alert" not in body
