"""Notifier Function: job outcomes and Azure Monitor alerts in, GitHub issues out.

Two HTTP endpoints, both behind a function key:
    POST /api/job-outcome   a report from the job's last task (claims_lakehouse.jobs.outcome)
    POST /api/alert         an Azure Monitor action group webhook (common alert schema)

All the logic is in claims_lakehouse.notifier (tested locally). The GitHub token comes from
Key Vault through an app-setting reference, so it never appears in code or configuration.
A GitHub error returns 502, so the job task fails visibly and Azure Monitor retries.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error

import azure.functions as func

from claims_lakehouse.notifier.alerts import report_from_alert
from claims_lakehouse.notifier.github import GitHubIssues
from claims_lakehouse.notifier.issues import reconcile
from claims_lakehouse.notifier.report import Report

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
log = logging.getLogger("notifier")


def _apply(report: Report) -> func.HttpResponse:
    tracker = GitHubIssues(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"])
    try:
        actions = reconcile(report, tracker)
    except urllib.error.HTTPError as error:
        log.error("github %s for %s %s", error.code, report.source, report.reference)
        return func.HttpResponse(f"github error {error.code}", status_code=502)
    summary = {
        "source": report.source,
        "reference": report.reference,
        "status": report.status.value,
        "actions": [{"kind": a.kind, "key": a.key, "issue": a.number} for a in actions],
    }
    log.info("notifier %s", json.dumps(summary))
    return func.HttpResponse(json.dumps(summary), mimetype="application/json")


@app.route(route="job-outcome", methods=["POST"])
def job_outcome(req: func.HttpRequest) -> func.HttpResponse:
    try:
        report = Report.from_json(req.get_body())
    except (ValueError, KeyError, TypeError):
        return func.HttpResponse("not a job outcome report", status_code=400)
    return _apply(report)


@app.route(route="alert", methods=["POST"])
def alert(req: func.HttpRequest) -> func.HttpResponse:
    try:
        report = report_from_alert(req.get_body())
    except (ValueError, KeyError, TypeError):
        return func.HttpResponse("not a common alert schema payload", status_code=400)
    return _apply(report)
