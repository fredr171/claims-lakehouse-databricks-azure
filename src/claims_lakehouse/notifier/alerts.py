"""Azure Monitor alerts (common alert schema) as notifier reports.

A fired alert is a problem; its resolution closes that alert's issue. A log alert split by a
`feed` dimension gives one alert per feed, so the issue is keyed by the feed it is about.
Only the rule name, the dimension and the measured count are used: the alert ID and target
resource IDs (which contain the subscription) never reach an issue.
"""

from __future__ import annotations

import json
from typing import Any

from claims_lakehouse.notifier.report import ALL_FEEDS, Problem, Report, Status

SCHEMA = "azureMonitorCommonAlertSchema"
# Sev0-Sev2 are errors; Sev3 and Sev4 are warnings.
_WARNING_SEVERITIES = {"Sev3", "Sev4"}


def _condition(context: dict[str, Any]) -> dict[str, Any]:
    all_of = (context.get("condition") or {}).get("allOf") or [{}]
    return all_of[0]


def _feed(condition: dict[str, Any]) -> str:
    for dimension in condition.get("dimensions") or []:
        if dimension.get("name") == "feed" and dimension.get("value"):
            return str(dimension["value"])
    return ALL_FEEDS


def _count(condition: dict[str, Any]) -> str:
    value = condition.get("metricValue")
    if isinstance(value, int | float) and not isinstance(value, bool):
        return f"{value:g}"
    return "unknown"


def report_from_alert(payload: str | bytes | dict[str, Any]) -> Report:
    data = json.loads(payload) if isinstance(payload, str | bytes) else payload
    if data.get("schemaId") != SCHEMA:
        raise ValueError("not a common alert schema payload")
    essentials = data["data"]["essentials"]
    condition = _condition(data["data"].get("alertContext") or {})
    rule = str(essentials["alertRule"])
    feed = _feed(condition)
    check = f"alert/{rule}"
    fired = essentials.get("firedDateTime", "")
    reference = f"alert {rule} ({fired})" if fired else f"alert {rule}"

    if essentials.get("monitorCondition") == "Resolved":
        return Report("alert", reference, problems=(), scopes=(f"{feed}/{check}",))
    status = Status.WARNED if essentials.get("severity") in _WARNING_SEVERITIES else Status.ERROR
    detail = f"{_count(condition)} in the alert window"
    return Report("alert", reference, problems=(Problem(feed, check, status, detail),), scopes=())
