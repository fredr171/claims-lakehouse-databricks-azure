"""Job task (always last, run_if ALL_DONE): report the run's outcome to the notifier.

Collects this run's task states, each payer's pipeline update (expectation failures from
its event log, and the expectations that stopped it) and the answer-key task value, turns
them into a report (claims_lakehouse.jobs.outcome) and posts it to the notifier Function.
The URL, with its function key, is a secret in the notifier secret scope.

The task then fails if the run stopped or errored. It is the run's last task, so without
that a stopped payer could still leave the run marked as succeeded.

    report_outcome.py --src <synced src folder> --run-id {{job.run_id}}
                      --pipeline cclf=<pipeline id> --pipeline humana=<pipeline id>
                      --secret-scope <scope>
"""

import argparse
import json
import sys
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True)
parser.add_argument("--run-id", required=True, type=int)
parser.add_argument("--pipeline", action="append", default=[], help="feed=pipeline id")
parser.add_argument("--secret-scope", required=True)
args = parser.parse_args()
sys.path.insert(0, args.src)

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.runtime import dbutils, spark  # noqa: E402

from claims_lakehouse.config import load_config  # noqa: E402
from claims_lakehouse.jobs.outcome import (  # noqa: E402
    PipelineResult,
    classify_run,
    latest_states,
    violated_expectations,
)
from claims_lakehouse.notifier.report import Status  # noqa: E402

THIS_TASK = "report_outcome"
EXPECTATIONS = (
    "array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>"
)

config = load_config()
pipelines = dict(p.split("=", 1) for p in args.pipeline)
run = WorkspaceClient().jobs.get_run(args.run_id)

states = latest_states(
    (
        t.task_key,
        t.start_time,
        t.state.result_state.value if t.state and t.state.result_state else None,
    )
    for t in run.tasks or []
    if t.task_key != THIS_TASK
)


def pipeline_result(pipeline_id: str) -> PipelineResult:
    """The pipeline's latest update since this run started."""
    log = f"event_log('{pipeline_id}')"
    update = spark.sql(
        f"SELECT origin.update_id AS update_id FROM {log} "
        f"WHERE origin.update_id IS NOT NULL AND timestamp >= timestamp_millis({run.start_time}) "
        "ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    if not update:
        return PipelineResult()
    in_update = f"origin.update_id = '{update[0]['update_id']}'"
    failures = spark.sql(
        "SELECT e.name AS name, CAST(SUM(e.failed_records) AS BIGINT) AS failed FROM ("
        f"  SELECT explode(from_json(details:flow_progress.data_quality.expectations, "
        f"  '{EXPECTATIONS}')) AS e FROM {log} "
        f"  WHERE event_type = 'flow_progress' AND {in_update}"
        ") GROUP BY e.name"
    ).collect()
    errors = spark.sql(
        f"SELECT concat_ws(' ', message, to_json(error)) AS text FROM {log} "
        f"WHERE level = 'ERROR' AND {in_update}"
    ).collect()
    return PipelineResult(
        expectation_failures={r["name"]: int(r["failed"] or 0) for r in failures},
        stopped_by=violated_expectations(r["text"] for r in errors),
    )


answer_keys = dbutils.jobs.taskValues.get(
    taskKey="answer_keys", key="answer_keys", default=None, debugValue=None
)
report = classify_run(
    args.run_id,
    states,
    feeds=[p.feed for p in config.payers],
    pipelines={feed: pipeline_result(pid) for feed, pid in pipelines.items()},
    answer_keys=json.loads(answer_keys) if answer_keys else None,
    payer_feeds={p.payer_id: p.feed for p in config.payers},
)

request = urllib.request.Request(
    dbutils.secrets.get(args.secret_scope, "job-outcome-url"),
    data=report.to_json().encode(),
    method="POST",
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(request, timeout=60) as response:
    print(f"notifier: {response.status} {response.read().decode()[:500]}")

print(f"run {args.run_id}: {report.status.value}")
for problem in report.problems:
    print(f"  [{problem.status.value}] {problem.path}: {problem.detail}")
if report.status in (Status.STOPPED, Status.ERROR):
    raise RuntimeError(f"run {args.run_id} {report.status.value}; see the notifier's issues")
