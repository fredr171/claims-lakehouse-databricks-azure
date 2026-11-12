"""Job task (the last one): compare gold with the answer keys and fail the run on mismatch.

The keys are in the ops.answer_keys volume. Until every month has been delivered, the task
reports how many months are missing and passes. Results are also set as task values
(counts and check names only) for the notifier.

    check_answer_keys.py --src <synced src folder> --catalog <catalog>
"""

import argparse
import json
import sys
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True)
parser.add_argument("--catalog", required=True)
args = parser.parse_args()
sys.path.insert(0, args.src)

from databricks.sdk.runtime import dbutils, spark  # noqa: E402

from claims_lakehouse.checks.answer_keys import check_gold, missing_months  # noqa: E402
from claims_lakehouse.gold.marts import CHECKS, MARTS  # noqa: E402
from claims_lakehouse.jobs.gold_tables import gold_table  # noqa: E402

keys = Path(f"/Volumes/{args.catalog}/ops/answer_keys")
marts = {name: spark.read.table(gold_table(args.catalog, name)) for name in MARTS + CHECKS}

# No sys.exit() for the early pass: serverless runs the script in IPython, which reports any
# SystemExit, even exit code 0, as a failed task.
if missing := missing_months(marts, keys):
    print(f"not every month delivered yet, months missing: {missing}; nothing compared")
    dbutils.jobs.taskValues.set("answer_keys", json.dumps({"status": "incomplete", **missing}))
else:
    problems = check_gold(marts, keys)
    dbutils.jobs.taskValues.set(
        "answer_keys",
        json.dumps({"status": "mismatch" if problems else "match",
                    "mismatches": [vars(p) for p in problems]}),
    )  # fmt: skip
    if problems:
        raise AssertionError("gold differs from the answer keys:\n" + "\n".join(map(str, problems)))
    print("gold matches the answer keys")
