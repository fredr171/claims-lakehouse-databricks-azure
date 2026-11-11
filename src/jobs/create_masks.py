"""Job task (the first one): create or update the column-mask functions.

Every pipeline table that holds members' names, addresses or birth dates names these
functions in its schema, so they must exist before the pipelines run. See
claims_lakehouse.silver.masks.

    create_masks.py --src <synced src folder> --catalog <catalog> --group <PII readers group>
"""

import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True)
parser.add_argument("--catalog", required=True)
parser.add_argument("--group", required=True)
args = parser.parse_args()
sys.path.insert(0, args.src)

from databricks.sdk.runtime import spark  # noqa: E402

from claims_lakehouse.silver.masks import mask_function_statements  # noqa: E402

for statement in mask_function_statements(args.catalog, args.group):
    spark.sql(statement)
    print(statement.splitlines()[0])
