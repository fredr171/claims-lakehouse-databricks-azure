"""Job task: decide whether a payer's pipeline runs as a full refresh.

Sets the task value full_refresh ("true" / "false"), which a condition task reads.
See claims_lakehouse.jobs.supersession.

    prepare_payer.py --src <synced src folder> --catalog <catalog> --feed cclf|ma_payer
"""

import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True)
parser.add_argument("--catalog", required=True)
parser.add_argument("--feed", required=True)
args = parser.parse_args()
sys.path.insert(0, args.src)

from databricks.sdk.runtime import dbutils, spark  # noqa: E402

from claims_lakehouse.bronze.lineage import bronze_table_name  # noqa: E402
from claims_lakehouse.config import load_config  # noqa: E402
from claims_lakehouse.jobs.supersession import (  # noqa: E402
    needs_full_refresh,
    superseded_deliveries,
)
from claims_lakehouse.silver.cclf import tables as cclf_tables  # noqa: E402
from claims_lakehouse.silver.ma_payer import tables as ma_payer_tables  # noqa: E402

config = load_config()
history = {"cclf": cclf_tables, "ma_payer": ma_payer_tables}[args.feed].MEMBER_HISTORY
bronze = f"{args.catalog}.bronze.{bronze_table_name(config.file_types[history.file_type])}"
published_table = f"{args.catalog}.silver_{args.feed}.{history.table}_superseded"

in_bronze = (
    superseded_deliveries(spark.read.table(bronze)) if spark.catalog.tableExists(bronze) else 0
)
published = (
    spark.read.table(published_table).first()["superseded"]
    if spark.catalog.tableExists(published_table)
    else None
)
full_refresh = needs_full_refresh(in_bronze, published)
print(f"{args.feed}: superseded member deliveries in bronze={in_bronze}, "
      f"published={published}, full_refresh={full_refresh}")  # fmt: skip
dbutils.jobs.taskValues.set("full_refresh", str(full_refresh).lower())
