"""Job task: publish the gold marts from every payer's conformed views.

Runs even when a payer's pipeline stopped (run_if ALL_DONE): that payer's conformed views
still hold its last good update, so gold refreshes the other payer and keeps this one as it
was. Each conformed view is checked against the contract before any mart is written.

    publish_gold.py --src <synced src folder> --catalog <catalog>
"""

import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--src", required=True)
parser.add_argument("--catalog", required=True)
args = parser.parse_args()
sys.path.insert(0, args.src)

from databricks.sdk.runtime import spark  # noqa: E402

from claims_lakehouse.config import load_config  # noqa: E402
from claims_lakehouse.gold.contract import KINDS, check_contract  # noqa: E402
from claims_lakehouse.jobs.gold_tables import (  # noqa: E402
    conformed_table,
    gold_statements,
    published_feeds,
)

config = load_config()
feeds = published_feeds(config, args.catalog, spark.catalog.tableExists)
for payer in config.payers:
    if payer.feed in feeds:
        for kind in KINDS:
            check_contract(
                spark.read.table(conformed_table(args.catalog, payer.feed, kind)),
                kind,
                payer.payer_id,
            )
print(f"publishing gold from feeds: {sorted(feeds)}")
for name, statement in gold_statements(config, args.catalog, feeds).items():
    spark.sql(statement)
    print(f"published {name}")
