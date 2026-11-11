"""Run the conformed views and the gold marts on a Spark session from DataFrames.

This is how the local tests build gold; on Databricks the conformed views are part of each
payer's pipeline and the marts read Unity Catalog tables, from the same SQL files.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame, SparkSession

from claims_lakehouse.config import LakehouseConfig
from claims_lakehouse.gold.contract import KINDS, check_contract
from claims_lakehouse.gold.marts import (
    MARTS,
    placeholder_values,
    read_sql,
    render,
    rendered_marts,
)


def build_conformed(
    spark: SparkSession, feed: str, payer_id: str, silver: Mapping[str, DataFrame]
) -> dict[str, DataFrame]:
    """A payer's conformed views from its silver tables (table name -> DataFrame).

    The SQL names silver tables directly, as it will inside the payer's pipeline. Each
    view is analyzed as soon as it is created, so the temporary views it read can be
    dropped and reused by the next payer.
    """
    for name, df in silver.items():
        df.createOrReplaceTempView(name)
    try:
        return {
            kind: spark.sql(
                render(
                    read_sql(f"claims_lakehouse.silver.{feed}", f"conformed_{kind}"),
                    {"payer_id": payer_id},
                )
            )
            for kind in KINDS
        }
    finally:
        for name in silver:
            spark.catalog.dropTempView(name)


def build_gold(
    spark: SparkSession,
    config: LakehouseConfig,
    conformed: Mapping[str, Mapping[str, DataFrame]],
) -> dict[str, DataFrame]:
    """conformed: feed -> its conformed views (feeds without data yet are absent).

    Returns every mart, plus check_marts_reconcile (rows = mismatches). Raises
    ContractError if a registered payer's view breaks the conformed contract.
    """
    registered = [p for p in config.payers if p.feed in conformed]
    for payer in registered:
        for kind in KINDS:
            df = conformed[payer.feed][kind]
            check_contract(df, kind, payer.payer_id)
            df.createOrReplaceTempView(f"conformed_{kind}__{payer.payer_id}")

    values = placeholder_values(
        config.payers,
        conformed=lambda p, kind: f"conformed_{kind}__{p.payer_id}" if p in registered else None,
        mart=lambda name: f"gold_{name}",
    )
    result = {}
    for name, sql in rendered_marts(values).items():
        result[name] = spark.sql(sql)
        if name in MARTS:
            # Marts are tables on Databricks: cache each one so later marts read it instead
            # of recomputing its whole lineage.
            result[name] = result[name].cache()
            result[name].createOrReplaceTempView(f"gold_{name}")
    return result
