"""The gold task: the mart SQL, rendered against Unity Catalog tables.

The same SQL files the local tests run (claims_lakehouse.gold.build); only the names differ.
Each payer's conformed views live in its pipeline's schema, <catalog>.silver_<feed>. A
registered payer whose pipeline has not published yet is left out (its marts are empty), as
locally. Marts are tables in <catalog>.gold; the reconciliation check is a view next to them.
"""

from __future__ import annotations

from collections.abc import Callable

from claims_lakehouse.config import LakehouseConfig
from claims_lakehouse.gold.contract import KINDS
from claims_lakehouse.gold.marts import MARTS, placeholder_values, rendered_marts


def conformed_table(catalog: str, feed: str, kind: str) -> str:
    return f"{catalog}.silver_{feed}.conformed_{kind}"


def gold_table(catalog: str, name: str) -> str:
    return f"{catalog}.gold.{name}"


def published_feeds(
    config: LakehouseConfig, catalog: str, table_exists: Callable[[str], bool]
) -> set[str]:
    """Registered feeds whose pipeline has published every conformed view."""
    return {
        p.feed
        for p in config.payers
        if all(table_exists(conformed_table(catalog, p.feed, kind)) for kind in KINDS)
    }


def gold_statements(config: LakehouseConfig, catalog: str, feeds: set[str]) -> dict[str, str]:
    """Mart or check name -> the statement that publishes it, in build order."""
    values = placeholder_values(
        config.payers,
        conformed=lambda p, kind: (
            conformed_table(catalog, p.feed, kind) if p.feed in feeds else None
        ),
        mart=lambda name: gold_table(catalog, name),
    )
    return {
        name: f"CREATE OR REPLACE {'TABLE' if name in MARTS else 'VIEW'} "
        f"{gold_table(catalog, name)} AS\n{sql}"
        for name, sql in rendered_marts(values).items()
    }
