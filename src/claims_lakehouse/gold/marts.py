"""Gold marts: SQL files rendered against the payer registry.

The marts are SQL (gold/sql/<mart>.sql). They name what they read with placeholders:
    ${coverage} ${claims} ${deliveries}   the union of every registered payer's conformed
                                          views that exist (a payer registered before its
                                          first delivery is left out, not an error)
    ${payer_registry}                     config/payers.yml as an inline table
    ${dim_payer} ${fct_claims} ...        marts built earlier in MARTS
render() fills them in from a `relation` function, so the same SQL runs here (local temp
views) and on Databricks (Unity Catalog table names) unchanged. The registry is the only
place gold learns about payers: retiring one is a retired_month, offboarding one is
removing its entry, and no mart's SQL changes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from importlib.resources import files
from string import Template

from claims_lakehouse.config import Payer
from claims_lakehouse.gold.contract import CONTRACT, KINDS, empty_relation

MARTS = ("dim_payer", "dim_member", "fct_claims", "fct_member_months", "agg_payer_monthly")
CHECKS = ("check_marts_reconcile",)
REGISTRY_COLUMNS = (
    ("payer_id", "string"),
    ("payer_name", "string"),
    ("line_of_business", "string"),
    ("feed", "string"),
    ("onboarded_month", "date"),
    ("retired_month", "date"),
)


def read_sql(package: str, name: str) -> str:
    return files(package).joinpath("sql", f"{name}.sql").read_text(encoding="utf-8")


def render(sql: str, values: dict[str, str]) -> str:
    """Fill ${name} placeholders; a missing one is an error, not left in the SQL."""
    return Template(sql).substitute(values)


def _literal(value: str | None, dtype: str) -> str:
    if value is None:
        return f"CAST(NULL AS {dtype})"
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"DATE'{escaped}-01'" if dtype == "date" else f"'{escaped}'"


def registry_relation(payers: Iterable[Payer]) -> str:
    """The registry as an inline table, one row per payer, typed by REGISTRY_COLUMNS."""
    rows = [
        "(" + ", ".join(_literal(getattr(p, name), dtype) for name, dtype in REGISTRY_COLUMNS) + ")"
        for p in payers
    ]
    names = ", ".join(name for name, _ in REGISTRY_COLUMNS)
    typed = ", ".join(f"CAST({name} AS {dtype}) AS {name}" for name, dtype in REGISTRY_COLUMNS)
    return f"(SELECT {typed} FROM VALUES {', '.join(rows)} AS registry({names}))"


def union_relation(kind: str, relations: list[str]) -> str:
    """UNION ALL of payers' conformed views, with the contract's columns named explicitly."""
    if not relations:
        return empty_relation(kind)
    columns = ", ".join(name for name, _ in CONTRACT[kind])
    return "(" + " UNION ALL ".join(f"SELECT {columns} FROM {r}" for r in relations) + ")"


def placeholder_values(
    payers: Iterable[Payer],
    conformed: Callable[[Payer, str], str | None],
    mart: Callable[[str], str],
) -> dict[str, str]:
    """conformed(payer, kind): the payer's conformed view, or None if not built yet.
    mart(name): where a mart is (or will be) written."""
    payers = list(payers)
    values = {"payer_registry": registry_relation(payers)}
    for kind in KINDS:
        built = [r for p in payers if (r := conformed(p, kind)) is not None]
        values[kind] = union_relation(kind, built)
    values |= {name: mart(name) for name in MARTS}
    return values


def rendered_marts(values: dict[str, str]) -> dict[str, str]:
    """Mart or check name -> its SQL, ready to run, in build order."""
    return {
        name: render(read_sql("claims_lakehouse.gold", name), values) for name in MARTS + CHECKS
    }
