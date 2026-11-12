"""A payer's silver tables as data: what each table reads, how it is built, what it checks.

Each payer's tables module (silver/cclf/tables.py, silver/humana/tables.py) lists its
tables once, in build order. Two runtimes read that one list, so the wiring cannot drift
between them:
- locally, build_tables() calls each table's function on the DataFrames it reads;
- on Databricks, the payer's pipeline defines one table per entry, attaching its
  expectations, and reads its inputs from the pipeline (pipelines/silver.py).

An input is another silver table's name, or bronze(file_type) for that file type's bronze
table. The pipeline replaces the table marked as member history with AUTO CDC (see
MemberHistory); locally it is the batch member_versions().
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame

from claims_lakehouse.silver.quality import Action, Expectation

_BRONZE = "bronze:"


def bronze(file_type: str) -> str:
    """The input name of a file type's bronze table."""
    return f"{_BRONZE}{file_type}"


def bronze_file_type(input_name: str) -> str | None:
    """The file type if input_name names a bronze table, else None."""
    return input_name.removeprefix(_BRONZE) if input_name.startswith(_BRONZE) else None


@dataclass(frozen=True)
class SilverTable:
    name: str
    inputs: tuple[str, ...]
    build: Callable[..., DataFrame]  # called with the input DataFrames, in order
    expectations: tuple[Expectation, ...] = ()
    published: bool = False  # read by gold or the answer-key check

    @property
    def gates(self) -> bool:
        """Carries a FAIL expectation: must pass before anything of the payer is published."""
        return any(e.action is Action.FAIL for e in self.expectations)


@dataclass(frozen=True)
class MemberHistory:
    """The member-history (SCD type 2) table: batch locally, AUTO CDC in the pipeline."""

    table: str  # e.g. beneficiary_versions
    source: str  # the staged table it is built from
    file_type: str  # the delivered file it comes from
    key: str
    tracked: tuple[str, ...]
    coverage_end: str


def check_tables(tables: Sequence[SilverTable], history: MemberHistory) -> None:
    """Names unique, every input defined earlier (or bronze), history table listed."""
    seen: set[str] = set()
    for table in tables:
        if table.name in seen:
            raise ValueError(f"silver table {table.name!r} is listed twice")
        for name in table.inputs:
            if bronze_file_type(name) is None and name not in seen:
                raise ValueError(f"{table.name!r} reads {name!r}, which is not built before it")
        seen.add(table.name)
    if history.table not in seen or history.source not in seen:
        raise ValueError(f"member history {history.table!r} or its source is not listed")


def build_tables(
    tables: Sequence[SilverTable], bronze_tables: Mapping[str, DataFrame]
) -> dict[str, DataFrame]:
    """bronze_tables: file type -> its bronze table. Returns table name -> DataFrame."""
    built: dict[str, DataFrame] = {}
    for table in tables:
        inputs = [
            bronze_tables[ft] if (ft := bronze_file_type(name)) is not None else built[name]
            for name in table.inputs
        ]
        built[table.name] = table.build(*inputs)
    return built


def expectations_by_table(tables: Sequence[SilverTable]) -> dict[str, list[Expectation]]:
    """Table -> its expectations, for tables that carry any."""
    return {t.name: list(t.expectations) for t in tables if t.expectations}
