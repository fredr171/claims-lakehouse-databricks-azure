"""A local stand-in for one job run: each payer's pipeline update, then gold.

    lakehouse = LocalLakehouse(spark, config, archive)
    outcomes = lakehouse.update()        # payer_id -> PayerOutcome
    lakehouse.published["humana_ma"]     # that payer's last published silver and conformed views
    lakehouse.gold                       # the last published marts

What it models (what Lakeflow and the job give on Databricks):
- Each payer updates on its own. Its silver tables are rebuilt from bronze and every
  expectation on them is evaluated. Any FAIL stops the payer: nothing of it is published,
  so its last good silver stays (a Lakeflow update that fails publishes nothing
  downstream of the failure; phase 3 makes every published table depend on the payer's
  gate tables so that "downstream" is the whole payer).
- Gold always refreshes, from each payer's last published conformed views, so a stop in
  one payer still refreshes the other.
- Published tables are checkpointed when published: later deliveries cannot change them,
  and gold reads them without recomputing their lineage.

Only the tables the tests compare are published (member history, claim families, conformed
views and the marts); Lakeflow publishes every table.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from harness import LocalArchive
from pyspark.sql import DataFrame, SparkSession

from claims_lakehouse.config import LakehouseConfig
from claims_lakehouse.gold.build import build_conformed, build_gold
from claims_lakehouse.silver.cclf import tables as cclf_tables
from claims_lakehouse.silver.humana import tables as humana_tables
from claims_lakehouse.silver.quality import Action, evaluate

BUILDERS = {"cclf": cclf_tables, "humana": humana_tables}
SILVER_OUTPUTS = {
    "cclf": ("beneficiary_versions", "claim_families"),
    "humana": ("member_versions", "claim_families"),
}


@dataclass(frozen=True)
class Problem:
    """A failed expectation: identifiers and counts only (what an issue may carry)."""

    table: str
    expectation: str
    action: Action
    failed_rows: int


@dataclass(frozen=True)
class PayerOutcome:
    payer_id: str
    status: str  # passed | warned | stopped | no_data
    problems: tuple[Problem, ...] = ()

    @property
    def stopping(self) -> set[str]:
        return {p.expectation for p in self.problems if p.action is Action.FAIL}


@dataclass
class LocalLakehouse:
    spark: SparkSession
    config: LakehouseConfig
    archive: LocalArchive
    published: dict[str, dict[str, DataFrame]] = field(default_factory=dict)
    gold: dict[str, DataFrame] = field(default_factory=dict)

    def update(self) -> dict[str, PayerOutcome]:
        outcomes = {payer.payer_id: self._update_payer(payer) for payer in self.config.payers}
        conformed = {
            payer.feed: {
                name.removeprefix("conformed_"): df
                for name, df in tables.items()
                if name.startswith("conformed_")
            }
            for payer in self.config.payers
            if (tables := self.published.get(payer.payer_id))
        }
        if conformed:
            marts = build_gold(self.spark, self.config, conformed)
            self.gold = {name: _publish(df) for name, df in marts.items()}
        return outcomes

    def _update_payer(self, payer) -> PayerOutcome:
        builder = BUILDERS[payer.feed]
        if not any(f.file_type in builder.FILE_TYPES for f in self.archive.files):
            return PayerOutcome(payer.payer_id, "no_data")
        bronze = {ft: self.archive.bronze(self.spark, ft).cache() for ft in builder.FILE_TYPES}
        cached = list(bronze.values())
        try:
            silver = builder.build(bronze)
            # Every expectation table and output reads these: compute them once per update.
            for name in [*silver]:
                if name.endswith("_current") or name.startswith("stg_"):
                    silver[name] = silver[name].cache()
                    cached.append(silver[name])
            problems = tuple(
                Problem(table, r.name, r.action, r.failed_rows)
                for table, expectations in builder.EXPECTATIONS.items()
                for r in evaluate(silver[table], expectations)[1]
                if not r.passed
            )
            if any(p.action is Action.FAIL for p in problems):
                return PayerOutcome(payer.payer_id, "stopped", problems)
            views = build_conformed(self.spark, payer.feed, payer.payer_id, silver)
            self.published[payer.payer_id] = {
                **{name: _publish(silver[name]) for name in SILVER_OUTPUTS[payer.feed]},
                **{f"conformed_{kind}": _publish(df) for kind, df in views.items()},
            }
            return PayerOutcome(payer.payer_id, "warned" if problems else "passed", problems)
        finally:
            for df in cached:
                df.unpersist()


def _publish(df: DataFrame) -> DataFrame:
    return df.localCheckpoint(eager=True)


@dataclass(frozen=True)
class Snapshot:
    """Every row of every table, sorted, without lineage columns (archive times differ
    between loads). Compare snapshots with ==."""

    tables: dict[str, tuple[tuple[str, ...], list[tuple]]]

    def payer(self, payer_id: str) -> Snapshot:
        """Only that payer's rows, of the tables that have a payer_id (dim_member, a
        cross-payer table, has no single payer's slice)."""
        sliced = {}
        for name, (columns, rows) in self.tables.items():
            if "payer_id" in columns:
                i = columns.index("payer_id")
                sliced[name] = (columns, [r for r in rows if r[i] == payer_id])
        return Snapshot(sliced)


def snapshot(tables: dict[str, DataFrame]) -> Snapshot:
    """One collect per table."""
    result = {}
    for name, df in tables.items():
        columns = tuple(c for c in df.columns if not c.startswith("_"))
        rows = sorted((tuple(r) for r in df.select(*columns).collect()), key=repr)
        result[name] = (columns, rows)
    return Snapshot(result)
