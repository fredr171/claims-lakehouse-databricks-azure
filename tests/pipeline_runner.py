"""Run a Lakeflow pipeline source file (src/pipelines/*.py) on local Spark, for tests.

Local PySpark has no `pyspark.pipelines`, so a stand-in module records what the file
defines: materialized views, temporary views, streaming tables and AUTO CDC flows, with
their expectations. run_pipeline() then materializes every dataset once its inputs exist,
each as a temp view, the way a pipeline update resolves its graph:
- spark.readStream.table reads the same table as a batch (one update over everything);
- an AUTO CDC flow is computed by tests/auto_cdc_model.py;
- fully qualified names (catalog.schema.table) map to temp views named with "__".

Expectations are recorded, not enforced: the tests compare against the local build, which
enforces them (claims_lakehouse.silver.quality.evaluate).
"""

from __future__ import annotations

import runpy
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path

import pyspark
from auto_cdc_model import auto_cdc_scd2
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from claims_lakehouse.silver.member_history import SEQUENCE_COLUMNS

PIPELINES_DIR = Path(__file__).resolve().parents[1] / "src" / "pipelines"


def view_name(table: str) -> str:
    return table.replace(".", "__")


@dataclass
class Dataset:
    name: str
    kind: str  # materialized_view | temporary_view | table
    function: object
    expectations: dict[str, dict[str, str]] = field(default_factory=dict)  # action -> rules


@dataclass
class Definitions:
    datasets: list[Dataset] = field(default_factory=list)
    streaming_tables: list[str] = field(default_factory=list)
    flows: list[dict] = field(default_factory=list)


def _stand_in(defs: Definitions) -> types.ModuleType:
    dp = types.ModuleType("pyspark.pipelines")

    def expect(action):
        def factory(rules: dict[str, str]):
            def decorate(function):
                function.__dict__.setdefault("_expectations", {})[action] = dict(rules)
                return function

            return decorate

        return factory

    def dataset(kind):
        def factory(name=None, comment=None, **_):
            def decorate(function):
                defs.datasets.append(
                    Dataset(name or function.__name__, kind, function,
                            function.__dict__.get("_expectations", {}))
                )  # fmt: skip
                return function

            return decorate

        return factory

    dp.materialized_view = dataset("materialized_view")
    dp.temporary_view = dataset("temporary_view")
    dp.table = dataset("table")
    dp.expect_all = expect("warn")
    dp.expect_all_or_drop = expect("drop")
    dp.expect_all_or_fail = expect("fail")
    dp.create_streaming_table = lambda name, **_: defs.streaming_tables.append(name)
    dp.create_auto_cdc_flow = lambda **kwargs: defs.flows.append(kwargs)
    return dp


class _Reader:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def table(self, name: str) -> DataFrame:
        return self._spark.read.table(view_name(name))


class _Spark:
    """What a pipeline file sees as `spark`."""

    def __init__(self, spark: SparkSession, conf: dict[str, str]):
        self._spark = spark
        self.conf = types.SimpleNamespace(get=lambda key, default=None: conf.get(key, default))
        self.read = _Reader(spark)
        self.readStream = _Reader(spark)

    def sql(self, query: str) -> DataFrame:
        return self._spark.sql(query)


def load_pipeline(spark: SparkSession, file_name: str, conf: dict[str, str]) -> Definitions:
    defs = Definitions()
    previous = sys.modules.get("pyspark.pipelines")
    sys.modules["pyspark.pipelines"] = pyspark.pipelines = _stand_in(defs)
    try:
        runpy.run_path(str(PIPELINES_DIR / file_name), init_globals={"spark": _Spark(spark, conf)})
    finally:
        if previous is None:
            sys.modules.pop("pyspark.pipelines", None)
            del pyspark.pipelines
        else:
            sys.modules["pyspark.pipelines"] = pyspark.pipelines = previous
    return defs


def _auto_cdc(spark: SparkSession, flow: dict) -> DataFrame:
    source = spark.read.table(flow["source"])
    sequence = list(SEQUENCE_COLUMNS)  # the pipeline sequences by these (test_pipelines checks)
    rows = [r.asDict() for r in source.collect()]
    [key] = flow["keys"]
    versions = auto_cdc_scd2(
        rows, key, flow["track_history_column_list"], sequence, flow["except_column_list"]
    )
    stored = [f for f in source.schema.fields if f.name not in flow["except_column_list"]]
    at = ", ".join(f"{f.name}: {f.dataType.simpleString()}" for f in source.schema.fields
                   if f.name in sequence)  # fmt: skip
    schema = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in stored)
    return spark.createDataFrame(
        versions, f"{schema}, __START_AT struct<{at}>, __END_AT struct<{at}>"
    )


def run_pipeline(
    spark: SparkSession, defs: Definitions, skip: frozenset[str] = frozenset()
) -> dict[str, DataFrame]:
    """Materialize every dataset (except skip) whose inputs exist. Returns name -> DataFrame
    for everything built; datasets whose inputs never appear are left out."""
    built: dict[str, DataFrame] = {}
    pending = [d for d in defs.datasets if d.name not in skip]
    flows = list(defs.flows)
    progress = True
    while progress and (pending or flows):
        progress = False
        for dataset in list(pending):
            try:
                df = dataset.function()
                _ = df.schema  # resolve every table it reads now
            except AnalysisException:
                continue
            built[dataset.name] = df.localCheckpoint(eager=True)
            built[dataset.name].createOrReplaceTempView(dataset.name)
            pending.remove(dataset)
            progress = True
        for flow in list(flows):
            if flow["source"] not in built:
                continue
            target = _auto_cdc(spark, flow).localCheckpoint(eager=True)
            target.createOrReplaceTempView(flow["target"])
            built[flow["target"]] = target
            flows.remove(flow)
            progress = True
    return built
