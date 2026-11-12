"""Shared fixtures: a local Spark session and one 36-month generated dataset per run."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest
from harness import FIRST_MONTH, LAST_MONTH, LocalArchive, months
from synthetic_payer_files import generate_cclf, generate_ma_payer

from claims_lakehouse.config import load_config


@pytest.fixture(scope="session")
def config():
    return load_config()


@pytest.fixture(scope="session")
def spark():
    """Local Spark, in memory only (nothing is written to disk).

    driver.host: a hostname with an underscore is not a valid Spark URL, so bind to
    localhost. PYSPARK_PYTHON: workers must run this interpreter, not whatever `python`
    is first on PATH.
    """
    if not os.environ.get("JAVA_HOME") and shutil.which("java") is None:
        pytest.fail("local Spark needs Java 17: set JAVA_HOME or put java on PATH")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("claims-lakehouse-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def generated(tmp_path_factory) -> Path:
    """All 36 months from both generators (seed 42), plus answer keys under key/.

    Layout: cms/YYYY-MM/CCLF*.csv, ma_payer/YYYY-MM/MAP_*.csv, key/, key/ma_payer/.
    """
    root = tmp_path_factory.mktemp("generated")
    month_args = ["--seed", "42", "--start", FIRST_MONTH, "--end", LAST_MONTH]
    generate_cclf.main(
        [*month_args, "--output-dir", str(root / "cms"), "--answer-key-dir", str(root / "key")]
    )
    generate_ma_payer.main(
        [
            *month_args,
            "--output-dir", str(root / "ma_payer"),
            "--answer-key-dir", str(root / "key" / "ma_payer"),
            "--crossover-dir", str(root / "key"),
        ]
    )  # fmt: skip
    return root


@pytest.fixture(scope="session")
def full_load(config, generated, tmp_path_factory):
    """Every generated month delivered in order: (LocalArchive, validation results)."""
    archive = LocalArchive(tmp_path_factory.mktemp("full"), config)
    results = []
    for month in months():
        results.extend(archive.deliver_month(generated, month))
    return archive, results
