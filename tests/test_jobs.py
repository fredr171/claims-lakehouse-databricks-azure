"""The job's Python tasks: the full-refresh decision and the gold statements.

The supersession count is also checked against what the pipeline publishes, in
test_pipelines.py (the two must agree, or the job would refresh on every run or never).
"""

from __future__ import annotations

import re

import pytest

from claims_lakehouse.config import load_config
from claims_lakehouse.gold.marts import CHECKS, MARTS
from claims_lakehouse.jobs.gold_tables import gold_statements, published_feeds
from claims_lakehouse.jobs.supersession import needs_full_refresh


@pytest.mark.parametrize(
    ("in_bronze", "published", "expected"),
    [
        (0, None, False),  # first run, nothing superseded
        (1, None, True),  # first run after a re-send: build from current deliveries
        (0, 0, False),
        (1, 1, False),  # the re-send is already reflected
        (2, 1, True),  # a new re-send since the last good update
    ],
)
def test_full_refresh_only_when_a_new_supersession_arrived(in_bronze, published, expected):
    assert needs_full_refresh(in_bronze, published) is expected


@pytest.fixture(scope="module")
def config():
    return load_config()


def test_only_feeds_with_every_conformed_view_are_published(config):
    existing = {
        "cat.silver_cclf.conformed_coverage",
        "cat.silver_cclf.conformed_claims",
        "cat.silver_cclf.conformed_deliveries",
        "cat.silver_ma_payer.conformed_coverage",  # ma_payer only part-way through
    }
    assert published_feeds(config, "cat", existing.__contains__) == {"cclf"}


def test_gold_statements_publish_every_mart_into_the_gold_schema(config):
    statements = gold_statements(config, "cat", {"cclf", "ma_payer"})
    assert list(statements) == [*MARTS, *CHECKS]
    for name, sql in statements.items():
        kind = "TABLE" if name in MARTS else "VIEW"
        assert sql.startswith(f"CREATE OR REPLACE {kind} cat.gold.{name} AS\n")
        assert "${" not in sql, name
    for name in MARTS + CHECKS:
        for table in re.findall(r"\bcat\.(\w+)\.(\w+)", statements[name]):
            assert table[0] in {"gold", "silver_cclf", "silver_ma_payer"}, (name, table)


def test_an_unpublished_feed_is_left_out_of_gold(config):
    statements = gold_statements(config, "cat", {"cclf"})
    text = "\n".join(statements.values())
    assert "cat.silver_cclf.conformed_claims" in text
    assert "silver_ma_payer" not in text
