"""Gold against the generator's answer keys: the job's final task, and the local tests.

check_gold() compares the marts with the answer keys of the generated months and returns
every mismatch. A mismatch carries the check's name and counts, never data values: it is
what a failed run reports, and the notifier turns it into a GitHub issue.

The keys directory has the generator's layout (synthetic-payer-files):
    <keys>/                 CMS CCLF answer keys, crossover_members.csv, summary.json
    <keys>/humana/          MA payer answer keys, summary.json
Member months have no answer key, so write_member_month_keys() adds member_months.csv to
each payer's key folder, counted independently from the delivered eligibility files with
the generator's own coverage-span rule. It needs the generator package (a dev dependency);
the job only reads the file.
"""

from __future__ import annotations

import calendar
import csv
import json
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from claims_lakehouse.config import LakehouseConfig

KEY_DIRS = {"cms_ffs": ".", "humana_ma": "humana"}
MEMBER_MONTHS_FILE = "member_months.csv"
SWITCHERS = 17  # payer switchers in the generated data (seed 42)

# payer -> (generated folder, file type, member key, span start, span end, death, MBI)
ELIGIBILITY = {
    "cms_ffs": ("cms", "beneficiary_demographics", "BENE_MBI_ID", "BENE_RNG_BGN_DT",
                "BENE_RNG_END_DT", "BENE_DEATH_DT", "BENE_MBI_ID"),
    "humana_ma": ("humana", "member_eligibility", "MEMBER_ID", "ENROLLMENT_START_DT",
                  "ENROLLMENT_END_DT", "DEATH_DT", "MBI"),
}  # fmt: skip


@dataclass(frozen=True)
class Mismatch:
    check: str
    payer_id: str | None
    detail: str  # counts only

    def __str__(self) -> str:
        who = f" [{self.payer_id}]" if self.payer_id else ""
        return f"{self.check}{who}: {self.detail}"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _key(keys: Path, payer_id: str, name: str) -> Path:
    return keys / KEY_DIRS[payer_id] / name


def _differing(expected: Mapping, actual: Mapping) -> int:
    return sum(1 for k in expected.keys() | actual.keys() if expected.get(k) != actual.get(k))


def missing_months(marts: Mapping[str, DataFrame], keys: Path) -> dict[str, int]:
    """Payer -> how many months of the answer keys' window gold has not received yet.

    The answer keys describe the complete delivery, so the job compares only once every
    month has arrived; while files are still landing, runs report what is missing instead.
    """
    delivered = {
        r.payer_id: r.last_delivery_month
        for r in marts["dim_payer"].select("payer_id", "last_delivery_month").collect()
    }
    missing = {}
    for payer_id in KEY_DIRS:
        months = sorted(r["month"] for r in _read_csv(_key(keys, payer_id, MEMBER_MONTHS_FILE)))
        last = delivered.get(payer_id)
        received = last.strftime("%Y-%m") if last else ""
        if behind := sum(1 for m in months if m > received):
            missing[payer_id] = behind
    return missing


def check_gold(marts: Mapping[str, DataFrame], keys: Path) -> list[Mismatch]:
    """Every mismatch between the marts and the answer keys; empty means gold is right."""
    problems: list[Mismatch] = []
    claims, monthly = marts["fct_claims"], marts["agg_payer_monthly"]

    for payer_id in KEY_DIRS:
        rows = _read_csv(_key(keys, payer_id, "net_paid_by_service_month.csv"))
        expected = {r["service_month"]: Decimal(r["total_net_paid"]) for r in rows}
        actual = {
            r.month: r.net_paid
            for r in claims.where(F.col("payer_id") == payer_id)
            .groupBy(F.date_format("service_month", "yyyy-MM").alias("month"))
            .agg(F.sum("net_paid_amount").alias("net_paid"))
            .collect()
        }
        if differing := _differing(expected, actual):
            problems.append(
                Mismatch(
                    "net_paid_by_service_month",
                    payer_id,
                    f"{differing} of {len(expected)} service months differ",
                )
            )
        [total] = monthly.where(F.col("payer_id") == payer_id).agg(F.sum("net_paid_amount")).first()
        if total != sum(expected.values()):
            problems.append(
                Mismatch(
                    "pmpm_net_paid_total", payer_id, "agg_payer_monthly total differs from the key"
                )
            )

        months_file = _key(keys, payer_id, MEMBER_MONTHS_FILE)
        expected_months = {r["month"]: int(r["members"]) for r in _read_csv(months_file)}
        actual_months = {
            r.month: r["count"]
            for r in marts["fct_member_months"]
            .where(F.col("payer_id") == payer_id)
            .groupBy(F.date_format("month", "yyyy-MM").alias("month"))
            .count()
            .collect()
        }
        if differing := _differing(expected_months, actual_months):
            problems.append(
                Mismatch(
                    "member_months",
                    payer_id,
                    f"{differing} of {len(expected_months)} months differ",
                )
            )

    cms = json.loads(_key(keys, "cms_ffs", "summary.json").read_text(encoding="utf-8"))
    hum = json.loads(_key(keys, "humana_ma", "summary.json").read_text(encoding="utf-8"))
    expected_statuses = {
        "VOIDED": cms["cancellations"] + hum["voids"],
        "ADJUSTED": cms["adjustments"] + hum["replacements"],
        "DENIED": hum["denied"],
    }
    statuses = {
        r.claim_status: r["count"] for r in claims.groupBy("claim_status").count().collect()
    }
    if differing := _differing(expected_statuses, {s: statuses.get(s) for s in expected_statuses}):
        problems.append(Mismatch("claim_statuses", None, f"{differing} status counts differ"))

    switches: dict[str, list[dict]] = defaultdict(list)
    crossover = sorted(_read_csv(_key(keys, "cms_ffs", "crossover_members.csv")),
                       key=lambda r: r["effective_date"])  # fmt: skip
    for row in crossover:
        switches[row["BENE_MBI_ID"]].append(row)
    expected_switchers = {
        mbi: (len(rows) + 1, "humana_ma" if rows[-1]["direction"] == "FFS_TO_MA" else "cms_ffs")
        for mbi, rows in switches.items()
    }
    actual_switchers = {
        r.member_key: (r.coverage_span_count, r.current_payer_id)
        for r in marts["dim_member"].where("is_multi_payer").collect()
    }
    if differing := _differing(expected_switchers, actual_switchers):
        problems.append(
            Mismatch(
                "payer_switchers",
                None,
                f"{differing} of {len(expected_switchers)} switchers differ",
            )
        )
    if len(expected_switchers) != SWITCHERS:
        problems.append(
            Mismatch(
                "payer_switchers",
                None,
                f"the key lists {len(expected_switchers)} switchers, not {SWITCHERS}",
            )
        )

    if unreconciled := marts["check_marts_reconcile"].count():
        problems.append(Mismatch("marts_reconcile", None, f"{unreconciled} rows do not reconcile"))
    return problems


# --- Member months: an independent count from the delivered files ---------------------------


def expected_member_months(config: LakehouseConfig, generated: Path, payer_id: str) -> dict:
    """Month -> members covered any day of it, from every delivered eligibility file.

    generated: the generator's output root (cms/YYYY-MM/..., humana/YYYY-MM/...).
    """
    from synthetic_payer_files.cclf_common import coverage_spans  # dev dependency

    folder, file_type, key, start, end, death, mbi = ELIGIBILITY[payer_id]
    pattern = config.file_types[file_type].file_name_regex
    month_dirs = sorted((generated / folder).iterdir())
    rows = []
    for month_dir in month_dirs:
        [path] = [p for p in month_dir.iterdir() if re.match(pattern, p.name)]
        rows.extend(_read_csv(path))
    deaths = {r[key]: date.fromisoformat(r[death]) for r in rows if r[death]}
    mbis = {r[key]: r[mbi] for r in rows}
    spans = coverage_spans(rows, key, start, end)
    counts = {}
    for month_dir in month_dirs:
        year, mon = map(int, month_dir.name.split("-"))
        first, last = date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])
        covered = set()
        for member, member_spans in spans.items():
            for span_start, span_end in member_spans:
                if member in deaths and (span_end is None or deaths[member] < span_end):
                    span_end = deaths[member]
                if span_start <= last and (span_end is None or span_end >= first):
                    covered.add(mbis[member])
        counts[month_dir.name] = len(covered)
    return counts


def write_member_month_keys(config: LakehouseConfig, generated: Path, keys: Path) -> None:
    """Write member_months.csv into each payer's key folder."""
    for payer_id in KEY_DIRS:
        counts = expected_member_months(config, generated, payer_id)
        path = _key(keys, payer_id, MEMBER_MONTHS_FILE)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(["month", "members"])
            writer.writerows(sorted(counts.items()))
