"""Arrival order and corrected re-sends do not change the result (all 36 months, both feeds).

    in order       every file delivered month by month, one job run      the reference
    shuffled       the same files in random order across feeds and       == in order
                   months, in two uneven batches with a run after each
    re-sent        then corrected re-sends of three old files, a run     == a clean load of
                                                                            the corrected files
The corrections are the supersession cases: a beneficiary whose only row is removed must
vanish, a removed member change must be retracted from history, and a removed replacement
claim must leave its family voided.
"""

import csv
import random

import pytest
from harness import LocalArchive, edited_copy, generated_file, months
from lakehouse import LocalLakehouse, snapshot

pytestmark = pytest.mark.slow

FEED_OF = {"beneficiary_demographics": "cclf", "member_eligibility": "ma_payer",
           "medical_claim_lines": "ma_payer"}  # fmt: skip


def outputs(lakehouse):
    """Everything a run publishes: each payer's silver outputs and the marts."""
    tables = {f"{p}.{name}": df for p, t in lakehouse.published.items() for name, df in t.items()}
    return snapshot(tables | {f"gold.{name}": df for name, df in lakehouse.gold.items()})


def read_rows(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def corrections(config, generated):
    """The three corrected re-sends: ({(file type, month): edit}, {what each removes}).

    The rows to remove are picked from the generated files, so they exist whatever the
    generator's seed produces.
    """

    def file(file_type, month):
        return generated_file(config, generated, file_type, month)

    feb = read_rows(file("beneficiary_demographics", "2023-02"))
    later = {
        r["BENE_MBI_ID"]
        for m in months("2023-03")
        for r in read_rows(file("beneficiary_demographics", m))
    }
    gone = next(
        r["BENE_MBI_ID"]
        for r in feb
        if r["RECORD_CHANGE_TYPE"] == "NEW" and r["BENE_MBI_ID"] not in later
    )
    changed = next(r["BENE_MBI_ID"] for r in feb if r["RECORD_CHANGE_TYPE"] == "UPDATE")
    claims_month, replacement_row = next(
        (m, r)
        for m in months("2024-01")
        for r in read_rows(file("medical_claim_lines", m))
        if r["CLAIM_FREQ_CD"] == "7"
    )
    replacement, replaced_family = replacement_row["CLAIM_ID"], replacement_row["ORIG_CLAIM_ID"]
    update = next(
        r["MEMBER_ID"]
        for r in read_rows(file("member_eligibility", "2024-05"))
        if r["RECORD_CHANGE_TYPE"] == "UPDATE"
    )
    return {
        ("beneficiary_demographics", "2023-02"): (
            lambda rows: [r for r in rows if r["BENE_MBI_ID"] not in (gone, changed)]
        ),
        ("medical_claim_lines", claims_month): (
            lambda rows: [
                r for r in rows if not (r["CLAIM_ID"] == replacement and r["CLAIM_FREQ_CD"] == "7")
            ]
        ),
        ("member_eligibility", "2024-05"): (
            lambda rows: [r for r in rows if r["MEMBER_ID"] != update]
        ),
    }, {"gone": gone, "changed": changed, "replaced_family": replaced_family, "update": update}


@pytest.fixture(scope="module")
def loads(spark, config, generated, tmp_path_factory):
    root = tmp_path_factory.mktemp("load_order")
    edits, removed = corrections(config, generated)
    corrected = {
        key: edited_copy(generated_file(config, generated, *key), root / "corrected", edit)
        for key, edit in edits.items()
    }

    # In order: the reference.
    in_order = LocalLakehouse(spark, config, LocalArchive(root / "in_order", config))
    for month in months():
        in_order.archive.deliver_month(generated, month)
    in_order.update()

    # Shuffled across feeds and months, in two uneven batches.
    folders = {"cclf": "cms", "ma_payer": "ma_payer"}
    files = [
        (feed, path)
        for month in months()
        for feed, folder in folders.items()
        for path in sorted((generated / folder / month).glob("*.csv"))
    ]
    random.Random(7).shuffle(files)
    shuffled = LocalLakehouse(spark, config, LocalArchive(root / "shuffled", config))
    for batch in (files[:23], files[23:]):
        for feed, path in batch:
            shuffled.archive.deliver_file(path, shuffled.archive.landing_path(feed, path.name))
        shuffled.update()
    shuffled_outputs = outputs(shuffled)

    # Then the corrected re-sends.
    for (file_type, _), path in corrected.items():
        shuffled.archive.deliver_file(
            path, shuffled.archive.landing_path(FEED_OF[file_type], path.name)
        )
    resent_outcomes = shuffled.update()

    # A clean load of the corrected files.
    clean = LocalLakehouse(spark, config, LocalArchive(root / "clean", config))
    for month in months():
        replace = {p.name: p for (_, m), p in corrected.items() if m == month}
        clean.archive.deliver_month(generated, month, replace=replace)
    clean.update()

    return {
        "in_order": outputs(in_order),
        "shuffled": shuffled_outputs,
        "resent": outputs(shuffled),
        "resent_outcomes": resent_outcomes,
        "clean": outputs(clean),
        "resent_lakehouse": shuffled,
        "removed": removed,
    }


def test_arrival_order_and_batches_do_not_change_the_result(loads):
    assert loads["shuffled"] == loads["in_order"]


def test_resends_equal_a_clean_load_of_the_corrected_files(loads):
    assert {o.status for o in loads["resent_outcomes"].values()} == {"passed"}
    assert loads["resent"] == loads["clean"]
    assert loads["resent"] != loads["in_order"]  # the corrections changed something


def test_resent_corrections_are_retracted(loads):
    removed, lakehouse = loads["removed"], loads["resent_lakehouse"]
    beneficiaries = lakehouse.published["cms_ffs"]["beneficiary_versions"]
    assert beneficiaries.where(beneficiaries.beneficiary_mbi == removed["gone"]).count() == 0
    assert lakehouse.gold["dim_member"].where(f"member_key = '{removed['gone']}'").count() == 0
    no_feb = beneficiaries.where(
        (beneficiaries.beneficiary_mbi == removed["changed"])
        & (beneficiaries.valid_from == "2023-02")
    )
    assert no_feb.count() == 0
    members = lakehouse.published["ma_payer"]["member_versions"]
    assert (
        members.where(
            (members.member_id == removed["update"]) & (members.valid_from == "2024-05")
        ).count()
        == 0
    )
    families = lakehouse.published["ma_payer"]["claim_families"]
    [family] = families.where(families.claim_family_key == removed["replaced_family"]).collect()
    assert (family.family_status, family.net_paid_amount) == ("VOIDED", 0)
    assert (family.reversal_count, family.replacement_count) == (1, 0)
