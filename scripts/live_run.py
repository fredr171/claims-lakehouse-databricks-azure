"""Drive a live run: generate the synthetic deliveries and send them to the lake.

    python scripts/live_run.py generate                  # 36 months + answer keys -> output/
    python scripts/live_run.py keys                      # answer keys -> the ops volume
    python scripts/live_run.py deliver 2023-01 2023-12   # those months -> landing
    python scripts/live_run.py resend beneficiary_demographics 2023-02

Files go to the landing container exactly as a payer would send them; the validation
Function archives them and the archive's file-arrival trigger starts the job. `resend`
re-sends a delivered file with its rows in another order: a new checksum (so a corrected
re-send supersedes the original) with the same records, so the answer keys still hold.

Needs the Azure CLI (signed in; the deployer has Storage Blob Data Contributor on the lake)
and the Databricks CLI (DATABRICKS_HOST set). Names come from `terraform output`.
"""

from __future__ import annotations

import argparse
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from claims_lakehouse.checks.answer_keys import write_member_month_keys  # noqa: E402
from claims_lakehouse.config import load_config  # noqa: E402

OUTPUT = REPO / "output"
GENERATED_DIRS = {"cclf": "cms", "humana": "humana"}
SEED, FIRST_MONTH, LAST_MONTH = "42", "2023-01", "2025-12"


def run(*args: str) -> str:
    exe = shutil.which(args[0]) or args[0]  # az is az.cmd on Windows
    return subprocess.run([exe, *args[1:]], check=True, capture_output=True, text=True).stdout


def terraform_output(name: str) -> str:
    return run("terraform", f"-chdir={REPO / 'infra' / 'terraform'}", "output", "-raw", name)


def generate() -> None:
    from synthetic_payer_files import generate_cclf, generate_humana

    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    months = ["--seed", SEED, "--start", FIRST_MONTH, "--end", LAST_MONTH]
    key = OUTPUT / "key"
    generate_cclf.main([*months, "--output-dir", str(OUTPUT / "cms"), "--answer-key-dir", str(key)])
    generate_humana.main(
        [
            *months,
            "--output-dir",
            str(OUTPUT / "humana"),
            "--answer-key-dir",
            str(key / "humana"),
            "--crossover-dir",
            str(key),
        ]
    )
    write_member_month_keys(load_config(), OUTPUT, key)
    print(f"generated {FIRST_MONTH}..{LAST_MONTH} and answer keys in {OUTPUT}")


def keys() -> None:
    volume = f"dbfs:/Volumes/{terraform_output('catalog')}/ops/answer_keys"
    run("databricks", "fs", "cp", "--recursive", "--overwrite", str(OUTPUT / "key"), volume)
    print(f"answer keys uploaded to {volume}")


def landing_path(feed: str, file_name: str) -> str:
    [file_type] = [
        ft for ft in load_config().file_types_for_feed(feed) if ft.delivery_month(file_name)
    ]
    return f"{file_type.feed.prefix}{file_type.file_type}/{file_name}"


def upload(files: list[tuple[Path, str]]) -> None:
    """Upload (local file, landing path) pairs in one batch."""
    account = terraform_output("lake_storage_account_name")
    with tempfile.TemporaryDirectory() as staging:
        for local, path in files:
            target = Path(staging) / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local, target)
        run("az", "storage", "blob", "upload-batch", "--account-name", account,
            "--destination", "landing", "--source", staging, "--auth-mode", "login",
            "--overwrite", "true", "--only-show-errors", "-o", "none")  # fmt: skip
    print(f"delivered {len(files)} files to landing")


def months_between(first: str, last: str) -> list[str]:
    folders = sorted(p.name for p in (OUTPUT / "cms").iterdir())
    return [m for m in folders if first <= m <= last]


def deliver(first: str, last: str) -> None:
    files = []
    for month in months_between(first, last):
        for feed, folder in GENERATED_DIRS.items():
            for local in sorted((OUTPUT / folder / month).glob("*.csv")):
                files.append((local, landing_path(feed, local.name)))
    upload(files)


def resend(file_type: str, month: str) -> None:
    ft = load_config().file_types[file_type]
    folder = OUTPUT / GENERATED_DIRS[ft.feed.name] / month
    [original] = [p for p in folder.glob("*.csv") if ft.delivery_month(p.name) == month]
    header, *rows = original.read_text(encoding="utf-8").splitlines(keepends=True)
    random.Random(month).shuffle(rows)
    with tempfile.TemporaryDirectory() as tmp:
        corrected = Path(tmp) / original.name
        corrected.write_text(header + "".join(rows), encoding="utf-8", newline="")
        upload([(corrected, landing_path(ft.feed.name, original.name))])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("generate")
    commands.add_parser("keys")
    d = commands.add_parser("deliver")
    d.add_argument("first")
    d.add_argument("last")
    r = commands.add_parser("resend")
    r.add_argument("file_type")
    r.add_argument("month")
    args = parser.parse_args()
    if args.command == "deliver":
        deliver(args.first, args.last)
    elif args.command == "resend":
        resend(args.file_type, args.month)
    else:
        {"generate": generate, "keys": keys}[args.command]()


if __name__ == "__main__":
    main()
