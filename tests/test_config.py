"""Config loading: the repo's own config, path resolution, and every rejection rule."""

import csv
import shutil
from pathlib import Path

import pytest
import yaml
from synthetic_payer_files import generate_cclf, generate_ma_payer

from claims_lakehouse.config import CONFIG_DIR_ENV_VAR, ConfigError, load_config

REPO_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

# Generated file name stem -> configured file type.
GENERATED_FILES = {
    "CCLF8": "beneficiary_demographics",
    "CCLF1": "claims_header",
    "CCLF4": "claims_diagnosis",
    "CCLF5": "part_b_physician_lines",
    "MAP_ELIG": "member_eligibility",
    "MAP_MEDCLM": "medical_claim_lines",
}


@pytest.fixture(scope="module")
def repo_config():
    return load_config(REPO_CONFIG_DIR)


# --- The repo's config -------------------------------------------------------------------


def test_repo_config_loads(repo_config):
    assert [f.name for f in repo_config.feeds] == ["cclf", "ma_payer"]
    assert sorted(repo_config.file_types) == sorted(GENERATED_FILES.values())
    assert [p.payer_id for p in repo_config.payers] == ["cms_ffs", "ma_payer"]
    assert {ft.file_type for ft in repo_config.file_types_for_feed("ma_payer")} == {
        "member_eligibility",
        "medical_claim_lines",
    }


def test_config_dir_from_environment(monkeypatch, tmp_path):
    shutil.copytree(REPO_CONFIG_DIR, tmp_path / "config")
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(tmp_path / "config"))
    assert len(load_config().file_types) == len(GENERATED_FILES)


@pytest.fixture(scope="module")
def generated_month(tmp_path_factory):
    """One month from each generator (seed 42), the release the tests pin."""
    root = tmp_path_factory.mktemp("generated")
    months = ["--seed", "42", "--start", "2023-01", "--end", "2023-01"]
    generate_cclf.main(
        [*months, "--output-dir", str(root / "cms"), "--answer-key-dir", str(root / "key")]
    )
    generate_ma_payer.main(
        [
            *months,
            "--output-dir", str(root / "ma_payer"),
            "--answer-key-dir", str(root / "key" / "ma_payer"),
            "--crossover-dir", str(root / "key"),
        ]
    )  # fmt: skip
    return {path.name: path for path in root.rglob("*_202301.csv")}


@pytest.mark.parametrize("stem", GENERATED_FILES)
def test_expected_columns_match_generated_header(repo_config, generated_month, stem):
    file_name = f"{stem}_202301.csv"
    with generated_month[file_name].open(newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    file_type = repo_config.file_types[GENERATED_FILES[stem]]
    assert list(file_type.expected_columns) == header
    assert file_type.delivery_month(file_name) == "2023-01"


# --- File names and landing paths --------------------------------------------------------


@pytest.mark.parametrize(
    ("file_name", "expected"),
    [
        ("CCLF1_202512.csv", "2025-12"),
        ("CCLF1_202513.csv", None),  # no month 13
        ("CCLF1_202500.csv", None),
        ("CCLF1_2025012.csv", None),
        ("cclf1_202501.csv", None),  # case matters
        ("CCLF1_202501.CSV", None),
        ("CCLF1_202501.csv.bak", None),
        ("XCCLF1_202501.csv", None),
        ("CCLF8_202501.csv", None),  # another file type's name
    ],
)
def test_delivery_month_from_file_name(repo_config, file_name, expected):
    assert repo_config.file_types["claims_header"].delivery_month(file_name) == expected


@pytest.mark.parametrize(
    ("path", "feed", "file_type", "file_name"),
    [
        ("cms/cclf/claims_header/CCLF1_202301.csv", "cclf", "claims_header", "CCLF1_202301.csv"),
        ("ma_payer/ma/unknown_type/x.csv", "ma_payer", "unknown_type", "x.csv"),
    ],
)
def test_parse_landing_path(repo_config, path, feed, file_type, file_name):
    landing = repo_config.parse_landing_path(path)
    assert (landing.feed.name, landing.file_type, landing.file_name) == (feed, file_type, file_name)


@pytest.mark.parametrize(
    "path",
    [
        "other/feed/claims_header/CCLF1_202301.csv",  # outside every feed
        "cms/cclf/CCLF1_202301.csv",  # no file type folder
        "cms/cclf/claims_header/2023/CCLF1_202301.csv",  # nested deeper
        "cms/cclf/claims_header/",  # a folder, not a file
        "cms/CCLF1_202301.csv",
    ],
)
def test_paths_that_are_not_deliveries(repo_config, path):
    assert repo_config.parse_landing_path(path) is None


def test_file_type_must_belong_to_the_feed(repo_config):
    right = repo_config.parse_landing_path("cms/cclf/claims_header/CCLF1_202301.csv")
    wrong = repo_config.parse_landing_path("ma_payer/ma/claims_header/CCLF1_202301.csv")
    unknown = repo_config.parse_landing_path("cms/cclf/claims_headers/CCLF1_202301.csv")
    assert repo_config.file_type_for(right).file_type == "claims_header"
    assert repo_config.file_type_for(wrong) is None
    assert repo_config.file_type_for(unknown) is None


def test_rule_override_applies_to_one_file_type(tmp_path):
    config_dir = _copy_config(tmp_path)
    _edit(
        config_dir / "file_types/claims_header.yml",
        lambda d: d.update(rules={"max_file_bytes": 10}),
    )
    config = load_config(config_dir)
    assert config.file_types["claims_header"].rules.max_file_bytes == 10
    assert config.file_types["claims_header"].rules.encoding == "utf-8"
    assert config.file_types["claims_diagnosis"].rules.max_file_bytes == 536870912


# --- Every rule rejects a broken config --------------------------------------------------

FEEDS = "feeds.yml"
PAYERS = "payers.yml"
HEADER = "file_types/claims_header.yml"


def _set(key, value):
    return lambda d: d.__setitem__(key, value)


def _drop(key):
    return lambda d: d.pop(key)


def _feed(i, key, value):
    return lambda d: d["feeds"][i].__setitem__(key, value)


def _payer(i, key, value):
    return lambda d: d["payers"][i].__setitem__(key, value)


BROKEN = {
    # feeds.yml
    "feeds missing": (FEEDS, _drop("feeds"), "missing key"),
    "feeds empty": (FEEDS, _set("feeds", []), "non-empty list"),
    "unknown top-level key": (FEEDS, _set("feed", []), "unknown key"),
    "feed name not snake_case": (FEEDS, _feed(0, "name", "CMS-CCLF"), "snake_case"),
    "prefix without slash": (FEEDS, _feed(0, "prefix", "cms/cclf"), "must end with '/'"),
    "prefix leading slash": (FEEDS, _feed(0, "prefix", "/cms/cclf/"), "must end with '/'"),
    "duplicate feed name": (FEEDS, _feed(1, "name", "cclf"), "unique"),
    "overlapping prefixes": (FEEDS, _feed(1, "prefix", "cms/"), "must not overlap"),
    "feed without file types": (
        FEEDS,
        lambda d: d["feeds"].append({"name": "aetna", "prefix": "aetna/"}),
        "has no file types",
    ),
    "default rule missing": (FEEDS, lambda d: d["default_rules"].pop("encoding"), "missing key"),
    "unknown default rule": (
        FEEDS,
        lambda d: d["default_rules"].update(max_rows=5),
        "unknown key",
    ),
    "size zero": (FEEDS, lambda d: d["default_rules"].update(max_file_bytes=0), "positive"),
    "size as text": (FEEDS, lambda d: d["default_rules"].update(max_file_bytes="1"), "positive"),
    "size as bool": (FEEDS, lambda d: d["default_rules"].update(max_file_bytes=True), "positive"),
    "unknown encoding": (
        FEEDS,
        lambda d: d["default_rules"].update(encoding="utf-9"),
        "unknown encoding",
    ),
    # file_types/*.yml
    "file type key missing": (HEADER, _drop("key_columns"), "missing key"),
    "file type key misspelled": (HEADER, _set("key_column", ["CUR_CLM_UNIQ_ID"]), "unknown key"),
    "unknown feed": (HEADER, _set("feed", "aetna"), "unknown feed"),
    "feed not a string": (HEADER, _set("feed", ["cclf"]), "non-empty string"),
    "stem mismatch": (HEADER, _set("file_type", "claim_headers"), "file name stem"),
    "invalid regex": (
        HEADER,
        _set("file_name_regex", "^CCLF1_(?P<year>"),
        "invalid file_name_regex",
    ),
    "regex without month": (
        HEADER,
        _set("file_name_regex", r"^CCLF1_(?P<year>\d{4})\d{2}\.csv$"),
        "named groups ['month']",
    ),
    "no columns": (HEADER, _set("expected_columns", []), "non-empty list"),
    "duplicate column": (
        HEADER,
        lambda d: d["expected_columns"].append("CLM_PMT_AMT"),
        "duplicates",
    ),
    "blank column": (HEADER, lambda d: d["expected_columns"].append(" "), "non-empty string"),
    "lineage column clash": (
        HEADER,
        lambda d: d["expected_columns"].append("_checksum"),
        "reserved for bronze lineage",
    ),
    "no key columns": (HEADER, _set("key_columns", []), "non-empty list"),
    "key not a column": (HEADER, _set("key_columns", ["CLAIM_ID"]), "not in columns"),
    "unknown rule override": (HEADER, _set("rules", {"max_rows": 5}), "unknown rule"),
    "bad rule override": (HEADER, _set("rules", {"max_file_bytes": -1}), "positive"),
    "rules not a mapping": (HEADER, _set("rules", ["max_file_bytes"]), "must be a mapping"),
    # payers.yml
    "no payers": (PAYERS, _set("payers", []), "register at least one"),
    "payer key missing": (PAYERS, lambda d: d["payers"][0].pop("payer_name"), "missing key"),
    "payer key misspelled": (PAYERS, _payer(0, "retire_month", "2025-01"), "unknown key"),
    "payer id not snake_case": (PAYERS, _payer(0, "payer_id", "CMS FFS"), "snake_case"),
    "payer unknown feed": (PAYERS, _payer(0, "feed", "aetna"), "unknown feed"),
    "duplicate payer id": (PAYERS, _payer(1, "payer_id", "cms_ffs"), "payer_id must be unique"),
    "two payers one feed": (PAYERS, _payer(1, "feed", "cclf"), "feed must be unique"),
    "month without leading zero": (PAYERS, _payer(0, "onboarded_month", "2023-1"), "YYYY-MM"),
    "month 13": (PAYERS, _payer(0, "retired_month", "2025-13"), "YYYY-MM"),
    "month as date": (PAYERS, _payer(0, "onboarded_month", "2023-01-01"), "YYYY-MM"),
    "retired before onboarded": (
        PAYERS,
        _payer(0, "retired_month", "2022-12"),
        "before onboarded_month",
    ),
}


@pytest.mark.parametrize(("file", "change", "message"), BROKEN.values(), ids=BROKEN.keys())
def test_broken_config_is_rejected(tmp_path, file, change, message):
    config_dir = _copy_config(tmp_path)
    _edit(config_dir / file, change)
    with pytest.raises(ConfigError, match=message.replace("[", r"\[")):
        load_config(config_dir)


@pytest.mark.parametrize(
    ("setup", "message"),
    [
        (lambda d: (d / "payers.yml").unlink(), "not found"),
        (lambda d: [p.unlink() for p in (d / "file_types").glob("*.yml")], "no file types"),
        (lambda d: (d / "feeds.yml").write_text("- a list\n"), "YAML mapping"),
        (lambda d: (d / "feeds.yml").write_text("feeds: [\n"), "invalid YAML"),
    ],
    ids=["missing file", "no file types", "not a mapping", "invalid yaml"],
)
def test_broken_config_files_are_rejected(tmp_path, setup, message):
    config_dir = _copy_config(tmp_path)
    setup(config_dir)
    with pytest.raises(ConfigError, match=message):
        load_config(config_dir)


def _copy_config(tmp_path: Path) -> Path:
    return Path(shutil.copytree(REPO_CONFIG_DIR, tmp_path / "config"))


def _edit(path: Path, change) -> None:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    change(data)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
