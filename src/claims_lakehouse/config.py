"""Load feeds, file types and the payer registry from the YAML files in config/.

    feeds.yml               feeds (one landing prefix each) and default file rules
    file_types/<name>.yml   one per file type: name pattern, header, key columns
    payers.yml              the payer registry gold reads

Adding a file type or onboarding a payer is a config change, not a code change. Everything
is checked on load (unknown keys included), so a typo fails fast instead of mid-run.
"""

from __future__ import annotations

import codecs
import os
import re
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR_ENV_VAR = "CLAIMS_LAKEHOUSE_CONFIG_DIR"
_REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

# Bronze adds lineage columns named _<something>; delivered columns must never collide.
LINEAGE_COLUMN_PREFIX = "_"
# Every file name regex must capture these; together they give the delivery month.
DELIVERY_MONTH_GROUPS = ("year", "month")

_SNAKE_CASE = re.compile(r"[a-z][a-z0-9_]*")
_MONTH = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


class ConfigError(ValueError):
    """A config file is missing, malformed or inconsistent."""


@dataclass(frozen=True)
class Feed:
    """A data source. Its files land at <prefix><file_type>/<file_name>."""

    name: str
    prefix: str


@dataclass(frozen=True)
class FileRules:
    """File-level rules with a feed-wide default and optional per-file-type overrides."""

    max_file_bytes: int
    encoding: str

    def with_overrides(self, overrides: dict[str, Any], where: str) -> FileRules:
        unknown = set(overrides) - {f.name for f in fields(FileRules)}
        if unknown:
            raise ConfigError(f"{where}: unknown rule(s) {sorted(unknown)}")
        return _checked_rules(replace(self, **overrides), where)


@dataclass(frozen=True)
class FileType:
    """Everything needed to validate one kind of delivered file."""

    file_type: str
    feed: Feed
    file_name_regex: re.Pattern[str]
    key_columns: tuple[str, ...]
    expected_columns: tuple[str, ...]
    rules: FileRules

    def delivery_month(self, file_name: str) -> str | None:
        """'YYYY-MM' from a matching file name, or None if the name does not match."""
        match = self.file_name_regex.fullmatch(file_name)
        if match is None:
            return None
        return f"{match['year']}-{match['month']}"


@dataclass(frozen=True)
class Payer:
    """One payer registry entry."""

    payer_id: str
    payer_name: str
    line_of_business: str
    feed: str
    onboarded_month: str
    retired_month: str | None


@dataclass(frozen=True)
class LandingPath:
    """A path in the landing container split into its parts."""

    feed: Feed
    file_type: str  # the folder name; may not be a configured file type
    file_name: str


@dataclass(frozen=True)
class LakehouseConfig:
    feeds: tuple[Feed, ...]
    file_types: dict[str, FileType]
    payers: tuple[Payer, ...]

    def parse_landing_path(self, path: str) -> LandingPath | None:
        """Split '<feed prefix><file_type>/<file_name>'.

        None for paths outside every feed or at any other depth: those are not deliveries.
        """
        feed = next((f for f in self.feeds if path.startswith(f.prefix)), None)
        if feed is None:
            return None
        parts = path[len(feed.prefix) :].split("/")
        if len(parts) != 2 or not all(parts):
            return None
        return LandingPath(feed=feed, file_type=parts[0], file_name=parts[1])

    def file_type_for(self, landing: LandingPath) -> FileType | None:
        """The configured file type of a landing path, only if it belongs to that feed."""
        file_type = self.file_types.get(landing.file_type)
        if file_type is None or file_type.feed != landing.feed:
            return None
        return file_type

    def file_types_for_feed(self, feed_name: str) -> tuple[FileType, ...]:
        return tuple(ft for ft in self.file_types.values() if ft.feed.name == feed_name)


def default_config_dir() -> Path:
    """$CLAIMS_LAKEHOUSE_CONFIG_DIR if set, else the repo's config/ folder."""
    return Path(os.environ.get(CONFIG_DIR_ENV_VAR, _REPO_CONFIG_DIR))


def load_config(config_dir: Path | str | None = None) -> LakehouseConfig:
    """Load and cross-check feeds.yml, file_types/*.yml and payers.yml."""
    config_dir = Path(config_dir) if config_dir is not None else default_config_dir()

    settings = _read_yaml(config_dir / "feeds.yml")
    _check_keys(settings, "feeds.yml", required=("feeds", "default_rules"))
    feeds = _parse_feeds(settings["feeds"])
    default_rules = _parse_default_rules(settings["default_rules"])

    paths = sorted((config_dir / "file_types").glob("*.yml"))
    if not paths:
        raise ConfigError(f"no file types in {config_dir / 'file_types'}")
    feeds_by_name = {feed.name: feed for feed in feeds}
    file_types = {}
    for path in paths:
        file_type = _parse_file_type(_read_yaml(path), path.name, feeds_by_name, default_rules)
        if file_type.file_type != path.stem:
            raise ConfigError(f"{path.name}: file_type must equal the file name stem")
        file_types[file_type.file_type] = file_type
    for feed in feeds:
        if not any(ft.feed == feed for ft in file_types.values()):
            raise ConfigError(f"feed {feed.name!r} has no file types")

    payers_raw = _read_yaml(config_dir / "payers.yml")
    _check_keys(payers_raw, "payers.yml", required=("payers",))
    payers = _parse_payers(payers_raw["payers"], feeds_by_name)

    return LakehouseConfig(feeds=feeds, file_types=file_types, payers=payers)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name}: invalid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name}: must contain a YAML mapping")
    return data


def _check_keys(
    raw: Any, where: str, required: tuple[str, ...], optional: tuple[str, ...] = ()
) -> None:
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: must be a mapping, got {raw!r}")
    missing = [key for key in required if key not in raw]
    if missing:
        raise ConfigError(f"{where}: missing key(s) {missing}")
    unknown = sorted(set(raw) - set(required) - set(optional))
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {unknown}")


def _text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}: must be a non-empty string, got {value!r}")
    return value


def _snake_case(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _SNAKE_CASE.fullmatch(value):
        raise ConfigError(f"{where}: must be snake_case, got {value!r}")
    return value


def _month(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _MONTH.fullmatch(value):
        raise ConfigError(f"{where}: must be a quoted YYYY-MM string, got {value!r}")
    return value


def _unique_names(value: Any, where: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{where}: must be a non-empty list")
    names = tuple(_text(item, where) for item in value)
    if len(set(names)) != len(names):
        raise ConfigError(f"{where}: has duplicates")
    return names


def _parse_feeds(value: Any) -> tuple[Feed, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError("feeds.yml: feeds must be a non-empty list")
    feeds = []
    for i, raw in enumerate(value):
        where = f"feeds.yml: feeds[{i}]"
        _check_keys(raw, where, required=("name", "prefix"))
        prefix = _text(raw["prefix"], f"{where}.prefix")
        if not prefix.endswith("/") or prefix.startswith("/"):
            raise ConfigError(f"{where}.prefix: must end with '/' and not start with it")
        feeds.append(Feed(name=_snake_case(raw["name"], f"{where}.name"), prefix=prefix))

    if len({feed.name for feed in feeds}) != len(feeds):
        raise ConfigError("feeds.yml: feed names must be unique")
    for i, feed in enumerate(feeds):
        for other in feeds[i + 1 :]:
            if feed.prefix.startswith(other.prefix) or other.prefix.startswith(feed.prefix):
                raise ConfigError(
                    f"feeds.yml: prefixes must not overlap: {feed.prefix!r}, {other.prefix!r}"
                )
    return tuple(feeds)


def _parse_default_rules(raw: Any) -> FileRules:
    where = "feeds.yml: default_rules"
    names = tuple(f.name for f in fields(FileRules))
    _check_keys(raw, where, required=names)
    return _checked_rules(FileRules(**raw), where)


def _checked_rules(rules: FileRules, where: str) -> FileRules:
    size = rules.max_file_bytes
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ConfigError(f"{where}: max_file_bytes must be a positive integer, got {size!r}")
    try:
        codecs.lookup(_text(rules.encoding, f"{where}.encoding"))
    except LookupError as exc:
        raise ConfigError(f"{where}: unknown encoding {rules.encoding!r}") from exc
    return rules


def _parse_file_type(
    raw: dict[str, Any], where: str, feeds: dict[str, Feed], default_rules: FileRules
) -> FileType:
    _check_keys(
        raw,
        where,
        required=("feed", "file_type", "file_name_regex", "key_columns", "expected_columns"),
        optional=("rules",),
    )
    feed = feeds.get(_text(raw["feed"], f"{where}: feed"))
    if feed is None:
        raise ConfigError(f"{where}: unknown feed {raw['feed']!r} (not in feeds.yml)")

    try:
        regex = re.compile(_text(raw["file_name_regex"], f"{where}: file_name_regex"))
    except re.error as exc:
        raise ConfigError(f"{where}: invalid file_name_regex: {exc}") from exc
    missing_groups = [g for g in DELIVERY_MONTH_GROUPS if g not in regex.groupindex]
    if missing_groups:
        raise ConfigError(f"{where}: file_name_regex must capture named groups {missing_groups}")

    columns = _unique_names(raw["expected_columns"], f"{where}: expected_columns")
    reserved = [c for c in columns if c.startswith(LINEAGE_COLUMN_PREFIX)]
    if reserved:
        raise ConfigError(
            f"{where}: columns {reserved} start with {LINEAGE_COLUMN_PREFIX!r} "
            "(reserved for bronze lineage columns)"
        )
    keys = _unique_names(raw["key_columns"], f"{where}: key_columns")
    if not set(keys) <= set(columns):
        raise ConfigError(f"{where}: key_columns {sorted(set(keys) - set(columns))} not in columns")

    overrides = raw.get("rules") or {}
    if not isinstance(overrides, dict):
        raise ConfigError(f"{where}: rules must be a mapping")

    return FileType(
        file_type=_snake_case(raw["file_type"], f"{where}: file_type"),
        feed=feed,
        file_name_regex=regex,
        key_columns=keys,
        expected_columns=columns,
        rules=default_rules.with_overrides(overrides, f"{where}: rules"),
    )


def _parse_payers(value: Any, feeds: dict[str, Feed]) -> tuple[Payer, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError("payers.yml: payers must be a non-empty list (register at least one)")
    payers = []
    for i, raw in enumerate(value):
        where = f"payers.yml: payers[{i}]"
        _check_keys(
            raw,
            where,
            required=("payer_id", "payer_name", "line_of_business", "feed", "onboarded_month"),
            optional=("retired_month",),
        )
        if _text(raw["feed"], f"{where}.feed") not in feeds:
            raise ConfigError(f"{where}: unknown feed {raw['feed']!r} (not in feeds.yml)")
        onboarded = _month(raw["onboarded_month"], f"{where}.onboarded_month")
        retired = raw.get("retired_month")
        if retired is not None:
            retired = _month(retired, f"{where}.retired_month")
            if retired < onboarded:  # YYYY-MM strings sort chronologically
                raise ConfigError(f"{where}: retired_month is before onboarded_month")
        payers.append(
            Payer(
                payer_id=_snake_case(raw["payer_id"], f"{where}.payer_id"),
                payer_name=_text(raw["payer_name"], f"{where}.payer_name"),
                line_of_business=_text(raw["line_of_business"], f"{where}.line_of_business"),
                feed=raw["feed"],
                onboarded_month=onboarded,
                retired_month=retired,
            )
        )

    for attr in ("payer_id", "feed"):
        values = [getattr(p, attr) for p in payers]
        if len(set(values)) != len(values):
            raise ConfigError(f"payers.yml: {attr} must be unique across payers")
    return tuple(payers)
