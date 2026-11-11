"""A small Python model of AUTO CDC's SCD type 2 output, for tests (AUTO CDC only runs on
Databricks).

Rows are applied in sequence order per key. A row starts a new version only when a tracked
column differs from the current version; otherwise the current version's untracked
columns are updated in place. __START_AT / __END_AT are the sequence values of a version's
first row and of the next version's first row (NULL while current). Every source column is
stored except those in except_columns, as with except_column_list.
"""

from __future__ import annotations

from collections.abc import Sequence


def auto_cdc_scd2(
    rows: list[dict],
    key: str,
    tracked: Sequence[str],
    sequence: Sequence[str],
    except_columns: Sequence[str] = (),
) -> list[dict]:
    ordered = sorted(rows, key=lambda r: (r[key], *[r[c] for c in sequence]))
    versions: list[dict] = []
    current: dict[object, dict] = {}
    skip = {key, *tracked, *except_columns}
    untracked = [c for c in (ordered[0] if ordered else {}) if c not in skip]
    for row in ordered:
        at = {c: row[c] for c in sequence}
        version = current.get(row[key])
        if version is not None and all(version[c] == row[c] for c in tracked):
            version.update({c: row[c] for c in untracked})
            continue
        if version is not None:
            version["__END_AT"] = at
        version = {
            key: row[key],
            **{c: row[c] for c in tracked},
            **{c: row[c] for c in untracked},
            "__START_AT": at,
            "__END_AT": None,
        }
        versions.append(version)
        current[row[key]] = version
    return versions
