"""Deliberately broken (and a few valid) CCLF4 files for the validation tests.

Each case is one file, tests/fixtures/validation/<case>/CCLF4_202301.csv, so a reviewer can
open it. Regenerate after changing a case (tests/test_validation.py checks the files on
disk still match these bytes, so git or an editor cannot quietly "fix" one):

    python tests/validation_fixtures.py

The rows are from synthetic-payer-files v2.0.0 (seed 42, 2023-01). All synthetic.
"""

from __future__ import annotations

import codecs
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "validation"
FILE_NAME = "CCLF4_202301.csv"

HEADER = (
    "CUR_CLM_UNIQ_ID,BENE_MBI_ID,BENE_HIC_NUM,CLM_TYPE_CD,CLM_PROD_TYPE_CD,CLM_VAL_SQNC_NUM,"
    "CLM_DGNS_CD,PRVDR_OSCAR_NUM,CLM_FROM_DT,CLM_THRU_DT,CLM_POA_IND,DGNS_PRCDR_ICD_IND"
)
ROWS = [
    "1000085822434,7ET9TQ4DF38,915549764A,40,P,0,E785,360588,2023-01-08,2023-01-08,,0",
    "1000085822434,7ET9TQ4DF38,915549764A,40,D,1,E669,360588,2023-01-08,2023-01-08,,0",
    "1000085822439,4EE7X62FH22,917206032A,40,P,0,R2681,360588,2022-12-12,2022-12-12,,0",
]
# Field values from ROWS long enough to recognise; no report may contain any of them.
DATA_VALUES = sorted({v for row in ROWS for v in row.split(",") if len(v) >= 4})


def _csv(header: str = HEADER, rows: list[str] = ROWS, eol: str = "\n") -> str:
    return "".join(line + eol for line in [header, *rows])


def _replace_row(i: int, row: str) -> list[str]:
    return [row if j == i else r for j, r in enumerate(ROWS)]


_columns = HEADER.split(",")
_swapped = [_columns[1], _columns[0], *_columns[2:]]

FIXTURES: dict[str, bytes] = {
    # Valid
    "valid": _csv().encode(),
    "valid_crlf": _csv(eol="\r\n").encode(),
    "valid_quoted": _csv(rows=_replace_row(0, ROWS[0].replace(",E785,", ',"E785",'))).encode(),
    # Encoding
    "bom_utf8": codecs.BOM_UTF8 + _csv().encode(),
    "utf16": _csv().encode("utf-16"),  # Python writes a little-endian BOM first
    "latin1": _csv(rows=_replace_row(1, ROWS[1].replace("E669", "É669"))).encode("latin-1"),
    "truncated_utf8": _csv().encode() + b"\xc3",  # ends inside a two-byte character
    # CSV structure
    "unclosed_quote": _csv(rows=_replace_row(2, ROWS[2].replace(",R2681,", ',"R2681,'))).encode(),
    "text_after_quote": _csv(rows=_replace_row(1, ROWS[1].replace(",E669,", ',"E66"9,'))).encode(),
    "embedded_line_break": _csv(
        rows=_replace_row(1, ROWS[1].replace(",E669,", ',"E6\n69",'))
    ).encode(),
    "ragged": _csv(rows=[ROWS[0], ROWS[1].rsplit(",", 1)[0], ROWS[2] + ",EXTRA"]).encode(),
    "empty_key": _csv(
        rows=[
            ROWS[0],
            ROWS[1].replace("1000085822434,", ",", 1),
            ROWS[2].replace("4EE7X62FH22", "  "),
        ]
    ).encode(),
    # Header
    "header_renamed": _csv(header=HEADER.replace("CLM_DGNS_CD", "CLM_DGNS_CODE")).encode(),
    "header_reordered": _csv(header=",".join(_swapped)).encode(),
    "header_lowercase": _csv(header=HEADER.lower()).encode(),
    "header_missing_column": _csv(
        header=HEADER.rsplit(",", 1)[0], rows=[r.rsplit(",", 1)[0] for r in ROWS]
    ).encode(),
    "header_extra_column": _csv(header=HEADER + ",NEW_COL", rows=[r + "," for r in ROWS]).encode(),
    "no_header": "".join(r + "\n" for r in ROWS).encode(),
    "header_only": (HEADER + "\n").encode(),
    "empty": b"",
    # Several problems at once: every one must be reported, not just the first.
    "several_problems": codecs.BOM_UTF8
    + _csv(rows=[ROWS[0], ROWS[1] + ",EXTRA", ROWS[2].replace("1000085822439,", ",", 1)]).encode(),
}


def write() -> None:
    for case, content in FIXTURES.items():
        path = FIXTURE_DIR / case / FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    print(f"wrote {len(FIXTURES)} fixtures under {FIXTURE_DIR}")


if __name__ == "__main__":
    write()
