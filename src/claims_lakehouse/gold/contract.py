"""The conformed contract: what every registered payer supplies to gold.

Each payer's silver pipeline defines three conformed views (silver/<feed>/sql/
conformed_<kind>.sql) with exactly these columns, in this order, with these types.
Payer-specific knowledge (code values, statuses, how coverage is delivered) lives in those
views; the marts only union them, so onboarding a payer never changes a mart.

check_contract() is a STOP: a view that differs would make every mart wrong or fail.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

CONTRACT: dict[str, list[tuple[str, str]]] = {
    # One row per coverage span of a payer member.
    "coverage": [
        ("payer_id", "string"),
        ("payer_member_id", "string"),  # the payer's own id (CMS: MBI; MA payer: MEMBER_ID)
        ("beneficiary_mbi", "string"),  # the person across payers
        ("coverage_start_date", "date"),
        ("coverage_end_date", "date"),  # last covered day, capped at death; NULL while open
        ("plan_code", "string"),
        ("birth_year", "int"),  # year only: full birth dates stay in silver
        ("sex", "string"),  # M, F or U
        ("state_code", "string"),
        ("is_dual", "boolean"),
        ("is_deceased", "boolean"),
        ("last_delivery_month", "date"),  # the delivery the span's values come from
    ],
    # One row per claim family.
    "claims": [
        ("payer_id", "string"),
        ("payer_claim_id", "string"),  # unique within the payer
        ("payer_member_id", "string"),
        ("beneficiary_mbi", "string"),
        ("claim_category", "string"),  # INSTITUTIONAL or PROFESSIONAL
        ("payer_claim_status", "string"),  # the payer's own status, for traceability
        ("claim_status", "string"),  # PAID, ADJUSTED, VOIDED or DENIED
        ("service_start_date", "date"),
        ("service_month", "date"),
        ("first_delivery_month", "date"),
        ("latest_delivery_month", "date"),
        ("net_paid_amount", "decimal(18,2)"),
        ("is_capitated", "boolean"),
    ],
    # One row per current delivery (file type and month): the payer's delivery window.
    "deliveries": [
        ("payer_id", "string"),
        ("file_type", "string"),
        ("delivery_month", "date"),
        ("checksum", "string"),
    ],
}
KINDS = tuple(CONTRACT)


class ContractError(Exception):
    """A conformed view does not match the contract."""


def check_contract(df: DataFrame, kind: str, payer_id: str) -> None:
    expected = CONTRACT[kind]
    actual = [(name, dtype) for name, dtype in df.dtypes]
    if actual == expected:
        return
    problems = []
    names = dict(actual)
    for name, dtype in expected:
        if name not in names:
            problems.append(f"missing {name}")
        elif names[name] != dtype:
            problems.append(f"{name} is {names[name]}, not {dtype}")
    problems += [f"unexpected {name}" for name, _ in actual if name not in dict(expected)]
    if not problems:
        problems.append("columns out of order")
    raise ContractError(f"{payer_id} conformed_{kind}: " + "; ".join(problems))


def empty_relation(kind: str) -> str:
    """SQL for a zero-row relation with the contract's columns (no payer has data yet)."""
    columns = ", ".join(f"CAST(NULL AS {dtype}) AS {name}" for name, dtype in CONTRACT[kind])
    return f"(SELECT {columns} WHERE false)"
