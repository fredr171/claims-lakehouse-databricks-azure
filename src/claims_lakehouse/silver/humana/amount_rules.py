"""MA payer line amount rules: the first rule a staged HUM_MEDCLM line breaks, or NULL.

Originals and replacements (frequency 1 and 7): 0 <= paid <= allowed <= billed; denied
lines allow and pay nothing; outside capitation, member cost share (copay + coinsurance
+ deductible) = allowed - paid. Reversals (frequency 8) negate their original, so no
amount may be positive.

Payers do send odd lines, so a single break only warns. A broken extract breaks many
lines at once, so the quality gate stops a month when more than 1% (and at least 5) of
its lines break a rule. Both are built on this one column.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from claims_lakehouse.silver.humana.claim_families import CAPITATED, DENIED, VOID

COST_SHARE_TOLERANCE = 0.005


def amount_problem() -> Column:
    billed, allowed, paid = F.col("billed_amount"), F.col("allowed_amount"), F.col("paid_amount")
    cost_share = sum(
        F.coalesce(F.col(c), F.lit(0))
        for c in ("copay_amount", "coinsurance_amount", "deductible_amount")
    )
    is_denied = F.col("claim_status_code") == DENIED
    is_capitated = F.col("capitated_flag") == CAPITATED
    return (
        F.when(billed.isNull() | allowed.isNull() | paid.isNull(), "missing amount")
        .when(
            F.col("claim_frequency_code") == VOID,
            F.when((billed > 0) | (allowed > 0) | (paid > 0), "reversal with a positive amount"),
        )
        .when((paid < 0) | (allowed < 0), "negative amount")
        .when(paid > allowed, "paid above allowed")
        .when(allowed > billed, "allowed above billed")
        .when(is_denied & ((allowed != 0) | (paid != 0)), "denied line with allowed or paid")
        .when(
            ~is_denied
            & ~F.coalesce(is_capitated, F.lit(False))
            & (F.abs(cost_share - (allowed - paid)) > COST_SHARE_TOLERANCE),
            "cost share is not allowed - paid",
        )
    )
