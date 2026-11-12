import importlib

import pytest

SUBPACKAGES = [
    "claims_lakehouse",
    "claims_lakehouse.validation",
    "claims_lakehouse.bronze",
    "claims_lakehouse.silver",
    "claims_lakehouse.silver.cclf",
    "claims_lakehouse.silver.ma_payer",
    "claims_lakehouse.gold",
]


@pytest.mark.parametrize("name", SUBPACKAGES)
def test_subpackage_imports(name):
    importlib.import_module(name)
