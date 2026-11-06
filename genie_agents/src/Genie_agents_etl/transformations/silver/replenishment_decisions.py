"""Silver: replenishment_decisions - one row per store x EAN for the run date."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("replenishment_decisions"),
    comment="Silver replenishment decisions - one row per store x EAN, engine output for the day.",
)
@dp.expect_all({"decision keys present": "run_date IS NOT NULL AND store_code IS NOT NULL AND ean IS NOT NULL"})
def replenishment_decisions():
    return silver_transform("replenishment_decisions")