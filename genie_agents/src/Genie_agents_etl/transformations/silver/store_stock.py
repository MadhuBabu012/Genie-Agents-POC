"""Silver: store_stock - one row per store x EAN on the as-of date."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("store_stock"),
    comment="Silver store stock - one row per store x EAN, snapshot on the as-of date.",
)
@dp.expect_all({"snapshot keys present": "snapshot_date IS NOT NULL AND store_code IS NOT NULL AND ean IS NOT NULL"})
def store_stock():
    return silver_transform("store_stock")