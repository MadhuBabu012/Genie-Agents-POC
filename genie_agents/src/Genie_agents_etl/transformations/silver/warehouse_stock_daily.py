"""Silver: warehouse_stock_daily - one row per day x warehouse x EAN."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("warehouse_stock_daily"),
    comment="Silver warehouse stock - one row per day x warehouse x EAN.",
)
@dp.expect_all({"snapshot keys present": "snapshot_date IS NOT NULL AND warehouse IS NOT NULL AND ean IS NOT NULL"})
def warehouse_stock_daily():
    return silver_transform("warehouse_stock_daily")