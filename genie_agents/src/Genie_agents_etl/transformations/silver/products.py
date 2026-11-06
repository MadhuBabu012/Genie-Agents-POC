"""Silver: products - one row per product."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("products"),
    comment="Silver products - one row per product (EAN = style x colour x size). mrp cast to double, is_pivotal to boolean, all strings trimmed.",
)
@dp.expect_all({"barcode present": "ean IS NOT NULL"})
def products():
    return silver_transform("products")