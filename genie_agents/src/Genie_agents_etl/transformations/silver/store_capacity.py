"""Silver: store_capacity - one row per store x category."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("store_capacity"),
    comment="Silver store capacity - one row per store x category.",
)
@dp.expect_all({"capacity keys present": "store_code IS NOT NULL AND category IS NOT NULL"})
def store_capacity():
    return silver_transform("store_capacity")