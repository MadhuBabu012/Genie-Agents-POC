"""Silver: stores - one row per store, keys trimmed."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("stores"),
    comment="Silver stores - one row per store. Keys trimmed, no other changes.",
)
@dp.expect_all({"store code present": "store_code IS NOT NULL"})
def stores():
    return silver_transform("stores")