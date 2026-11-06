"""Silver: in_transit - one row per transfer dispatched to a store."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("in_transit"),
    comment="Silver in-transit - one row per transfer dispatched to a store, not yet received.",
)
@dp.expect_all({"transfer id present": "transfer_id IS NOT NULL"})
def in_transit():
    return silver_transform("in_transit")