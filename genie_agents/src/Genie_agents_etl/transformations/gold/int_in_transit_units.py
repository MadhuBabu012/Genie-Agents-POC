"""Private MV: units in transit per store x product."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import silver_table


@dp.materialized_view(
    private=True,
    comment="Private: in-transit units summed per store x product, for fact_store_stock.",
)
def int_in_transit_units():
    return (
        silver_table("in_transit")
        .groupBy("store_code", "ean")
        .agg(F.sum("qty").cast("int").alias("in_transit_qty"))
    )