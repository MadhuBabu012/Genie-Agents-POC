"""Private MV: warehouse stock at the latest snapshot date."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import silver_table


@dp.materialized_view(
    private=True,
    comment="Private: warehouse stock as of the latest snapshot_date, keyed by serving "
            "warehouse x product, for fact_store_stock.",
)
def int_warehouse_stock_asof():
    wh = silver_table("warehouse_stock_daily")
    latest = wh.agg(F.max("snapshot_date").alias("snapshot_date"))
    return (
        wh.join(latest, "snapshot_date")
        .select(F.col("warehouse").alias("serving_warehouse"), "ean", "available_qty")
    )