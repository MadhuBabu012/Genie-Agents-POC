"""Private MV: stock rows enriched with in-transit and warehouse availability."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import spark


@dp.materialized_view(
    private=True,
    comment="Private: store stock enriched with in-transit units per store x product and "
            "serving-warehouse availability at the latest snapshot, for fact_store_stock.",
)
def int_stock_movement():
    return (
        spark.read.table("int_stock_rules")
        .join(spark.read.table("int_in_transit_units"), ["store_code", "ean"], "left")
        .join(spark.read.table("int_warehouse_stock_asof"), ["serving_warehouse", "ean"], "left")
        .select("snapshot_date", "store_key", "product_key", "store_code", "ean",
                "style_code", "is_pivotal", "stock_on_hand", "depth_target", "max_units",
                F.coalesce("in_transit_qty", F.lit(0)).alias("in_transit_qty"),
                F.coalesce("available_qty", F.lit(0)).alias("wh_available"))
    )