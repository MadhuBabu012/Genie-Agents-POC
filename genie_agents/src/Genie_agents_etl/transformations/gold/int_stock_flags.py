"""Private MV: stock rows enriched with style hold and clearance flags."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import silver_table, spark


@dp.materialized_view(
    private=True,
    comment="Private: store stock enriched with is_on_hold / hold_reason and is_clearance "
            "per style, for fact_store_stock.",
)
def int_stock_flags():
    holds = silver_table("style_holds").select(
        "style_code", "hold_reason", F.lit(True).alias("is_on_hold"))
    clearance = silver_table("clearance_styles").select(
        "style_code", F.lit(True).alias("is_clearance"))
    return (
        spark.read.table("int_stock_movement")
        .join(holds, "style_code", "left")
        .join(clearance, "style_code", "left")
        .withColumn("is_on_hold", F.coalesce("is_on_hold", F.lit(False)))
        .withColumn("is_clearance", F.coalesce("is_clearance", F.lit(False)))
        .select("snapshot_date", "store_key", "product_key", "store_code", "ean",
                "is_pivotal", "stock_on_hand", "in_transit_qty", "wh_available",
                "depth_target", "max_units", "is_on_hold", "hold_reason", "is_clearance")
    )