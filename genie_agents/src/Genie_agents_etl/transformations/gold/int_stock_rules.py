"""Private MV: stock rows enriched with depth target and category capacity."""
from pyspark import pipelines as dp

from common import spark, silver_table


@dp.materialized_view(
    private=True,
    comment="Private: store stock enriched with depth_target (grade x size_class) and "
            "max_units (store x category), for fact_store_stock.",
)
def int_stock_rules():
    return (
        spark.read.table("int_stock_keys")
        .join(silver_table("depth_targets"), ["grade", "size_class"])
        .join(silver_table("store_capacity"), ["store_code", "category"])
        .select("snapshot_date", "store_key", "product_key", "store_code", "ean",
                "style_code", "is_pivotal", "serving_warehouse", "stock_on_hand",
                "depth_target", "max_units")
    )