"""Gold: fact_store_stock - stock position at the as-of date, store x product grain."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import gold_name, gold_table, spark


@dp.materialized_view(
    name=gold_name("fact_store_stock"),
    comment="Gold fact_store_stock - one row per store x product on the as-of snapshot date. "
            "Measures: stock_on_hand, in_transit_qty, wh_available (serving warehouse), "
            "depth_target, units_needed = max(0, depth_target - stock_on_hand - in_transit_qty), "
            "max_units. Flags: is_pivotal, is_on_hold + hold_reason, is_clearance.",
)
@dp.expect_all({
    "date key present": "date_key IS NOT NULL",
    "store key present": "store_key IS NOT NULL",
    "product key present": "product_key IS NOT NULL",
})
def fact_store_stock():
    return (
        spark.read.table("int_stock_flags")
        .join(gold_table("dim_date").select(
            "date_key", F.col("full_date").alias("snapshot_date")), "snapshot_date")
        .withColumn("units_needed", F.greatest(
            F.lit(0), F.col("depth_target") - F.col("stock_on_hand") - F.col("in_transit_qty")))
        .select("date_key", "store_key", "product_key", "snapshot_date", "store_code",
                "ean", "stock_on_hand", "in_transit_qty", "wh_available", "depth_target",
                "units_needed", "max_units", "is_pivotal", "is_on_hold", "hold_reason",
                "is_clearance")
    )