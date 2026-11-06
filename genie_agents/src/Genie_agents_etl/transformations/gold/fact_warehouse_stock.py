"""Gold: fact_warehouse_stock - one row per day x warehouse x product."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import gold_name, gold_table, silver_table


@dp.materialized_view(
    name=gold_name("fact_warehouse_stock"),
    comment="Gold fact_warehouse_stock - one row per day x warehouse x EAN; warehouse is a "
            "degenerate dimension kept as a text column.",
)
@dp.expect_all({
    "date key present": "date_key IS NOT NULL",
    "product key present": "product_key IS NOT NULL",
})
def fact_warehouse_stock():
    return (
        silver_table("warehouse_stock_daily")
        .join(gold_table("dim_date").select(
            "date_key", F.col("full_date").alias("snapshot_date")), "snapshot_date")
        .join(gold_table("dim_product").select("product_key", "ean"), "ean")
        .select("date_key", "product_key", "snapshot_date", "warehouse", "ean", "available_qty")
    )