"""Private MV: store stock joined to gold keys and dimension attributes."""
from pyspark import pipelines as dp

from common import gold_table, silver_table


@dp.materialized_view(
    private=True,
    comment="Private: store_stock joined to gold surrogate keys and dimension attributes, "
            "for fact_store_stock.",
)
def int_stock_keys():
    return (
        silver_table("store_stock")
        .join(gold_table("dim_store").select(
            "store_key", "store_code", "grade", "serving_warehouse"), "store_code")
        .join(gold_table("dim_product").select(
            "product_key", "ean", "style_code", "category", "size_class", "is_pivotal"), "ean")
        .select("snapshot_date", "store_key", "product_key", "store_code", "ean",
                "style_code", "category", "size_class", "is_pivotal", "grade",
                "serving_warehouse", "stock_on_hand")
    )