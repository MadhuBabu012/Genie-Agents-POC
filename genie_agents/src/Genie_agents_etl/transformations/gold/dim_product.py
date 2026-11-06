"""Gold: dim_product - one row per product with a surrogate key."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import gold_name, silver_table


@dp.materialized_view(
    name=gold_name("dim_product"),
    comment="Gold product dimension - one row per product (EAN = style x colour x size), "
            "snapshot (SCD type 1). product_key is a surrogate key assigned in EAN order.",
)
@dp.expect_all({"product key present": "product_key IS NOT NULL"})
def dim_product():
    return (
        silver_table("products")
        .withColumn("product_key", F.row_number().over(Window.orderBy("ean")))
        .select("product_key", "ean", "style_code", "style_name", "category", "colour",
                "size", "size_class", "is_pivotal", "mrp", "season")
    )