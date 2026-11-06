"""Gold: dim_store - one row per store with a surrogate key."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import gold_name, silver_table


@dp.materialized_view(
    name=gold_name("dim_store"),
    comment="Gold store dimension - one row per store, snapshot (SCD type 1). store_key is a "
            "surrogate key assigned in store_code order.",
)
@dp.expect_all({"store key present": "store_key IS NOT NULL"})
def dim_store():
    return (
        silver_table("stores")
        .withColumn("store_key", F.row_number().over(Window.orderBy("store_code")))
        .select("store_key", "store_code", "store_name", "city", "state", "region",
                "grade", "store_format", "status", "serving_warehouse")
    )