"""Gold: fact_replenishment_decisions - one row per store x product for the run date."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import gold_name, gold_table, silver_table


@dp.materialized_view(
    name=gold_name("fact_replenishment_decisions"),
    comment="Gold fact_replenishment_decisions - one row per store x EAN for the run date. "
            "decision, units_needed, order_qty from the engine; source_location names the "
            "warehouse or donor store.",
)
@dp.expect_all({
    "date key present": "date_key IS NOT NULL",
    "store key present": "store_key IS NOT NULL",
    "product key present": "product_key IS NOT NULL",
})
def fact_replenishment_decisions():
    return (
        silver_table("replenishment_decisions")
        .join(gold_table("dim_date").select(
            "date_key", F.col("full_date").alias("run_date")), "run_date")
        .join(gold_table("dim_store").select("store_key", "store_code"), "store_code")
        .join(gold_table("dim_product").select("product_key", "ean"), "ean")
        .select("date_key", "store_key", "product_key", "run_date", "store_code", "ean",
                "decision", "units_needed", "order_qty", "source_location")
    )