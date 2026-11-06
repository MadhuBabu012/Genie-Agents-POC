"""Gold: fact_sales - daily sales at store x product x day grain."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import gold_name, gold_table, silver_table


@dp.materialized_view(
    name=gold_name("fact_sales"),
    comment="Gold fact_sales - one row per store x product x day. Measures: units_sold, "
            "units_returned, gross_amount, discount_amount, net_amount, txn_line_count. Returns "
            "count in units_returned and reduce net_amount.",
)
@dp.expect_all({
    "date key present": "date_key IS NOT NULL",
    "store key present": "store_key IS NOT NULL",
    "product key present": "product_key IS NOT NULL",
})
def fact_sales():
    measures = [
        F.coalesce(F.sum(F.when(F.col("qty") > 0, F.col("qty"))), F.lit(0)).cast("int").alias("units_sold"),
        F.coalesce(F.sum(F.when(F.col("qty") < 0, -F.col("qty"))), F.lit(0)).cast("int").alias("units_returned"),
        F.coalesce(F.sum(F.when(F.col("qty") > 0, F.col("qty") * F.col("mrp"))), F.lit(0.0)).alias("gross_amount"),
        F.coalesce(
            F.sum(F.when(F.col("qty") > 0, F.col("qty") * F.col("mrp") * F.col("discount_pct") / 100.0)),
            F.lit(0.0)).alias("discount_amount"),
        F.coalesce(F.sum("net_amount"), F.lit(0.0)).alias("net_amount"),
        F.count(F.lit(1)).alias("txn_line_count"),
    ]
    return (
        silver_table("pos_sales")
        .groupBy("bill_date", "store_code", "ean")
        .agg(*measures)
        .join(gold_table("dim_date").select("date_key", F.col("full_date").alias("bill_date")), "bill_date")
        .join(gold_table("dim_store").select("store_key", "store_code"), "store_code")
        .join(gold_table("dim_product").select("product_key", "ean"), "ean")
        .select("date_key", "store_key", "product_key", "bill_date", "store_code", "ean",
                "units_sold", "units_returned", "gross_amount", "discount_amount",
                "net_amount", "txn_line_count")
    )