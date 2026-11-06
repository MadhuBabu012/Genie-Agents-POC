"""Silver: pos_sales - deduplicated, validated POS transaction lines."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import FEED_COLUMNS, bronze_batch, silver_name, silver_table, typed

POS_COLS = FEED_COLUMNS["pos_sales"]


@dp.materialized_view(
    name=silver_name("pos_sales"),
    comment="Silver POS sales - one row per transaction line. Exact duplicates deduplicated "
            "(first copy kept), FK-validated against the store and product masters. Negative qty "
 "is a genuine return and is kept (is_return); a missing discount_pct stays NULL with "
            "discount_missing = true; net_amount comes from the source feed. Rejected rows are in "
            "pos_sales_quarantine.",
)
@dp.expect_all({
    "barcode present": "ean IS NOT NULL",
    "store present": "store_code IS NOT NULL",
    "quantity nonzero": "qty <> 0",
    "net amount present": "net_amount IS NOT NULL",
})
def pos_sales():
    pos = typed(bronze_batch("pos_sales"),
                dates=["bill_date"], ints=["qty", "discount_pct"],
                doubles=["mrp", "net_amount"])
    ranked = pos.withColumn(
        "_rn", F.row_number().over(Window.partitionBy(*POS_COLS).orderBy("txn_id")))
    valid = (
        ranked.filter("_rn = 1").drop("_rn")
        .join(silver_table("products").select("ean"), "ean", "left_semi")
        .join(silver_table("stores").select("store_code"), "store_code", "left_semi")
    )
    return (
        valid.withColumn("is_return", F.col("qty") < 0)
        .withColumn("discount_missing", F.col("discount_pct").isNull())
        .select("txn_id", "bill_date", "store_code", "ean", "qty", "is_return", "mrp",
                "discount_pct", "discount_missing", "net_amount")
    )