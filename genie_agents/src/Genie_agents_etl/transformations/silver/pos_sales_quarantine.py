"""Silver: pos_sales_quarantine - rejected POS rows with the rule they failed."""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import FEED_COLUMNS, bronze_batch, silver_name, silver_table, typed

POS_COLS = FEED_COLUMNS["pos_sales"]


def _quarantined(df, rule, detail):
    return (
        df.withColumn("rule", F.lit(rule))
        .withColumn("detail", F.lit(detail))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


@dp.materialized_view(
    name=silver_name("pos_sales_quarantine"),
    comment="Silver pos_sales quarantine - one row per rejected transaction line, with the "
            "data-quality rule it failed: DUPLICATE_ROW, UNKNOWN_PRODUCT or UNKNOWN_STORE. "
            "Row counts: 703 / 399 / 0.",
)
def pos_sales_quarantine():
    pos = typed(bronze_batch("pos_sales"),
                dates=["bill_date"], ints=["qty", "discount_pct"],
                doubles=["mrp", "net_amount"])
    ranked = pos.withColumn(
        "_rn", F.row_number().over(Window.partitionBy(*POS_COLS).orderBy("txn_id")))
    once = ranked.filter("_rn = 1").drop("_rn")

    dup_rows = ranked.filter("_rn > 1").drop("_rn")
    unknown_product = once.join(silver_table("products").select("ean"), "ean", "left_anti")
    known_product = once.join(silver_table("products").select("ean"), "ean", "left_semi")
    unknown_store = known_product.join(
        silver_table("stores").select("store_code"), "store_code", "left_anti")

    return (
        _quarantined(dup_rows, "DUPLICATE_ROW", "Exact duplicate of a kept transaction line; first copy kept")
        .unionByName(_quarantined(unknown_product, "UNKNOWN_PRODUCT", "EAN missing from the product master"))
        .unionByName(_quarantined(unknown_store, "UNKNOWN_STORE", "Store code not found in the stores master"))
    )