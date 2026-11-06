"""Private data-quality check: pos_sales reconciliation across layers.

Every rejected bronze row must land in the quarantine table, and the rule counts
must sum to the quarantine total: bronze = silver + quarantine.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import bronze_batch, silver_table


@dp.materialized_view(
    private=True,
    refresh_policy="incremental",
    comment="Row-count reconciliation for pos_sales: bronze = silver + quarantine. "
            "Expected 136,169 = 135,067 + 1,102 (703 DUPLICATE_ROW / 399 UNKNOWN_PRODUCT / "
            "0 UNKNOWN_STORE), plus kept returns.",
)
@dp.expect("bronze rows equal silver rows plus quarantined rows",
           "bronze_rows = silver_rows + quarantined_rows")
@dp.expect("quarantine rule counts sum to the quarantine total",
           "duplicate_rows + unknown_product_rows + unknown_store_rows = quarantined_rows")
@dp.expect("returns are kept in silver, not quarantined", "return_rows > 0")
def pos_sales_reconciliation_check():
    bronze = bronze_batch("pos_sales").agg(F.count(F.lit(1)).alias("bronze_rows"))
    silver = silver_table("pos_sales").agg(
        F.count(F.lit(1)).alias("silver_rows"),
        F.count(F.when(F.col("is_return"), 1)).alias("return_rows"))
    quarantine = silver_table("pos_sales_quarantine").agg(
        F.count(F.lit(1)).alias("quarantined_rows"),
        F.count(F.when(F.col("rule") == "DUPLICATE_ROW", 1)).alias("duplicate_rows"),
        F.count(F.when(F.col("rule") == "UNKNOWN_PRODUCT", 1)).alias("unknown_product_rows"),
        F.count(F.when(F.col("rule") == "UNKNOWN_STORE", 1)).alias("unknown_store_rows"))
    one = F.lit(1)
    return (
        bronze.withColumn("k", one)
        .join(silver.withColumn("k", one), "k")
        .join(quarantine.withColumn("k", one), "k")
        .drop("k")
    )