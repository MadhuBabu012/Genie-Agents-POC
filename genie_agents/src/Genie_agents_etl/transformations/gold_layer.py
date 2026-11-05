# Gold layer: star schema for Genie and reporting.
#
# Dimensions:
#   dim_store        — store attributes (grain: store_code)
#   dim_product      — product / barcode attributes (grain: EAN)
#   dim_date         — calendar spine (grain: full_date)
#
# Facts:
#   fact_sales_daily     — daily POS sales aggregated by bill_date × store_code × EAN
#   fact_stock_position  — latest stock position per store × EAN with replenishment context
#
# All tables land in the gold schema via fully-qualified three-part names.
# dp.read() uses the same fully-qualified names so DLT enforces bronze→silver→gold ordering.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType, IntegerType

catalog_name  = spark.conf.get("catalog_name")
silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")

# Helper: fully-qualified silver table name for dp.read()
def _silver(table: str) -> str:
    return f"{catalog_name}.{silver_schema}.{table}"


# ============================================================
# dim_store
# ============================================================

@dp.table(
    name=f"{catalog_name}.{gold_schema}.dim_store",
    comment=(
        "Dimension: stores. "
        "Grain: one row per store_code. "
        "Sourced from silver_stores."
    ),
    table_properties={"quality": "gold"},
)
def dim_store():
    return dp.read(_silver("silver_stores"))


# ============================================================
# dim_product
# ============================================================

@dp.table(
    name=f"{catalog_name}.{gold_schema}.dim_product",
    comment=(
        "Dimension: products (barcodes). "
        "Grain: one row per EAN. "
        "pivot_category is PIVOTAL or NON_PIVOTAL. "
        "Sourced from silver_products."
    ),
    table_properties={"quality": "gold"},
)
def dim_product():
    return (
        dp.read(_silver("silver_products"))
        .withColumn(
            "pivot_category",
            F.when(F.col("is_pivotal"), F.lit("PIVOTAL")).otherwise(F.lit("NON_PIVOTAL")),
        )
    )


# ============================================================
# dim_date
# ============================================================

@dp.table(
    name=f"{catalog_name}.{gold_schema}.dim_date",
    comment=(
        "Dimension: calendar dates. "
        "Grain: one row per calendar date. "
        "Covers 2026-01-01 to 2027-12-31."
    ),
    table_properties={"quality": "gold"},
)
def dim_date():
    return (
        spark.sql(
            "SELECT explode(sequence(date'2026-01-01', date'2027-12-31', interval 1 day)) AS full_date"
        )
        .withColumn("date_key",     F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year",         F.year("full_date"))
        .withColumn("quarter",      F.quarter("full_date"))
        .withColumn("month_num",    F.month("full_date"))
        .withColumn("month_name",   F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_week",  F.dayofweek("full_date"))   # 1=Sun … 7=Sat
        .withColumn("day_name",     F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",   F.dayofweek("full_date").isin(1, 7))
    )


# ============================================================
# fact_sales_daily
# ============================================================

@dp.table(
    name=f"{catalog_name}.{gold_schema}.fact_sales_daily",
    comment=(
        "Fact: daily POS sales. "
        "Grain: one row per bill_date × store_code × EAN. "
        "Negative-qty rows counted as returns. "
        "Joins to dim_date on bill_date, dim_store on store_code, dim_product on EAN. "
        "Sourced from silver_pos_sales."
    ),
    table_properties={"quality": "gold"},
)
def fact_sales_daily():
    return (
        dp.read(_silver("silver_pos_sales"))
        .groupBy("bill_date", "store_code", "ean")
        .agg(
            F.sum(F.when(F.col("qty") > 0, F.col("qty")).otherwise(F.lit(0)))
             .cast(IntegerType()).alias("gross_units_sold"),
            F.sum(F.when(F.col("qty") < 0, F.abs(F.col("qty"))).otherwise(F.lit(0)))
             .cast(IntegerType()).alias("return_units"),
            F.sum("qty").cast(IntegerType()).alias("net_units"),
            F.sum(F.when(F.col("qty") > 0, F.col("net_amount")).otherwise(F.lit(0)))
             .alias("gross_revenue"),
            F.sum(F.when(F.col("qty") < 0, F.abs(F.col("net_amount"))).otherwise(F.lit(0)))
             .alias("return_amount"),
            F.sum("net_amount").alias("net_revenue"),
            F.round(F.avg(F.when(F.col("qty") > 0, F.col("discount_pct"))), 2)
             .alias("avg_discount_pct"),
            F.count("txn_id").cast(IntegerType()).alias("transaction_count"),
        )
    )


# ============================================================
# fact_stock_position
# ============================================================

@dp.table(
    name=f"{catalog_name}.{gold_schema}.fact_stock_position",
    comment=(
        "Fact: stock position as of the latest store_stock snapshot. "
        "Grain: one row per store_code × EAN. "
        "Joins to dim_store on store_code, dim_product on EAN. "
        "Columns: snapshot_date, stock_on_hand, in_transit_qty, effective_inventory, "
        "depth_target, units_needed, is_on_hold, is_clearance. "
        "Sourced from silver_store_stock, silver_in_transit, silver_products, "
        "silver_stores, silver_depth_targets, silver_style_holds, silver_clearance_styles."
    ),
    table_properties={"quality": "gold"},
)
def fact_stock_position():
    # --- latest snapshot per store × EAN ---
    w_latest = Window.partitionBy("store_code", "ean").orderBy(F.col("snapshot_date").desc())
    stock = (
        dp.read(_silver("silver_store_stock"))
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_ingested_at", "_source_file")
    )

    # --- in-transit quantity per store × EAN ---
    transit = (
        dp.read(_silver("silver_in_transit"))
        .groupBy("store_code", "ean")
        .agg(F.sum("qty").cast(IntegerType()).alias("in_transit_qty"))
    )

    # --- product attributes ---
    products = (
        dp.read(_silver("silver_products"))
        .select(
            "ean", "style_code", "category", "is_pivotal",
            F.when(F.col("is_pivotal"), F.lit("PIVOTAL"))
             .otherwise(F.lit("NON_PIVOTAL")).alias("pivot_category"),
        )
    )

    # --- store attributes ---
    stores = (
        dp.read(_silver("silver_stores"))
        .select("store_code", "grade", "region", "state", "status", "serving_warehouse")
    )

    # --- depth targets (grade × PIVOTAL/NON_PIVOTAL) ---
    depth = (
        dp.read(_silver("silver_depth_targets"))
        .select(
            F.col("grade").alias("dt_grade"),
            F.col("size_class").alias("dt_size_class"),
            "depth_target",
        )
    )

    # --- style-level flags ---
    holds = (
        dp.read(_silver("silver_style_holds"))
        .select("style_code")
        .withColumn("is_on_hold", F.lit(True))
    )
    clearance = (
        dp.read(_silver("silver_clearance_styles"))
        .select("style_code")
        .withColumn("is_clearance", F.lit(True))
    )

    return (
        stock
        .join(transit,  ["store_code", "ean"], "left")
        .join(products, ["ean"],               "left")
        .join(stores,   ["store_code"],        "left")
        .join(
            depth,
            (F.col("grade") == F.col("dt_grade")) &
            (F.col("pivot_category") == F.col("dt_size_class")),
            "left",
        )
        .drop("dt_grade", "dt_size_class")
        .join(holds,     ["style_code"], "left")
        .join(clearance, ["style_code"], "left")
        .withColumn("in_transit_qty",
            F.coalesce(F.col("in_transit_qty"), F.lit(0)))
        .withColumn("effective_inventory",
            F.col("stock_on_hand") + F.col("in_transit_qty"))
        .withColumn("units_needed",
            F.greatest(F.lit(0), F.col("depth_target") - F.col("effective_inventory")))
        .withColumn("is_on_hold",
            F.coalesce(F.col("is_on_hold"), F.lit(False).cast(BooleanType())))
        .withColumn("is_clearance",
            F.coalesce(F.col("is_clearance"), F.lit(False).cast(BooleanType())))
        .select(
            "snapshot_date",
            "store_code", "ean", "style_code", "category",
            "is_pivotal", "pivot_category",
            "stock_on_hand", "in_transit_qty", "effective_inventory",
            "depth_target", "units_needed",
            "grade", "region", "state", "status", "serving_warehouse",
            "is_on_hold", "is_clearance",
        )
    )
