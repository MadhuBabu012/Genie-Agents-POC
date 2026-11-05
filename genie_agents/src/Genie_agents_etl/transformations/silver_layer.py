# Silver layer: type casting, key trimming, deduplication, quarantine.
#
# Every staged function applies three steps:
#   1. Cast & trim  — proper types, whitespace removed from key columns
#   2. Validation   — first failing rule written to _quarantine_rule
#   3. Dedup        — for valid rows only, rank by _ingested_at desc;
#                     rows with _row_num > 1 are flagged as duplicates
#                     and also routed to quarantine
#
# Reconciliation guarantee:
#   bronze_<feed>.count() == silver_<feed>.count() + silver_<feed>_quarantine.count()
#
# Schema routing: tables land in the silver schema via fully-qualified three-part names
# (catalog.schema.table) because schema= on @dp.table means column DDL schema, not UC schema.
# Each feed has its own quarantine table: silver_<feed>_quarantine (also in silver schema).

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, StringType,
)

# ------------------------------------------------------------------ config --

catalog_name  = spark.conf.get("catalog_name")
silver_schema = spark.conf.get("silver_schema")

# ---------------------------------------------------------------- helpers ---

def _to_quarantine(df, feed, data_cols):
    """Shape bad rows (invalid OR duplicate) into the per-feed quarantine schema."""
    return (
        df.filter(F.col("_quarantine_rule").isNotNull())
        .select(
            F.lit(feed).alias("feed_name"),
            F.col("_quarantine_rule").alias("quarantine_rule"),
            F.to_json(F.struct(*data_cols)).alias("raw_data"),
            F.col("_ingested_at"),
            F.col("_source_file"),
        )
    )


def _add_dedup_flag(df, dedup_keys, dup_rule_name):
    """
    For rows where _quarantine_rule IS NULL, rank by _ingested_at desc.
    Rows ranked > 1 (duplicates) get _quarantine_rule = dup_rule_name.
    _row_num is left on the df so callers can filter on it.
    """
    w = Window.partitionBy(*dedup_keys).orderBy(F.col("_ingested_at").desc())
    df = df.withColumn(
        "_row_num",
        F.when(F.col("_quarantine_rule").isNull(), F.row_number().over(w))
         .otherwise(F.lit(None).cast(IntegerType())),
    )
    return df.withColumn(
        "_quarantine_rule",
        F.when(F.col("_row_num") > 1, F.lit(dup_rule_name))
         .otherwise(F.col("_quarantine_rule")),
    )


def _clean(df):
    """Return only valid, non-duplicate rows with housekeeping columns dropped."""
    return df.filter(F.col("_quarantine_rule").isNull()).drop("_quarantine_rule", "_row_num")


# ============================================================
# STORES  (grain: store_code)
# ============================================================

def _staged_stores():
    df = dp.read("bronze_stores").select(
        F.trim("store_code").alias("store_code"),
        F.trim("store_name").alias("store_name"),
        F.trim("city").alias("city"),
        F.trim("state").alias("state"),
        F.trim("region").alias("region"),
        F.trim("grade").alias("grade"),
        F.trim("store_format").alias("store_format"),
        F.trim("status").alias("status"),
        F.trim("serving_warehouse").alias("serving_warehouse"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("store_code").isNull() | (F.col("store_code") == ""),
               F.lit("store_code_not_null"))
        .when(~F.col("status").isin("Open", "Hold", "Closed"),
              F.lit("invalid_status"))
        .when(~F.col("grade").isin("A", "B", "C"),
              F.lit("invalid_grade"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["store_code"], "duplicate_store_code")


_STORES_COLS = [
    "store_code", "store_name", "city", "state", "region",
    "grade", "store_format", "status", "serving_warehouse",
]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_stores",
    comment=(
        "Silver: stores. Grain: one row per store_code. "
        "Trimmed keys; invalid status/grade and duplicate store_codes quarantined."
    ),
    table_properties={"quality": "silver"},
)
def silver_stores():
    return _clean(_staged_stores())


# ============================================================
# PRODUCTS  (grain: ean)
# ============================================================

def _staged_products():
    df = dp.read("bronze_products").select(
        F.trim("ean").alias("ean"),
        F.trim("style_code").alias("style_code"),
        F.trim("style_name").alias("style_name"),
        F.trim("category").alias("category"),
        F.trim("colour").alias("colour"),
        F.trim("size").alias("size"),
        F.trim(F.upper(F.col("size_class"))).alias("size_class"),
        F.when(F.lower(F.trim(F.col("is_pivotal"))) == "true", F.lit(True))
         .when(F.lower(F.trim(F.col("is_pivotal"))) == "false", F.lit(False))
         .otherwise(F.lit(None).cast(BooleanType())).alias("is_pivotal"),
        F.col("mrp").cast(DecimalType(12, 2)).alias("mrp"),
        F.trim("season").alias("season"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("ean").isNull() | (F.col("ean") == ""),
               F.lit("ean_not_null"))
        .when(F.col("mrp").isNull() | (F.col("mrp") <= 0),
              F.lit("mrp_not_positive"))
        .when(F.col("is_pivotal").isNull(),
              F.lit("is_pivotal_not_null"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["ean"], "duplicate_ean")


_PRODUCTS_COLS = [
    "ean", "style_code", "style_name", "category", "colour",
    "size", "size_class", "is_pivotal", "mrp", "season",
]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_products",
    comment=(
        "Silver: products (barcodes). Grain: one row per EAN. "
        "is_pivotal cast to boolean; mrp cast to decimal(12,2)."
    ),
    table_properties={"quality": "silver"},
)
def silver_products():
    return _clean(_staged_products())


# ============================================================
# POS SALES  (grain: txn_id — negative qty = return)
# ============================================================

def _staged_pos_sales():
    df = dp.read("bronze_pos_sales").select(
        F.trim("txn_id").alias("txn_id"),
        F.col("bill_date").cast(DateType()).alias("bill_date"),
        F.trim("store_code").alias("store_code"),
        F.trim("ean").alias("ean"),
        F.col("qty").cast(IntegerType()).alias("qty"),
        F.col("mrp").cast(DecimalType(12, 2)).alias("mrp"),
        F.col("discount_pct").cast(DecimalType(5, 2)).alias("discount_pct"),
        F.col("net_amount").cast(DecimalType(14, 2)).alias("net_amount"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("txn_id").isNull() | (F.col("txn_id") == ""),
               F.lit("txn_id_not_null"))
        .when(F.col("store_code").isNull() | (F.col("store_code") == ""),
              F.lit("store_code_not_null"))
        .when(F.col("ean").isNull() | (F.col("ean") == ""),
              F.lit("ean_not_null"))
        .when(F.col("bill_date").isNull(),
              F.lit("bill_date_invalid"))
        .when(F.col("qty").isNull() | (F.col("qty") == 0),
              F.lit("qty_null_or_zero"))
        .when(F.col("net_amount").isNull(),
              F.lit("net_amount_null"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    # Duplicate txn_id rows are injected as deliberate DQ noise by the generator
    return _add_dedup_flag(df, ["txn_id"], "duplicate_txn_id")


_POS_SALES_COLS = [
    "txn_id", "bill_date", "store_code", "ean",
    "qty", "mrp", "discount_pct", "net_amount",
]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_pos_sales",
    comment=(
        "Silver: POS sales. Grain: one row per txn_id. "
        "Negative qty = customer return (valid business event, not quarantined). "
        "Duplicate txn_ids are quarantined with rule duplicate_txn_id."
    ),
    table_properties={"quality": "silver"},
)
def silver_pos_sales():
    return _clean(_staged_pos_sales())


# ============================================================
# WAREHOUSE STOCK DAILY  (grain: snapshot_date × warehouse × ean)
# ============================================================

def _staged_warehouse_stock():
    df = dp.read("bronze_warehouse_stock_daily").select(
        F.col("snapshot_date").cast(DateType()).alias("snapshot_date"),
        F.trim("warehouse").alias("warehouse"),
        F.trim("ean").alias("ean"),
        F.col("available_qty").cast(IntegerType()).alias("available_qty"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("snapshot_date").isNull(), F.lit("snapshot_date_invalid"))
        .when(F.col("warehouse").isNull() | (F.col("warehouse") == ""),
              F.lit("warehouse_not_null"))
        .when(F.col("ean").isNull() | (F.col("ean") == ""),
              F.lit("ean_not_null"))
        .when(F.col("available_qty").isNull() | (F.col("available_qty") < 0),
              F.lit("available_qty_negative"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["snapshot_date", "warehouse", "ean"],
                           "duplicate_snapshot_warehouse_ean")


_WH_STOCK_COLS = ["snapshot_date", "warehouse", "ean", "available_qty"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_warehouse_stock_daily",
    comment=(
        "Silver: warehouse stock snapshot. "
        "Grain: one row per snapshot_date × warehouse × EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_warehouse_stock_daily():
    return _clean(_staged_warehouse_stock())


# ============================================================
# STORE STOCK  (grain: snapshot_date × store_code × ean)
# ============================================================

def _staged_store_stock():
    df = dp.read("bronze_store_stock").select(
        F.col("snapshot_date").cast(DateType()).alias("snapshot_date"),
        F.trim("store_code").alias("store_code"),
        F.trim("ean").alias("ean"),
        F.col("stock_on_hand").cast(IntegerType()).alias("stock_on_hand"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("snapshot_date").isNull(), F.lit("snapshot_date_invalid"))
        .when(F.col("store_code").isNull() | (F.col("store_code") == ""),
              F.lit("store_code_not_null"))
        .when(F.col("ean").isNull() | (F.col("ean") == ""),
              F.lit("ean_not_null"))
        .when(F.col("stock_on_hand").isNull() | (F.col("stock_on_hand") < 0),
              F.lit("stock_on_hand_negative"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["snapshot_date", "store_code", "ean"],
                           "duplicate_snapshot_store_ean")


_STORE_STOCK_COLS = ["snapshot_date", "store_code", "ean", "stock_on_hand"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_store_stock",
    comment=(
        "Silver: store stock snapshot. "
        "Grain: one row per snapshot_date × store_code × EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_store_stock():
    return _clean(_staged_store_stock())


# ============================================================
# IN TRANSIT  (grain: transfer_id)
# ============================================================

def _staged_in_transit():
    df = dp.read("bronze_in_transit").select(
        F.trim("transfer_id").alias("transfer_id"),
        F.col("dispatch_date").cast(DateType()).alias("dispatch_date"),
        F.trim("store_code").alias("store_code"),
        F.trim("ean").alias("ean"),
        F.col("qty").cast(IntegerType()).alias("qty"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("transfer_id").isNull() | (F.col("transfer_id") == ""),
               F.lit("transfer_id_not_null"))
        .when(F.col("store_code").isNull() | (F.col("store_code") == ""),
              F.lit("store_code_not_null"))
        .when(F.col("ean").isNull() | (F.col("ean") == ""),
              F.lit("ean_not_null"))
        .when(F.col("dispatch_date").isNull(), F.lit("dispatch_date_invalid"))
        .when(F.col("qty").isNull() | (F.col("qty") <= 0),
              F.lit("qty_not_positive"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["transfer_id"], "duplicate_transfer_id")


_IN_TRANSIT_COLS = ["transfer_id", "dispatch_date", "store_code", "ean", "qty"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_in_transit",
    comment=(
        "Silver: stock in transit to stores. "
        "Grain: one row per transfer_id."
    ),
    table_properties={"quality": "silver"},
)
def silver_in_transit():
    return _clean(_staged_in_transit())


# ============================================================
# STYLE HOLDS  (grain: style_code)
# ============================================================

def _staged_style_holds():
    df = dp.read("bronze_style_holds").select(
        F.trim("style_code").alias("style_code"),
        F.trim("hold_reason").alias("hold_reason"),
        F.col("hold_from").cast(DateType()).alias("hold_from"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("style_code").isNull() | (F.col("style_code") == ""),
               F.lit("style_code_not_null"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["style_code"], "duplicate_style_code")


_STYLE_HOLDS_COLS = ["style_code", "hold_reason", "hold_from"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_style_holds",
    comment=(
        "Silver: styles blocked from replenishment. "
        "Grain: one row per style_code."
    ),
    table_properties={"quality": "silver"},
)
def silver_style_holds():
    return _clean(_staged_style_holds())


# ============================================================
# CLEARANCE STYLES  (grain: style_code)
# ============================================================

def _staged_clearance_styles():
    df = dp.read("bronze_clearance_styles").select(
        F.trim("style_code").alias("style_code"),
        F.col("clearance_from").cast(DateType()).alias("clearance_from"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("style_code").isNull() | (F.col("style_code") == ""),
               F.lit("style_code_not_null"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["style_code"], "duplicate_style_code")


_CLEARANCE_COLS = ["style_code", "clearance_from"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_clearance_styles",
    comment=(
        "Silver: end-of-season styles on clearance. "
        "Grain: one row per style_code."
    ),
    table_properties={"quality": "silver"},
)
def silver_clearance_styles():
    return _clean(_staged_clearance_styles())


# ============================================================
# STORE CAPACITY  (grain: store_code × category)
# ============================================================

def _staged_store_capacity():
    df = dp.read("bronze_store_capacity").select(
        F.trim("store_code").alias("store_code"),
        F.trim("category").alias("category"),
        F.col("max_units").cast(IntegerType()).alias("max_units"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("store_code").isNull() | (F.col("store_code") == ""),
               F.lit("store_code_not_null"))
        .when(F.col("category").isNull() | (F.col("category") == ""),
              F.lit("category_not_null"))
        .when(F.col("max_units").isNull() | (F.col("max_units") <= 0),
              F.lit("max_units_not_positive"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["store_code", "category"], "duplicate_store_category")


_CAPACITY_COLS = ["store_code", "category", "max_units"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_store_capacity",
    comment=(
        "Silver: maximum units a store can hold per category. "
        "Grain: one row per store_code × category."
    ),
    table_properties={"quality": "silver"},
)
def silver_store_capacity():
    return _clean(_staged_store_capacity())


# ============================================================
# DEPTH TARGETS  (grain: grade × size_class  — PIVOTAL | NON_PIVOTAL)
# ============================================================

def _staged_depth_targets():
    df = dp.read("bronze_depth_targets").select(
        F.trim(F.upper(F.col("grade"))).alias("grade"),
        F.trim(F.upper(F.col("size_class"))).alias("size_class"),
        F.col("depth_target").cast(IntegerType()).alias("depth_target"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("grade").isNull() | (F.col("grade") == ""),
               F.lit("grade_not_null"))
        .when(~F.col("size_class").isin("PIVOTAL", "NON_PIVOTAL"),
              F.lit("invalid_size_class"))
        .when(F.col("depth_target").isNull() | (F.col("depth_target") <= 0),
              F.lit("depth_target_not_positive"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["grade", "size_class"], "duplicate_grade_size_class")


_DEPTH_COLS = ["grade", "size_class", "depth_target"]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_depth_targets",
    comment=(
        "Silver: replenishment depth targets. "
        "Grain: one row per grade × size_class (PIVOTAL or NON_PIVOTAL). "
        "A/PIVOTAL=3, A/NON_PIVOTAL=2, B/PIVOTAL=2, B/NON_PIVOTAL=1, C/PIVOTAL=1, C/NON_PIVOTAL=1."
    ),
    table_properties={"quality": "silver"},
)
def silver_depth_targets():
    return _clean(_staged_depth_targets())


# ============================================================
# REPLENISHMENT DECISIONS  (grain: run_date × store_code × ean)
# ============================================================

def _staged_replenishment():
    df = dp.read("bronze_replenishment_decisions").select(
        F.col("run_date").cast(DateType()).alias("run_date"),
        F.trim("store_code").alias("store_code"),
        F.trim("ean").alias("ean"),
        F.trim("decision").alias("decision"),
        F.col("units_needed").cast(IntegerType()).alias("units_needed"),
        F.col("order_qty").cast(IntegerType()).alias("order_qty"),
        F.trim("source_location").alias("source_location"),
        "_ingested_at", "_source_file",
    ).withColumn("_quarantine_rule",
        F.when(F.col("run_date").isNull(), F.lit("run_date_invalid"))
        .when(F.col("store_code").isNull() | (F.col("store_code") == ""),
              F.lit("store_code_not_null"))
        .when(F.col("ean").isNull() | (F.col("ean") == ""),
              F.lit("ean_not_null"))
        .when(F.col("decision").isNull() | (F.col("decision") == ""),
              F.lit("decision_not_null"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    return _add_dedup_flag(df, ["run_date", "store_code", "ean"],
                           "duplicate_run_date_store_ean")


_REPLEN_COLS = [
    "run_date", "store_code", "ean", "decision",
    "units_needed", "order_qty", "source_location",
]


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_replenishment_decisions",
    comment=(
        "Silver: daily replenishment engine output. "
        "Grain: one row per run_date × store_code × EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_replenishment_decisions():
    return _clean(_staged_replenishment())


# ============================================================
# PER-FEED QUARANTINE TABLES  (one table per silver feed)
# ============================================================
# Reconciliation: bronze_<feed>.count()
#               == silver_<feed>.count() + silver_<feed>_quarantine.count()

@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_stores_quarantine",
    comment=(
        "Quarantine for stores feed. "
        "Captures rows with invalid status/grade or duplicate store_code."
    ),
    table_properties={"quality": "silver"},
)
def silver_stores_quarantine():
    return _to_quarantine(_staged_stores(), "stores", _STORES_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_products_quarantine",
    comment=(
        "Quarantine for products feed. "
        "Captures rows with null/invalid EAN, non-positive MRP, or duplicate EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_products_quarantine():
    return _to_quarantine(_staged_products(), "products", _PRODUCTS_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_pos_sales_quarantine",
    comment=(
        "Quarantine for POS sales feed. "
        "Captures rows with missing txn_id/store_code/EAN/bill_date, zero qty, "
        "null net_amount, or duplicate txn_id."
    ),
    table_properties={"quality": "silver"},
)
def silver_pos_sales_quarantine():
    return _to_quarantine(_staged_pos_sales(), "pos_sales", _POS_SALES_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_warehouse_stock_daily_quarantine",
    comment=(
        "Quarantine for warehouse_stock_daily feed. "
        "Captures rows with invalid snapshot_date, missing warehouse/EAN, "
        "negative available_qty, or duplicate snapshot×warehouse×EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_warehouse_stock_daily_quarantine():
    return _to_quarantine(_staged_warehouse_stock(), "warehouse_stock_daily", _WH_STOCK_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_store_stock_quarantine",
    comment=(
        "Quarantine for store_stock feed. "
        "Captures rows with invalid snapshot_date, missing store_code/EAN, "
        "negative stock_on_hand, or duplicate snapshot×store×EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_store_stock_quarantine():
    return _to_quarantine(_staged_store_stock(), "store_stock", _STORE_STOCK_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_in_transit_quarantine",
    comment=(
        "Quarantine for in_transit feed. "
        "Captures rows with missing transfer_id/store_code/EAN, "
        "invalid dispatch_date, non-positive qty, or duplicate transfer_id."
    ),
    table_properties={"quality": "silver"},
)
def silver_in_transit_quarantine():
    return _to_quarantine(_staged_in_transit(), "in_transit", _IN_TRANSIT_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_style_holds_quarantine",
    comment=(
        "Quarantine for style_holds feed. "
        "Captures rows with missing style_code or duplicate style_code."
    ),
    table_properties={"quality": "silver"},
)
def silver_style_holds_quarantine():
    return _to_quarantine(_staged_style_holds(), "style_holds", _STYLE_HOLDS_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_clearance_styles_quarantine",
    comment=(
        "Quarantine for clearance_styles feed. "
        "Captures rows with missing style_code or duplicate style_code."
    ),
    table_properties={"quality": "silver"},
)
def silver_clearance_styles_quarantine():
    return _to_quarantine(_staged_clearance_styles(), "clearance_styles", _CLEARANCE_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_store_capacity_quarantine",
    comment=(
        "Quarantine for store_capacity feed. "
        "Captures rows with missing store_code/category, "
        "non-positive max_units, or duplicate store×category."
    ),
    table_properties={"quality": "silver"},
)
def silver_store_capacity_quarantine():
    return _to_quarantine(_staged_store_capacity(), "store_capacity", _CAPACITY_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_depth_targets_quarantine",
    comment=(
        "Quarantine for depth_targets feed. "
        "Captures rows with missing grade, invalid size_class, "
        "non-positive depth_target, or duplicate grade×size_class."
    ),
    table_properties={"quality": "silver"},
)
def silver_depth_targets_quarantine():
    return _to_quarantine(_staged_depth_targets(), "depth_targets", _DEPTH_COLS)


@dp.table(
    name=f"{catalog_name}.{silver_schema}.silver_replenishment_decisions_quarantine",
    comment=(
        "Quarantine for replenishment_decisions feed. "
        "Captures rows with invalid run_date, missing store_code/EAN/decision, "
        "or duplicate run_date×store×EAN."
    ),
    table_properties={"quality": "silver"},
)
def silver_replenishment_decisions_quarantine():
    return _to_quarantine(_staged_replenishment(), "replenishment_decisions", _REPLEN_COLS)
