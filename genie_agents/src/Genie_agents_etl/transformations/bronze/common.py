"""Shared configuration and helpers for the retail medallion pipeline.

Lives at the pipeline root so every transformation file can `from common import ...`.

Catalogs and schemas are pipeline configuration parameters (Settings > Configuration),
so the source and target can be changed without touching any code:

  source_catalog / source_schema  -> landing volume location
  target_catalog + bronze_schema / silver_schema / gold_schema -> published tables

The hidden answer key schema (source_catalog + `_eval`) is never referenced here.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()


def _conf(key, default):
    return spark.conf.get(key, default)


SOURCE_CATALOG = _conf("source_catalog", "genie_agents_poc")
SOURCE_SCHEMA = _conf("source_schema", "bronze")
TARGET_CATALOG = _conf("target_catalog", "genie_agents_poc")
BRONZE_SCHEMA = _conf("bronze_schema", "bronze")
SILVER_SCHEMA = _conf("silver_schema", "silver")
GOLD_SCHEMA = _conf("gold_schema", "gold")

LANDING = f"/Volumes/{SOURCE_CATALOG}/{SOURCE_SCHEMA}/landing"

# feed -> business columns in landing-file order (kept as text in bronze)
FEED_COLUMNS = {
    "stores": ["store_code", "store_name", "city", "state", "region", "grade",
               "store_format", "status", "serving_warehouse"],
    "products": ["ean", "style_code", "style_name", "category", "colour", "size",
                 "size_class", "is_pivotal", "mrp", "season"],
    "pos_sales": ["txn_id", "bill_date", "store_code", "ean", "qty", "mrp",
                  "discount_pct", "net_amount"],
    "warehouse_stock_daily": ["snapshot_date", "warehouse", "ean", "available_qty"],
    "store_stock": ["snapshot_date", "store_code", "ean", "stock_on_hand"],
    "in_transit": ["transfer_id", "dispatch_date", "store_code", "ean", "qty"],
    "style_holds": ["style_code", "hold_reason", "hold_from"],
    "clearance_styles": ["style_code", "clearance_from"],
    "store_capacity": ["store_code", "category", "max_units"],
    "depth_targets": ["grade", "size_class", "depth_target"],
    "replenishment_decisions": ["run_date", "store_code", "ean", "decision",
                                 "units_needed", "order_qty", "source_location"],
}

# feed -> grain (from the BRIEF.md feed table), used in table comments
FEED_GRAIN = {
    "stores": "one row per store",
    "products": "one row per product (EAN = style x colour x size)",
    "pos_sales": "one row per POS transaction line (negative qty = a return)",
    "warehouse_stock_daily": "one row per day x warehouse x EAN",
    "store_stock": "one row per store x EAN, snapshot on the as-of date",
    "in_transit": "one row per transfer dispatched to a store, not yet received",
    "style_holds": "one row per style blocked from replenishment",
    "clearance_styles": "one row per end-of-season clearance style",
    "store_capacity": "one row per store x category",
    "depth_targets": "one row per store grade x size class",
    "replenishment_decisions": "one row per store x EAN, engine output for the day",
}

# feed -> cast spec applied by the silver typed() helper
FEED_CASTS = {
    "stores": {},
    "products": {"doubles": ["mrp"], "bools": ["is_pivotal"]},
    "pos_sales": {"dates": ["bill_date"], "ints": ["qty", "discount_pct"],
                  "doubles": ["mrp", "net_amount"]},
    "warehouse_stock_daily": {"dates": ["snapshot_date"], "ints": ["available_qty"]},
    "store_stock": {"dates": ["snapshot_date"], "ints": ["stock_on_hand"]},
    "in_transit": {"dates": ["dispatch_date"], "ints": ["qty"]},
    "style_holds": {"dates": ["hold_from"]},
    "clearance_styles": {"dates": ["clearance_from"]},
    "store_capacity": {"ints": ["max_units"]},
    "depth_targets": {"ints": ["depth_target"]},
    "replenishment_decisions": {"dates": ["run_date"], "ints": ["units_needed", "order_qty"]},
}


def bronze_name(feed):
    return f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{feed}_raw"


def feed_columns(feed):
    return FEED_COLUMNS[feed]


def feed_grain(feed):
    return FEED_GRAIN[feed]


def silver_name(name):
    return f"{TARGET_CATALOG}.{SILVER_SCHEMA}.{name}"


def gold_name(name):
    return f"{TARGET_CATALOG}.{GOLD_SCHEMA}.{name}"


def ingest(feed):
    """Streaming Auto Loader read of one landing feed.

    Every business column is kept as text, exactly as landed, plus the
    source-feed, source-file and ingestion-timestamp metadata columns.
    Incremental: only files not yet ingested are processed on each update.
    """
    schema = StructType([StructField(c, StringType(), True) for c in FEED_COLUMNS[feed]])
    src = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{LANDING}/{feed}")
    )
    return (
        src.withColumn("_source_feed", F.lit(feed))
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*FEED_COLUMNS[feed], "_source_feed", "_source_file", "_ingested_at")
    )


def bronze_stream(feed):
    """Streaming read of a bronze table without the ingestion metadata columns."""
    return spark.readStream.table(bronze_name(feed)).drop(
        "_source_feed", "_source_file", "_ingested_at")


def bronze_batch(feed):
    """Batch read of a bronze table without the ingestion metadata columns."""
    return spark.read.table(bronze_name(feed)).drop(
        "_source_feed", "_source_file", "_ingested_at")


def silver_table(name):
    return spark.read.table(silver_name(name))


def gold_table(name):
    return spark.read.table(gold_name(name))


def typed(df, dates=(), ints=(), doubles=(), bools=()):
    """Trim every string column and cast the listed columns to their types."""
    casts = {}
    for cols, t in [(dates, "date"), (ints, "int"), (doubles, "double"), (bools, "boolean")]:
        for c in cols:
            casts[c] = t
    return df.select([
        (F.trim(F.col(c)).cast(casts[c]) if c in casts else F.trim(F.col(c))).alias(c)
        if dtype == "string" else F.col(c)
        for c, dtype in df.dtypes])


def silver_transform(feed):
    """Typed, trimmed streaming transform of one feed for the silver layer."""
    return typed(bronze_stream(feed), **FEED_CASTS[feed])