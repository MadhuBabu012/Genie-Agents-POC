# Bronze layer: ingest all 11 Simbus Fashion raw feeds from the landing volume
# using Auto Loader (cloudFiles). Every column is kept as STRING — no casting.
# This file is executed by the Lakeflow Declarative Pipeline (DLT).

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# ---------------------------------------------------------------------------
# Pipeline configuration — injected via 'configuration' in the pipeline YAML
# ---------------------------------------------------------------------------
catalog_name         = spark.conf.get("catalog_name")
landing_schema_name  = spark.conf.get("landing_schema_name")   # schema that owns the landing volume

landing_base = f"/Volumes/{catalog_name}/{landing_schema_name}/landing"

# ---------------------------------------------------------------------------
# Feed list — one bronze table per feed
# ---------------------------------------------------------------------------
FEEDS = [
    "stores",
    "products",
    "pos_sales",
    "warehouse_stock_daily",
    "store_stock",
    "in_transit",
    "style_holds",
    "clearance_styles",
    "store_capacity",
    "depth_targets",
    "replenishment_decisions",
]


def _register_bronze_table(feed: str, source_path: str):
    """
    Register a streaming Lakeflow table for one feed via Auto Loader.
    Uses a factory function so each closure captures its own feed/path values.
    DLT manages the schema location internally.
    """

    @dp.table(
        name=f"bronze_{feed}",
        comment=(
            f"Bronze: raw '{feed}' feed ingested from {source_path}. "
            "All columns kept as STRING. "
            "Grain: one row per source CSV row. "
            "Audit columns: _ingested_at (timestamp), _source_file (string)."
        ),
        table_properties={
            "quality": "bronze",
            "pipelines.reset.allowed": "true",
        },
    )
    def _load():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            # Keep every column as STRING — casting happens in silver
            .option("inferSchema", "false")
            .option("cloudFiles.inferColumnTypes", "false")
            # Tolerate new columns arriving in later drops
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            # DLT manages the schema location internally — no need to specify it
            .load(source_path)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
        )

    return _load


# Register a bronze streaming table for every feed
for _feed in FEEDS:
    _register_bronze_table(
        feed=_feed,
        source_path=f"{landing_base}/{_feed}/",
    )
