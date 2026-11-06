"""Gold: dim_date - one row per calendar day used anywhere in the pipeline."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common import gold_name, silver_table


@dp.materialized_view(
    name=gold_name("dim_date"),
    comment="Gold date dimension - one row per calendar day, spanning every date used anywhere "
            "in the pipeline (sales window through the as-of run date), so no fact date key is ever missing.",
)
@dp.expect_all({"date key present": "date_key IS NOT NULL"})
def dim_date():
    dates = (
        silver_table("pos_sales").select(F.col("bill_date").alias("full_date"))
        .unionByName(silver_table("warehouse_stock_daily").select(F.col("snapshot_date").alias("full_date")))
        .unionByName(silver_table("store_stock").select(F.col("snapshot_date").alias("full_date")))
        .unionByName(silver_table("in_transit").select(F.col("dispatch_date").alias("full_date")))
        .unionByName(silver_table("style_holds").select(F.col("hold_from").alias("full_date")))
        .unionByName(silver_table("clearance_styles").select(F.col("clearance_from").alias("full_date")))
        .unionByName(silver_table("replenishment_decisions").select(F.col("run_date").alias("full_date")))
        .where("full_date IS NOT NULL")
        .distinct()
    )
    return dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.date_sub("full_date", F.dayofweek("full_date") - 1).alias("week_start_date"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )