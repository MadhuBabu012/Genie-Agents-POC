# Databricks notebook source
raw_tables = [
    "stores_raw",
    "products_raw",
    "pos_sales_raw",
    "warehouse_stock_daily_raw",
    "store_stock_raw",
    "in_transit_raw",
    "style_holds_raw",
    "clearance_styles_raw",
    "store_capacity_raw",
    "depth_targets_raw",
    "replenishment_decisions_raw",
]

# Drop all other tables in genie_agents_poc.bronze except the ones listed above
keep = set(raw_tables)
existing = [t.name for t in spark.catalog.listTables("genie_agents_poc.bronze")]
to_drop = [t for t in existing if t not in keep]

print(f"Found {len(existing)} tables; dropping {len(to_drop)}:")
print(to_drop)

for t in to_drop:
    spark.sql(f"DROP TABLE genie_agents_poc.bronze.{t}")
    print(f"Dropped genie_agents_poc.bronze.{t}")

print("Done. Remaining tables:", [t.name for t in spark.catalog.listTables("genie_agents_poc.bronze")])

for t in raw_tables:
    df = spark.table(t)
    print(f"{t}: {df.count()} rows, {len(df.columns)} columns")
    df.printSchema()
    print("-" * 80)

# COMMAND ----------

