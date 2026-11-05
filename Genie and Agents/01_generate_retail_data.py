# Databricks notebook source
# MAGIC %md
# MAGIC # Urban Loom — synthetic retail data generator
# MAGIC
# MAGIC Generates the raw files for the **Genie and Agents** exercise, plus the answer key the
# MAGIC agent is evaluated against.
# MAGIC
# MAGIC | Output | Where | Who uses it |
# MAGIC |---|---|---|
# MAGIC | 11 raw CSV feeds | `/Volumes/<catalog>/<schema>/landing/<feed>/` | You ingest these in week 1 |
# MAGIC | `ground_truth` table | `<catalog>.<schema>_eval` | **Answer key — never give the agent access** |
# MAGIC | `eval_questions` table | `<catalog>.<schema>_eval` | Questions your agent is scored on |
# MAGIC
# MAGIC **The story.** Urban Loom is a fashion retailer with 40 stores. Six weeks before the as-of
# MAGIC date, a receiving delay emptied the South warehouse of core-size (M and L) shirts for two
# MAGIC weeks, and South store sales of those shirts collapsed. Week 1 is about finding that in the
# MAGIC data with Genie. Week 2 is about an agent that explains each of today's replenishment
# MAGIC decisions.
# MAGIC
# MAGIC **Runs on Databricks Free Edition** (serverless, a few minutes, about 130K sales rows).
# MAGIC Every random value is a hash of the seed and the row's keys, so the same seed always
# MAGIC produces the same data.

# COMMAND ----------

import datetime as dt
import math
import random
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "", "1. Catalog")
dbutils.widgets.text("schema", "", "2. Schema")
dbutils.widgets.text("as_of_date", "2026-10-15", "3. As-of date (YYYY-MM-DD)")
dbutils.widgets.text("seed", "42", "4. Seed")
dbutils.widgets.dropdown("inject_dq_issues", "true", ["true", "false"], "5. Inject data-quality issues")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
AS_OF = dt.date.fromisoformat(dbutils.widgets.get("as_of_date").strip())
SEED = int(dbutils.widgets.get("seed"))
INJECT_DQ = dbutils.widgets.get("inject_dq_issues") == "true"

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
if not CATALOG or not SCHEMA:
    raise ValueError(
        "Set both the catalog and schema widgets. On Free Edition the catalog is usually "
        "`workspace`; pick a schema of your own, e.g. retail_<yourname>."
    )
if not (_IDENT.match(CATALOG) and _IDENT.match(SCHEMA)):
    raise ValueError("Catalog and schema may contain only letters, digits and underscores.")

EVAL_SCHEMA = f"{SCHEMA}_eval"
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
DAYS = 90
START = AS_OF - dt.timedelta(days=DAYS - 1)
INCIDENT_FROM = AS_OF - dt.timedelta(days=40)
INCIDENT_TO = AS_OF - dt.timedelta(days=27)
FESTIVE_FROM = AS_OF - dt.timedelta(days=12)
FESTIVE_TO = AS_OF - dt.timedelta(days=9)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{EVAL_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
print(f"Output: {VOL}  |  answer key: {CATALOG}.{EVAL_SCHEMA}  |  as-of {AS_OF}  |  seed {SEED}")

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------


def u(salt, *cols):
    """Deterministic uniform [0, 1) from the seed, a salt and the row's key columns."""
    return F.pmod(F.xxhash64(F.lit(SEED), F.lit(salt), *cols), F.lit(1_000_000)) / F.lit(1_000_000.0)


def scratch(name):
    return f"{CATALOG}.{SCHEMA}._gen_{name}"


def save_scratch(df, name):
    """Materialise to Delta and read back. Serverless has no .cache(), and reading back also
    stops long lineages being recomputed by every later join."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(scratch(name))
    return spark.table(scratch(name))


def write_feed(df, feed):
    path = f"{VOL}/{feed}"
    n = df.count()
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"  {feed:<24}{n:>9,} rows")


# COMMAND ----------

# MAGIC %md ## Master data — stores and products

# COMMAND ----------

rng = random.Random(SEED)

CITIES = [  # city, state, region, number of stores
    ("Bengaluru", "Karnataka", "South", 5), ("Mysuru", "Karnataka", "South", 1),
    ("Chennai", "Tamil Nadu", "South", 4), ("Coimbatore", "Tamil Nadu", "South", 1),
    ("Hyderabad", "Telangana", "South", 2), ("Kochi", "Kerala", "South", 1),
    ("Mumbai", "Maharashtra", "West", 5), ("Pune", "Maharashtra", "West", 3),
    ("Ahmedabad", "Gujarat", "West", 3),
    ("Delhi", "Delhi", "North", 4), ("Jaipur", "Rajasthan", "North", 2),
    ("Lucknow", "Uttar Pradesh", "North", 2), ("Chandigarh", "Chandigarh", "North", 1),
    ("Kolkata", "West Bengal", "East", 4), ("Bhubaneswar", "Odisha", "East", 1),
    ("Guwahati", "Assam", "East", 1),
]
SUFFIXES = ["Central", "Mall", "High Street", "Phoenix", "Forum", "Metro"]

stores = []
code = 1001
for city, state, region, k in CITIES:
    for i in range(k):
        stores.append([
            f"S{code}", f"Urban Loom {city} {SUFFIXES[i % len(SUFFIXES)]}", city, state, region,
            rng.choices(["A", "B", "C"], weights=[25, 45, 30])[0],
            rng.choices(["Mall", "High Street", "Airport"], weights=[55, 40, 5])[0],
            "Open", "WH-SOUTH" if region == "South" else "WH-NORTH",
        ])
        code += 1
_pick = rng.sample(range(len(stores)), 3)
stores[_pick[0]][7] = "Hold"
stores[_pick[1]][7] = "Hold"
stores[_pick[2]][7] = "Closed"
TIGHT_CAPACITY_STORES = rng.sample([s[0] for s in stores if s[7] == "Open"], 4)

stores_df = spark.createDataFrame(
    [tuple(s) for s in stores],
    "store_code string, store_name string, city string, state string, region string, "
    "grade string, store_format string, status string, serving_warehouse string",
)

CATEGORIES = [  # category, code, sizes, pivotal sizes, MRP range, number of styles
    ("SHIRT", "SH", ["S", "M", "L", "XL", "XXL"], {"M", "L"}, (1499, 2499), 18),
    ("T-SHIRT", "TS", ["S", "M", "L", "XL", "XXL"], {"M", "L"}, (799, 1299), 12),
    ("TROUSER", "TR", ["30", "32", "34", "36", "38"], {"32", "34"}, (1799, 2499), 8),
    ("DENIM", "DN", ["30", "32", "34", "36", "38"], {"32", "34"}, (1999, 2999), 8),
    ("JACKET", "JK", ["S", "M", "L", "XL"], {"M", "L"}, (3499, 4999), 4),
]
FITS = ["Slim", "Regular", "Relaxed", "Tailored"]
FABRICS = {
    "SHIRT": ["Oxford", "Linen", "Poplin", "Flannel", "Twill"],
    "T-SHIRT": ["Pique", "Jersey", "Slub", "Waffle"],
    "TROUSER": ["Chino", "Cotton", "Stretch"],
    "DENIM": ["Indigo", "Washed", "Raw"],
    "JACKET": ["Bomber", "Quilted", "Denim"],
}
TITLES = {"SHIRT": "Shirt", "T-SHIRT": "T-Shirt", "TROUSER": "Trouser", "DENIM": "Jeans", "JACKET": "Jacket"}
PALETTE = ["Navy", "White", "Sky Blue", "Olive", "Black", "Maroon", "Grey", "Beige", "Charcoal", "Indigo"]

products, styles = [], []
ean_no = 1
for cat, prefix, sizes, pivotal, (lo, hi), n_styles in CATEGORIES:
    for n in range(1, n_styles + 1):
        style_code = f"UL{prefix}{n:04d}"
        name = f"{rng.choice(FITS)} {rng.choice(FABRICS[cat])} {TITLES[cat]}"
        season = "AW-26" if cat == "JACKET" or rng.random() >= 0.15 else "SS-26"
        mrp = float(int(rng.uniform(lo, hi) / 100) * 100 + 99)
        styles.append((style_code, cat, season))
        for colour in rng.sample(PALETTE, rng.choices([1, 2, 3], weights=[40, 40, 20])[0]):
            for size in sizes:
                is_piv = size in pivotal
                products.append((
                    f"890{ean_no:010d}", style_code, name, cat, colour, size,
                    "PIVOTAL" if is_piv else "NON_PIVOTAL", is_piv, mrp, season,
                ))
                ean_no += 1

products_df = spark.createDataFrame(
    products,
    "ean string, style_code string, style_name string, category string, colour string, "
    "size string, size_class string, is_pivotal boolean, mrp double, season string",
)

aw_styles = [s for s in styles if s[2] == "AW-26" and s[1] != "JACKET"]
HELD = rng.sample(aw_styles, 3)
holds_df = spark.createDataFrame(
    [(s[0], r, AS_OF - dt.timedelta(days=rng.randint(3, 20)))
     for s, r in zip(HELD, ["Quality check", "Buyer hold", "Price change pending"])],
    "style_code string, hold_reason string, hold_from date",
)

EOSS = [s[0] for s in styles if s[2] == "SS-26"]
eoss_df = spark.createDataFrame([(s, AS_OF - dt.timedelta(days=30)) for s in EOSS],
                                "style_code string, clearance_from date")

depth_df = spark.createDataFrame(
    [("A", "PIVOTAL", 3), ("A", "NON_PIVOTAL", 2), ("B", "PIVOTAL", 2),
     ("B", "NON_PIVOTAL", 1), ("C", "PIVOTAL", 1), ("C", "NON_PIVOTAL", 1)],
    "grade string, size_class string, depth_target int",
)
print(f"{len(stores)} stores, {len(styles)} styles, {len(products)} products, "
      f"{len(HELD)} held styles, {len(EOSS)} clearance styles")

# COMMAND ----------

# MAGIC %md ## Which products each store carries

# COMMAND ----------

carry = (
    stores_df.select("store_code", "grade")
    .crossJoin(products_df.select("style_code", "category").distinct())
    .withColumn("_p", F.when(F.col("grade") == "A", 0.85)
                .when(F.col("grade") == "B", 0.65).otherwise(0.45))
    .withColumn("_p", F.when((F.col("category") == "JACKET") & (F.col("grade") == "C"), 0.0)
                .otherwise(F.col("_p")))
    .filter(u("carry", F.col("store_code"), F.col("style_code")) < F.col("_p"))
    .select("store_code", "style_code")
)
pairs = save_scratch(carry.join(products_df, "style_code"), "pairs")
print(f"store x product pairs: {pairs.count():,}")

# COMMAND ----------

# MAGIC %md ## POS sales — 90 days, with the South shirt incident and a festive spike

# COMMAND ----------

days = spark.range(DAYS).select(F.date_add(F.lit(START), F.col("id").cast("int")).alias("bill_date"))
st = stores_df.select("store_code", "grade", "region", "status")

z = F.sqrt(-2 * F.log(u("n1", F.col("store_code"), F.col("ean")) + 1e-6)) * \
    F.cos(2 * math.pi * u("n2", F.col("store_code"), F.col("ean")))
lam = (
    F.when(F.col("category") == "SHIRT", 0.10).when(F.col("category") == "T-SHIRT", 0.12)
     .when(F.col("category") == "TROUSER", 0.07).when(F.col("category") == "DENIM", 0.06)
     .otherwise(0.035)
    * F.when(F.col("grade") == "A", 1.6).when(F.col("grade") == "B", 1.0).otherwise(0.6)
    * F.when(F.col("is_pivotal"), 1.6).when(F.col("size").isin("XXL", "38"), 0.5).otherwise(1.0)
    * F.when(F.col("season") == "SS-26", 0.6).otherwise(1.0)
    * F.exp(0.5 * z)
)
is_festive = F.col("bill_date").between(F.lit(FESTIVE_FROM), F.lit(FESTIVE_TO))
day_mult = (
    F.when(F.dayofweek("bill_date").isin(1, 7), 1.35).otherwise(1.0)
    * F.when(is_festive, 1.9).otherwise(1.0)
    * F.when((F.col("region") == "South") & (F.col("category") == "SHIRT") & F.col("is_pivotal")
             & F.col("bill_date").between(F.lit(INCIDENT_FROM), F.lit(INCIDENT_TO)), 0.2).otherwise(1.0)
    * F.when((F.col("status") == "Closed")
             & (F.col("bill_date") > F.lit(AS_OF - dt.timedelta(days=20))), 0.0).otherwise(1.0)
)
sales = (
    pairs.join(st, "store_code").crossJoin(days)
    .withColumn("_p", F.least(F.lit(0.95), lam * day_mult))
    .filter(u("sale", F.col("store_code"), F.col("ean"), F.col("bill_date")) < F.col("_p"))
)
keys = (F.col("store_code"), F.col("ean"), F.col("bill_date"))
pos = (
    sales
    .withColumn("qty", F.when(u("ret", *keys) < 0.03, -1)
                .when(u("qty", *keys) < 0.12, 2).otherwise(1))
    .withColumn("discount_pct", F.when(F.col("season") == "SS-26", 30 + F.floor(u("d1", *keys) * 21))
                .when(is_festive & (u("d2", *keys) < 0.6), 10 + F.floor(u("d3", *keys) * 11))
                .when(u("d4", *keys) < 0.1, F.lit(10)).otherwise(F.lit(0)).cast("int"))
    .withColumn("net_amount", F.round(F.col("qty") * F.col("mrp") * (1 - F.col("discount_pct") / 100), 2))
    .withColumn("txn_id", F.concat(
        F.lit("TX"), F.date_format("bill_date", "yyyyMMdd"), F.lit("-"), F.col("store_code"), F.lit("-"),
        F.substring(F.sha2(F.concat_ws("|", "store_code", "ean", F.col("bill_date").cast("string")), 256), 1, 10)))
    .select("txn_id", "bill_date", "store_code", "ean", "qty", "mrp", "discount_pct", "net_amount")
)
pos = save_scratch(pos, "pos")

if INJECT_DQ:
    # Exact duplicate rows, barcodes missing from the product master, stray whitespace on
    # store codes and missing discounts. Returns (negative qty) are real business events, not
    # data-quality issues — the gold layer must keep them.
    dirty = pos.unionByName(pos.filter(u("dup", F.col("txn_id")) < 0.005))
    dirty = (
        dirty
        .withColumn("ean", F.when(u("bad_ean", F.col("txn_id")) < 0.003,
                                  F.concat(F.lit("8909999"), F.substring("ean", 8, 6))).otherwise(F.col("ean")))
        .withColumn("store_code", F.when(u("ws", F.col("txn_id")) < 0.01,
                                         F.concat(F.col("store_code"), F.lit(" "))).otherwise(F.col("store_code")))
        .withColumn("discount_pct", F.when(u("nulldisc", F.col("txn_id")) < 0.02, F.lit(None).cast("int"))
                    .otherwise(F.col("discount_pct")))
    )
else:
    dirty = pos

# COMMAND ----------

# MAGIC %md ## Warehouse stock — daily, by warehouse

# COMMAND ----------

whs = spark.createDataFrame([("WH-SOUTH",), ("WH-NORTH",)], "warehouse string")
wk = (F.col("ean"), F.col("warehouse"))
wh_daily = (
    products_df.select("ean", "category", "is_pivotal", "season")
    .crossJoin(whs)
    .crossJoin(days.withColumnRenamed("bill_date", "snapshot_date"))
    .withColumn("_base", F.when(F.col("is_pivotal"), 40 + F.floor(u("whb", *wk) * 120))
                .otherwise(10 + F.floor(u("whb", *wk) * 50))
                * F.when(F.col("warehouse") == "WH-NORTH", 1.5).otherwise(1.0)
                * F.when(F.col("season") == "SS-26", 0.4).otherwise(1.0))
    .withColumn("_level", F.floor(F.col("_base") * (0.7 + 0.3 * u("whd", *wk, F.col("snapshot_date")))))
    .withColumn("_c", u("whz", *wk))
    .withColumn("available_qty", F.when(
        (F.col("warehouse") == "WH-SOUTH") & (F.col("category") == "SHIRT") & F.col("is_pivotal")
        & F.col("snapshot_date").between(F.lit(INCIDENT_FROM), F.lit(INCIDENT_TO)), 0)
        .when((F.col("_c") < 0.22) & (F.col("snapshot_date") >= F.lit(AS_OF - dt.timedelta(days=5))), 0)
        .when((F.col("_c") < 0.32) & (F.col("snapshot_date") == F.lit(AS_OF)),
              1 + F.floor(u("whs", *wk) * 3))
        .otherwise(F.col("_level")).cast("int"))
    .select("snapshot_date", "warehouse", "ean", "available_qty")
)
wh_daily = save_scratch(wh_daily, "wh_daily")

# COMMAND ----------

# MAGIC %md ## Store stock, in-transit and capacity — at the as-of date

# COMMAND ----------

pk = (F.col("store_code"), F.col("ean"))
soh = save_scratch(
    pairs.withColumn("stock_on_hand", F.when(
        u("soh0", *pk) < F.when(F.col("is_pivotal"), 0.30).otherwise(0.22), 0)
        .otherwise(1 + F.floor(F.pow(u("soh", *pk), 1.6) * 6)).cast("int"))
    .select(F.lit(AS_OF).alias("snapshot_date"), "store_code", "ean", "category", "stock_on_hand"),
    "soh",
)

transit = save_scratch(
    pairs.filter(u("tr", *pk) < 0.07)
    .select(
        F.concat(F.lit("TRF-"), F.substring(F.sha2(F.concat_ws("|", "store_code", "ean"), 256), 1, 10)).alias("transfer_id"),
        F.date_sub(F.lit(AS_OF), (1 + F.floor(u("trd", *pk) * 4)).cast("int")).alias("dispatch_date"),
        "store_code", "ean",
        F.when(u("trq", *pk) < 0.3, 2).otherwise(1).alias("qty"),
    ),
    "transit",
)

capacity = (
    soh.groupBy("store_code", "category").agg(F.sum("stock_on_hand").alias("_current"))
    .withColumn("max_units", F.when(
        F.col("store_code").isin(TIGHT_CAPACITY_STORES) & (F.col("category") == "SHIRT"), F.col("_current"))
        .otherwise(F.ceil(F.col("_current") * (1.6 + u("cap", F.col("store_code"), F.col("category"))))).cast("int"))
    .select("store_code", "category", "max_units")
)
capacity = save_scratch(capacity, "capacity")

# COMMAND ----------

# MAGIC %md ## Run the replenishment engine (rules in `engine_rules.md`)

# COMMAND ----------

wh_asof = wh_daily.filter(F.col("snapshot_date") == F.lit(AS_OF)).select(
    F.col("warehouse").alias("serving_warehouse"), "ean", F.col("available_qty").alias("wh_available"))
cat_units = soh.groupBy("store_code", "category").agg(F.sum("stock_on_hand").alias("category_units"))

base = save_scratch(
    pairs.select("store_code", "ean", "style_code", "style_name", "category", "colour", "size",
                 "size_class", "is_pivotal", "season")
    .join(stores_df, "store_code")
    .join(depth_df, ["grade", "size_class"])
    .join(soh.select("store_code", "ean", "stock_on_hand"), ["store_code", "ean"], "left")
    .join(transit.groupBy("store_code", "ean").agg(F.sum("qty").alias("in_transit")), ["store_code", "ean"], "left")
    .join(wh_asof, ["serving_warehouse", "ean"], "left")
    .join(holds_df.withColumn("is_on_hold", F.lit(True)), "style_code", "left")
    .join(eoss_df.select("style_code", F.lit(True).alias("is_clearance")), "style_code", "left")
    .join(cat_units, ["store_code", "category"], "left")
    .join(capacity, ["store_code", "category"], "left")
    .fillna({"stock_on_hand": 0, "in_transit": 0, "wh_available": 0, "category_units": 0})
    .withColumn("is_on_hold", F.coalesce("is_on_hold", F.lit(False)))
    .withColumn("is_clearance", F.coalesce("is_clearance", F.lit(False)))
    .withColumn("units_needed", F.greatest(F.lit(0), F.col("depth_target") - F.col("stock_on_hand") - F.col("in_transit")))
    .withColumn("grade_ord", F.when(F.col("grade") == "A", 1).when(F.col("grade") == "B", 2).otherwise(3))
    .withColumn("pre_reason",
        F.when(F.col("status") != "Open", "STORE_NOT_OPEN")
         .when(F.col("is_on_hold"), "STYLE_ON_HOLD")
         .when(F.col("stock_on_hand") >= F.col("depth_target"), "DEPTH_MET")
         .when(F.col("stock_on_hand") + F.col("in_transit") >= F.col("depth_target"), "COVERED_BY_IN_TRANSIT")
         .when(F.col("category_units") >= F.col("max_units"), "CAPACITY_FULL")),
    "engine_base",
)

# Warehouse pool, shared in grade order.
in_pool = F.col("pre_reason").isNull() & (F.col("wh_available") > 0)
w_pool = Window.partitionBy("serving_warehouse", "ean").orderBy("grade_ord", "store_code") \
               .rowsBetween(Window.unboundedPreceding, -1)
alloc = (
    base.withColumn("in_pool", in_pool)
    .withColumn("claimed_before", F.coalesce(F.sum(F.when(in_pool, F.col("units_needed")).otherwise(0)).over(w_pool), F.lit(0)))
    .withColumn("wh_alloc", F.when(in_pool, F.least(F.col("units_needed"),
                F.greatest(F.lit(0), F.col("wh_available") - F.col("claimed_before")))).otherwise(0))
)

# Donors for inter-store transfer: same state, open, at least 2 units above their own depth.
# Both sides come from `base`, so every column is renamed before the join — a self-join on
# shared column references silently resolves to the wrong side.
donors = base.filter(F.col("status") == "Open").select(
    F.col("store_code").alias("donor_store_code"), F.col("store_name").alias("donor_store_name"),
    F.col("state").alias("donor_state"), F.col("ean").alias("donor_ean"),
    (F.col("stock_on_hand") - F.col("depth_target")).alias("donor_surplus"),
).filter(F.col("donor_surplus") >= 2)

receivers = alloc.filter(
    F.col("pre_reason").isNull() & (F.col("wh_available") == 0) & ~F.col("is_clearance")
).select(F.col("store_code").alias("recv_store_code"), F.col("ean").alias("recv_ean"),
         F.col("state").alias("recv_state"))
best_donor = (
    receivers.join(donors, (F.col("recv_ean") == F.col("donor_ean"))
                   & (F.col("recv_state") == F.col("donor_state"))
                   & (F.col("recv_store_code") != F.col("donor_store_code")))
    .withColumn("_rk", F.row_number().over(
        Window.partitionBy("recv_store_code", "recv_ean").orderBy(F.desc("donor_surplus"), "donor_store_code")))
    .filter(F.col("_rk") == 1)
    .select(F.col("recv_store_code").alias("store_code"), F.col("recv_ean").alias("ean"),
            "donor_store_code", "donor_store_name", "donor_surplus")
)

d = alloc.join(best_donor, ["store_code", "ean"], "left")
reason = F.coalesce(
    F.col("pre_reason"),
    F.when(F.col("in_pool") & (F.col("wh_alloc") > 0), "WH_REPLENISH")
     .when(F.col("in_pool"), "WH_ALLOCATED_TO_HIGHER_GRADE")
     .when(F.col("is_clearance"), "EOSS_NO_TRANSFER")
     .when(F.col("donor_store_code").isNotNull(), "IST_FROM_DONOR")
     .when(F.col("is_pivotal"), "REPLACE_CANDIDATE")
     .otherwise("NO_SOURCE_NON_PIVOTAL"),
)
d = (
    d.withColumn("reason_code", reason)
    .withColumn("decision", F.when(F.col("reason_code") == "WH_REPLENISH", "REPLENISH_WH")
                .when(F.col("reason_code") == "IST_FROM_DONOR", "IST")
                .when(F.col("reason_code") == "REPLACE_CANDIDATE", "REPLACE_CANDIDATE")
                .otherwise("NO_ACTION"))
    .withColumn("order_qty", F.when(F.col("decision") == "REPLENISH_WH", F.col("wh_alloc"))
                .when(F.col("decision") == "IST", F.least(F.col("units_needed"), F.col("donor_surplus")))
                .otherwise(0).cast("int"))
    .withColumn("source_location", F.when(F.col("decision") == "REPLENISH_WH", F.col("serving_warehouse"))
                .when(F.col("decision") == "IST", F.col("donor_store_code")))
)

rc = F.col("reason_code")
detail = (
    F.when(rc == "STORE_NOT_OPEN", F.format_string("Store status is %s, so the store is not eligible for replenishment.", "status"))
    .when(rc == "STYLE_ON_HOLD", F.format_string("Style %s is on hold (%s) since %s.", "style_code", "hold_reason", F.col("hold_from").cast("string")))
    .when(rc == "DEPTH_MET", F.format_string("Stock on hand %d meets the grade %s depth target of %d.", "stock_on_hand", "grade", "depth_target"))
    .when(rc == "COVERED_BY_IN_TRANSIT", F.format_string("Stock on hand %d plus %d in transit covers the depth target of %d.", "stock_on_hand", "in_transit", "depth_target"))
    .when(rc == "CAPACITY_FULL", F.format_string("The store already holds %d %s units against a capacity of %d.", "category_units", "category", "max_units"))
    .when(rc == "WH_REPLENISH", F.format_string("Needed %d. %s had %d available after %d were claimed by stores ahead in grade order, and allocated %d.", "units_needed", "serving_warehouse", "wh_available", "claimed_before", "wh_alloc"))
    .when(rc == "WH_ALLOCATED_TO_HIGHER_GRADE", F.format_string("Needed %d. %s had only %d units and %d were claimed by stores ahead in grade order, so none were left.", "units_needed", "serving_warehouse", "wh_available", "claimed_before"))
    .when(rc == "EOSS_NO_TRANSFER", F.format_string("%s has 0 units and style %s is on the clearance list, so it is not transferred or replaced.", "serving_warehouse", "style_code"))
    .when(rc == "IST_FROM_DONOR", F.format_string("%s has 0 units. %s (%s) in %s has %d units above its own depth target, so %d units are transferred.", "serving_warehouse", "donor_store_name", "donor_store_code", "state", "donor_surplus", "order_qty"))
    .when(rc == "REPLACE_CANDIDATE", F.format_string("Pivotal size. %s has 0 units and no store in %s has surplus, so the product is flagged for replacement.", "serving_warehouse", "state"))
    .otherwise(F.format_string("Non-pivotal size. %s has 0 units and no store in %s has surplus. Non-pivotal sizes are never replaced, so nothing is ordered.", "serving_warehouse", "state"))
)
ground_truth = save_scratch(
    d.withColumn("reason_detail", detail).select(
        F.lit(AS_OF).alias("run_date"), "store_code", "store_name", "city", "state", "region", "grade",
        "status", "ean", "style_code", "style_name", "category", "colour", "size", "size_class",
        "depth_target", "stock_on_hand", "in_transit", "serving_warehouse", "wh_available",
        "claimed_before", "category_units", "max_units", "is_on_hold", "is_clearance",
        "donor_store_code", "donor_surplus", "units_needed", "decision", "order_qty",
        "source_location", "reason_code", "reason_detail"),
    "ground_truth",
)

# COMMAND ----------

# MAGIC %md ## Write the raw feeds

# COMMAND ----------

print("Raw feeds:")
write_feed(stores_df, "stores")
write_feed(products_df, "products")
write_feed(dirty, "pos_sales")
write_feed(wh_daily, "warehouse_stock_daily")
write_feed(soh.drop("category"), "store_stock")
write_feed(transit, "in_transit")
write_feed(holds_df, "style_holds")
write_feed(eoss_df, "clearance_styles")
write_feed(capacity, "store_capacity")
write_feed(depth_df, "depth_targets")
write_feed(ground_truth.select("run_date", "store_code", "ean", "decision", "units_needed",
                               "order_qty", "source_location"), "replenishment_decisions")

# COMMAND ----------

# MAGIC %md ## Answer key and evaluation questions (keep these away from the agent)

# COMMAND ----------

ground_truth.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{EVAL_SCHEMA}.ground_truth")

q = (F.col("store_name"), F.col("store_code"), F.col("style_name"), F.col("colour"), F.col("size"))
eval_q = (
    ground_truth
    .withColumn("_rk", F.row_number().over(
        Window.partitionBy("reason_code").orderBy(u("eval", F.col("store_code"), F.col("ean")))))
    .filter(F.col("_rk") <= 3)
    .withColumn("question", F.when(F.col("decision") == "REPLENISH_WH", F.format_string(
            "Why did %s (%s) receive %d units of the %s, %s, size %s today?", "store_name", "store_code", "order_qty", "style_name", "colour", "size"))
        .when(F.col("decision") == "IST", F.format_string(
            "Why is %s (%s) getting the %s, %s, size %s from another store instead of the warehouse?", *q))
        .when(F.col("decision") == "REPLACE_CANDIDATE", F.format_string(
            "Why is the %s, %s, size %s flagged for replacement at %s (%s)?", "style_name", "colour", "size", "store_name", "store_code"))
        .otherwise(F.format_string("Why did %s (%s) not receive any %s, %s, size %s today?", *q)))
    .select(F.monotonically_increasing_id().alias("question_id"), "question", "store_code", "ean",
            "decision", "reason_code", F.col("reason_detail").alias("expected_answer"))
)
eval_q.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{EVAL_SCHEMA}.eval_questions")

# COMMAND ----------

# MAGIC %md ## Validate

# COMMAND ----------

gt = spark.table(f"{CATALOG}.{EVAL_SCHEMA}.ground_truth")
ALL_REASONS = ["STORE_NOT_OPEN", "STYLE_ON_HOLD", "DEPTH_MET", "COVERED_BY_IN_TRANSIT", "CAPACITY_FULL",
               "WH_REPLENISH", "WH_ALLOCATED_TO_HIGHER_GRADE", "EOSS_NO_TRANSFER", "IST_FROM_DONOR",
               "REPLACE_CANDIDATE", "NO_SOURCE_NON_PIVOTAL"]
counts = {r["reason_code"]: r["n"] for r in gt.groupBy("reason_code").agg(F.count("*").alias("n")).collect()}
print("Decisions by reason:")
for r in ALL_REASONS:
    print(f"  {r:<30}{counts.get(r, 0):>7,}")
thin = [r for r in ALL_REASONS if counts.get(r, 0) < 3]
if thin:
    print(f"\nWARNING: fewer than 3 examples for {thin}. Re-run with a different seed.")

over = (gt.filter(F.col("decision") == "REPLENISH_WH")
        .groupBy("serving_warehouse", "ean").agg(F.sum("order_qty").alias("sent"), F.max("wh_available").alias("had"))
        .filter(F.col("sent") > F.col("had")).count())
assert over == 0, f"{over} warehouse/product pairs allocated more than the warehouse held"
print(f"\nWarehouse over-allocation check: OK")
print(f"Evaluation questions: {spark.table(f'{CATALOG}.{EVAL_SCHEMA}.eval_questions').count()}")

for t in ["pairs", "pos", "wh_daily", "soh", "transit", "capacity", "engine_base", "ground_truth"]:
    spark.sql(f"DROP TABLE IF EXISTS {scratch(t)}")
print("Done. Scratch tables dropped.")
