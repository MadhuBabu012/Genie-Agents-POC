# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 — Free Edition smoke test (about 15 minutes)
# MAGIC
# MAGIC Checks that your Free Edition workspace supports everything this exercise needs **before**
# MAGIC you build on it. Run every cell and fill in the results table at the bottom.
# MAGIC
# MAGIC It creates only tiny objects in a schema called `<schema>_smoke`, and drops them at the end.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "1. Catalog (Free Edition: workspace)")
dbutils.widgets.text("schema", "", "2. Your schema, e.g. retail_<yourname>")
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
if not (CATALOG and SCHEMA and re.match(r"^\w+$", CATALOG) and re.match(r"^\w+$", SCHEMA)):
    raise ValueError("Set the catalog and schema widgets (letters, digits and underscores only).")

SMOKE = f"{CATALOG}.{SCHEMA}_smoke"
results = {}
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SMOKE}")
spark.createDataFrame(
    [("South", "SHIRT", 3), ("South", "DENIM", 1), ("North", "SHIRT", 5), ("North", "DENIM", 2)],
    "region string, category string, qty int",
).write.mode("overwrite").saveAsTable(f"{SMOKE}.sales")
results["Serverless compute + Delta write"] = "OK"
print("1. Serverless compute and Delta write: OK")

# COMMAND ----------

# MAGIC %md ## 2. Metric view

# COMMAND ----------

def _metric_view(version):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {SMOKE}.sales_metrics
        WITH METRICS LANGUAGE YAML AS $$
        version: {version}
        source: {SMOKE}.sales
        dimensions:
          - name: Region
            expr: region
        measures:
          - name: Units
            expr: SUM(qty)
        $$""")

try:
    for v in ("1.1", "0.1"):
        try:
            _metric_view(v)
            break
        except Exception as e:
            last = e
    else:
        raise last
    spark.sql(f"SELECT Region, MEASURE(Units) AS units FROM {SMOKE}.sales_metrics GROUP BY Region").show()
    results["Metric view"] = f"OK (YAML version {v})"
except Exception as e:
    results["Metric view"] = f"FAILED: {str(e)[:200]}"
print("2.", results["Metric view"])

# COMMAND ----------

# MAGIC %md ## 3. Foundation model (pay-per-token)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()
try:
    endpoints = sorted(e.name for e in w.serving_endpoints.list() if e.name.startswith("databricks-"))
    print("Foundation model endpoints available to you:")
    for n in endpoints:
        print("  ", n)
    chat = next((n for n in endpoints if any(k in n for k in ("llama", "claude", "gpt", "qwen"))), None)
    if chat is None:
        raise RuntimeError("no chat model endpoint found")
    reply = w.serving_endpoints.query(
        name=chat, max_tokens=20,
        messages=[ChatMessage(role=ChatMessageRole.USER, content="Reply with the single word OK.")],
    )
    print(f"\n{chat} replied: {reply.choices[0].message.content!r}")
    results["Foundation model"] = f"OK ({chat})"
except Exception as e:
    results["Foundation model"] = f"FAILED: {str(e)[:200]}"
print("3.", results["Foundation model"])

# COMMAND ----------

# MAGIC %md ## 4. Vector Search (list only — Free Edition allows one endpoint, so do not create one here)

# COMMAND ----------

try:
    eps = list(w.vector_search_endpoints.list_endpoints())
    print(f"Vector Search endpoints in this workspace: {[e.name for e in eps] or 'none yet'}")
    results["Vector Search API"] = "OK"
except Exception as e:
    results["Vector Search API"] = f"FAILED: {str(e)[:200]}"
print("4.", results["Vector Search API"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Genie — manual check in the UI (2 minutes)
# MAGIC
# MAGIC 1. Left sidebar → **Genie** → **New**.
# MAGIC 2. Add the metric view `<catalog>.<schema>_smoke.sales_metrics` as a data source.
# MAGIC 3. Ask: **"Which region sold the most units?"** The answer should be North (7).
# MAGIC 4. Delete the Genie agent afterwards.
# MAGIC
# MAGIC Record the result in the table below.

# COMMAND ----------

# MAGIC %md ## 6. Results and clean-up

# COMMAND ----------

print(f"{'Check':<36}Result")
for k, v in results.items():
    print(f"{k:<36}{v}")
print(f"{'Genie (manual)':<36}record in the team tracker")

spark.sql(f"DROP SCHEMA IF EXISTS {SMOKE} CASCADE")
print(f"\nDropped {SMOKE}.")
