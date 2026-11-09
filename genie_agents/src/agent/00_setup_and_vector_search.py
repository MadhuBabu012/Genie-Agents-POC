# Databricks notebook source
# MAGIC %md
# MAGIC # Why-agent - one-time setup (Day 8)
# MAGIC
# MAGIC Runs the idempotent Vector Search setup: chunks `engine_rules.md`
# MAGIC into a Delta table, provisions the endpoint and builds the index.
# MAGIC Safe to re-run - it reuses whatever already exists.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/madhubabu.duggirala@simbustech.com/.bundle/Genie_agents/dev/files/src/agent")

import setup_vector_search as svs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Chunk the engine rules and build the index

# COMMAND ----------

svs.setup(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify retrieval works

# COMMAND ----------

for hit in svs.lookup_rule("why was a pivotal size flagged for replacement instead of a transfer?", num_results=2):
    print(hit["section"], "->", hit["content"][:120].replace("\n", " "), "...")