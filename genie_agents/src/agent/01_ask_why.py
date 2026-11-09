# Databricks notebook source
# MAGIC %md
# MAGIC # Ask the Why-agent (Days 6-10 demo)
# MAGIC
# MAGIC The agent answers planner questions with the reason and the numbers
# MAGIC behind it, using read-only UC tool functions, Vector Search rule
# MAGIC retrieval and an LLM with a deterministic rules engine as backup.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/madhubabu.duggirala@simbustech.com/.bundle/Genie_agents/dev/files/src/agent")

from why_agent import ask_why

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ask a question (uses S1002 / 8900000000440 - a real decision)

# COMMAND ----------

r = ask_why("Why did store S1002 not receive EAN 8900000000440 today?", planner="madhubabu")
print(r["answer"])
print()
print("reason_code:", r["reason_code"], "| decision:", r["decision"], "| tools:", r["tools_used"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Guardrails - out of scope and unknown product

# COMMAND ----------

print(ask_why("What is the weather tomorrow?")["answer"])
print()
print(ask_why("Why did store S1002 not receive EAN 0000000000001 today?")["answer"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Planner memory - the agent remembered the last store

# COMMAND ----------

# No store in this question: the agent remembers "S1002" from the first call.
r2 = ask_why("how much of EAN 8900000000440 is in transit to my store?", planner="madhubabu")
print(r2["answer"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deterministic engine alone (no LLM) - the guaranteed fallback

# COMMAND ----------

from why_agent import explain_decision_deterministic
print(explain_decision_deterministic("S1002", "8900000000440")["narrative"])