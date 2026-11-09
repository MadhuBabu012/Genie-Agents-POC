# Why-agent - replenishment decision explainer

Answers planner questions like _"why did store S1002 not receive EAN
8900000000440 today?"_ with the reason and the numbers behind it.

## Pieces

| Piece | Where | What it does |
| --- | --- | --- |
| Tool functions | `genie_agents_poc.agent_tools` (from `files/tools.sql`) | 9 read-only UC functions: store profile, product, stock position, in-transit, warehouse stock, style flags, store capacity, donor candidates, replenishment decision |
| Metric views | `genie_agents_poc.gold` (from `files/metric_views.sql`) | `mv_retail_sales`, `mv_store_stock`, `mv_warehouse_stock` - governed KPIs used by the Genie agent |
| Vector Search | `engine_rules_chunks` table + `engine_rules_index` (TRIGGERED Delta Sync, endpoint `genie-agents-poc-vs`) | Chunks of `src/Documentation/engine_rules.md`, retrieved per question |
| Agent core | `why_agent.py` | Tool-calling agent: gathers core facts with tools, the model picks extra tools (round 1) and composes the answer (round 2); deterministic rules cascade as backup; guardrails; MLflow tracing; token usage; planner memory (`planner_memory` table) |
| Setup | `setup_vector_search.py` | Idempotent one-time setup: chunks table, endpoint, index. Run `setup(spark)` |
| Evaluation | `evaluate.py` | Developer-only scoring vs the hidden answer key in `bronze_eval`; logs to MLflow `/Shared/why-agent-evaluation`, writes `agent_tools.agent_eval_results`. The agent itself never reads `_eval` |
| Notebooks | `00_setup_and_vector_search.py`, `01_ask_why.py`, `evaluate.py` | One-time setup, interactive demo, evaluation |
| App | `../app/` (`why_agent_app.py` + `app.yaml`) | Dash front-end served as a Databricks App; runs Spark-free via a SQL warehouse, the UC tools, Vector Search and the LLM |

## Run order

1. `00_setup_and_vector_search.py` - one-time (idempotent) Vector Search setup.
2. `01_ask_why.py` - ask questions, see guardrails, planner memory, fallback.
3. `evaluate.py` - score the agent against the 33-question key (developer only).
4. Deploy the app from the bundle root: `databricks bundle deploy -t dev`.

## Guardrails

- Scope: only replenishment/stock/transfer questions; anything else is refused politely.
- Never reads `_eval` / answer-key tables (they live in `bronze_eval`, used only by `evaluate.py`).
- Says "I don't know" when tools return no data - never guesses.
- All tool arguments are regex-validated and SQL-escaped; no user input is interpolated into table names.

## Config

Constants at the top of `why_agent.py` and `setup_vector_search.py`:
`CATALOG` (genie_agents_poc), `TOOLS` schema (agent_tools), `MODEL`
(databricks-meta-llama-3-3-70b-instruct - any FM API chat model works).
The app reads `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`, `DATABRICKS_WAREHOUSE_ID`
from `src/app/app.yaml`.
