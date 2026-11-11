# Simbus Fashion — Genie & Agents POC: Architecture Document

**Project:** Retail Replenishment Intelligence Platform  
**Platform:** Databricks (Unity Catalog + Lakeflow Declarative Pipelines + Apps)  
**Catalog:** `genie_agents_poc`

---

## 1. Business Context

Simbus Fashion (branded as *Urban Loom*) operates **40 retail stores** across 4 regions in India,
supplied by **2 warehouses** (WH-SOUTH and WH-NORTH). Every morning the replenishment engine
decides, for every store × product combination, whether to send stock from the warehouse or arrange
an inter-store transfer.

Planners were raising a consistent complaint: **"The engine decided X, but I don't know why."**

This POC builds an end-to-end data platform that:

1. **Ingests** 11 raw data feeds from the engine and store systems.
2. **Transforms** them through a Bronze → Silver → Gold medallion pipeline.
3. **Exposes** business KPIs through metric views consumable by Databricks Genie (AI BI).
4. **Answers** the question *"Why did store S1002 not receive EAN 8900000000440 today?"* through
   an AI agent backed by deterministic engine rules and tool-grounded LLM reasoning.

---

## 2. High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA                                     │
│   11 CSV feeds → Unity Catalog Volume                                  │
│   /Volumes/genie_agents_poc/bronze/landing/                            │
└────────────────────────────┬───────────────────────────────────────────┘
                             │  Auto Loader (cloudFiles)
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (genie_agents_poc.bronze)                               │
│  11 streaming tables — all columns as STRING, unmodified               │
│  + _source_feed, _source_file, _ingested_at metadata                  │
└────────────────────────────┬───────────────────────────────────────────┘
                             │  Type casting · Deduplication · Validation
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (genie_agents_poc.silver)                               │
│  11 clean typed tables + 1 quarantine + 1 reconciliation check        │
└────────────────────────────┬───────────────────────────────────────────┘
                             │  Star schema · Aggregation · Surrogate keys
                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (genie_agents_poc.gold)                                   │
│  3 dimension tables · 4 fact tables · 3 metric views                  │
└────────────────────────────┬───────────────────────────────────────────┘
                             │
               ┌─────────────┴──────────────┐
               ▼                            ▼
  ┌────────────────────────┐   ┌────────────────────────────────┐
  │  Databricks Genie      │   │  Why-Agent App                 │
  │  (AI BI on metric      │   │  Databricks App (Dash UI)      │
  │   views)               │   │  LLM + 9 UC tool functions     │
  └────────────────────────┘   └────────────────────────────────┘
```

---

## 3. Source Data — 11 Feeds

All source files are CSV, landing in a Unity Catalog Volume under one sub-folder per feed.

| Feed | Grain | Key Business Content |
|---|---|---|
| `stores` | 1 row per store | Code, name, city, state, region, grade, format, status, serving warehouse |
| `products` | 1 row per EAN | Style code, name, category, colour, size, size class, is_pivotal, MRP, season |
| `pos_sales` | 1 row per POS transaction line | Bill date, store, EAN, qty (negative = return), MRP, discount, net amount |
| `warehouse_stock_daily` | 1 row per day × warehouse × EAN | Available qty in warehouse |
| `store_stock` | 1 row per store × EAN snapshot | Stock on hand |
| `in_transit` | 1 row per transfer dispatched | Transfer to store, qty, dispatch date |
| `style_holds` | 1 row per blocked style | Style code, hold reason, hold-from date |
| `clearance_styles` | 1 row per clearance style | Style code, clearance-from date |
| `store_capacity` | 1 row per store × category | Max units a store can hold per category |
| `depth_targets` | 1 row per grade × size class | How many units a store of that grade must hold |
| `replenishment_decisions` | 1 row per store × EAN (run date) | Engine output: decision, units_needed, order_qty, source_location |

---

## 4. Bronze Layer — Raw Ingestion

**Schema:** `genie_agents_poc.bronze`  
**Technology:** Databricks Auto Loader (`cloudFiles` format, CSV)  
**Pattern:** Streaming incremental — only new files are processed on each pipeline update.

Every feed has its own table. All business columns are kept **as STRING** exactly as landed —
no casting, no cleaning. Three metadata columns are added by the pipeline:

| Metadata Column | Content |
|---|---|
| `_source_feed` | Feed name (e.g. `pos_sales`) |
| `_source_file` | Filename that produced the row |
| `_ingested_at` | Timestamp when the row was ingested |

### Bronze Tables

```
genie_agents_poc.bronze
├── stores_raw
├── products_raw
├── pos_sales_raw
├── warehouse_stock_daily_raw
├── store_stock_raw
├── in_transit_raw
├── style_holds_raw
├── clearance_styles_raw
├── store_capacity_raw
├── depth_targets_raw
└── replenishment_decisions_raw
```

> **Why keep everything as text?** This is the "raw vault" principle — the bronze table is an exact
> copy of what landed. If a type cast fails downstream, the original data is never lost and can be
> re-processed.

---

## 5. Silver Layer — Clean, Typed, Validated

**Schema:** `genie_agents_poc.silver`  
**Technology:** Lakeflow Declarative Pipelines (`@dp.table`, `@dp.expect_all`, `@dp.materialized_view`)

Each bronze table is promoted to silver by:

1. **Trimming** all string columns (removes leading/trailing whitespace).
2. **Casting** to correct types (dates, integers, doubles, booleans) per feed.
3. **Validating** with expectations — rows that violate rules are quarantined.
4. **Deduplicating** where needed (POS sales uses `ROW_NUMBER()` over a composite key).

### Silver Tables

```
genie_agents_poc.silver
├── stores
├── products
├── pos_sales                       ← deduped + FK-validated MV
├── pos_sales_quarantine            ← rows that failed: DUPLICATE_ROW / UNKNOWN_PRODUCT / UNKNOWN_STORE
├── pos_sales_reconciliation_check  ← private MV: bronze count = silver + quarantine (data quality gate)
├── warehouse_stock_daily
├── store_stock
├── in_transit
├── style_holds
├── clearance_styles
├── store_capacity
├── depth_targets
└── replenishment_decisions
```

### POS Sales Data Quality Flow

```
                    ┌──────────────────────────┐
                    │  bronze.pos_sales_raw     │
                    │  (all rows, as text)      │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  Deduplication           │
                    │  ROW_NUMBER() over       │
                    │  (txn_id, store, EAN,    │
                    │   date, qty, amount)     │
                    └────────────┬─────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
   ┌──────────▼──────┐ ┌────────▼────────┐ ┌──────▼─────────────┐
   │ silver.pos_sales │ │ silver.         │ │ silver.pos_sales_  │
   │ (valid rows)     │ │ pos_sales_      │ │ reconciliation_    │
   │                  │ │ quarantine      │ │ check (private)    │
   └──────────────────┘ │ (bad rows)      │ │ asserts no data    │
                        └─────────────────┘ │ loss               │
                                            └────────────────────┘
```

**Quarantine rules:**

| Rule | Condition |
|---|---|
| `DUPLICATE_ROW` | Same transaction seen more than once |
| `UNKNOWN_PRODUCT` | EAN not found in `silver.products` |
| `UNKNOWN_STORE` | Store code not found in `silver.stores` |

---

## 6. Gold Layer — Star Schema

**Schema:** `genie_agents_poc.gold`  
**Technology:** Lakeflow Declarative Pipelines (`@dp.materialized_view`)

The gold layer models the data as a **star schema** — dimension tables describe the business
entities; fact tables contain the measurable events. Surrogate keys (`store_key`, `product_key`,
`date_key`) are assigned using `ROW_NUMBER()` and join the dims to facts.

### Entity-Relationship Overview

```
                        ┌─────────────┐
                        │  dim_date   │
                        │  date_key   │
                        └──────┬──────┘
                               │
            ┌──────────────────┼──────────────────────┐
            │                  │                      │
     ┌──────▼──────┐    ┌──────▼──────┐       ┌──────▼──────┐
     │  dim_store  │    │  fact_sales │       │  dim_product│
     │  store_key  │◄───│  store_key  │───►   │  product_key│
     │  store_code │    │  product_key│       │  ean        │
     │  region     │    │  date_key   │       │  style_code │
     │  grade      │    │  revenue    │       │  category   │
     │  status     │    │  units_sold │       │  size       │
     └──────┬──────┘    │  discount   │       │  is_pivotal │
            │           └─────────────┘       └──────┬──────┘
            │                                        │
     ┌──────▼─────────────────────────────────┬──────┘
     │           fact_store_stock             │
     │  store_key · product_key               │
     │  snapshot_date                         │
     │  stock_on_hand · in_transit_qty        │
     │  wh_available · units_needed           │
     │  is_on_hold · is_clearance             │
     └──────────────────┬─────────────────────┘
                        │
     ┌──────────────────┴──────────────────────────────┐
     │                                                 │
┌────▼──────────────────┐    ┌────────────────────────▼──────┐
│  fact_warehouse_stock │    │  fact_replenishment_decisions │
│  store_key (via wh)   │    │  store_key · product_key      │
│  product_key          │    │  run_date                     │
│  snapshot_date        │    │  decision · order_qty         │
│  available_qty        │    │  source_location              │
└───────────────────────┘    └───────────────────────────────┘
```

### Dimension Tables

| Table | Rows | Description |
|---|---|---|
| `dim_store` | 1 per store | Store attributes: name, city, state, region, grade, format, status, serving warehouse |
| `dim_product` | 1 per EAN | Product attributes: style, colour, size, size class, category, MRP, season, is_pivotal |
| `dim_date` | 1 per calendar date | Derived from actual dates across all fact tables — no gaps, no pre-generated calendar |

### Fact Tables

| Table | Grain | Key Measures |
|---|---|---|
| `fact_sales` | Store × Product × Day | revenue, gross_amount, units_sold, units_returned, discount_amount |
| `fact_store_stock` | Store × Product (latest snapshot) | stock_on_hand, in_transit_qty, wh_available, units_needed, is_on_hold, is_clearance |
| `fact_warehouse_stock` | Day × Warehouse × Product | available_qty |
| `fact_replenishment_decisions` | Store × Product (run date) | decision, units_needed, order_qty, source_location |

### Intermediate Private MVs (fact_store_stock build chain)

`fact_store_stock` is computed via a chain of **6 private materialized views** (not visible in
Unity Catalog — pipeline-internal only):

```
silver.store_stock  ──► int_stock_keys      (join to dim_store + dim_product surrogate keys)
                              │
                              ▼
                        int_stock_rules     (add depth_target + max_units from silver)
                              │
         ┌────────────────────┤
         │                    │
int_in_transit_units   int_warehouse_stock_asof   (latest warehouse snapshot)
         │                    │
         └────────────────────▼
                        int_stock_movement  (add in_transit_qty + wh_available)
                              │
                              ▼
                        int_stock_flags     (add is_on_hold, hold_reason, is_clearance)
                              │
                              ▼
                        fact_store_stock    ← PUBLISHED to gold schema
```

---

## 7. Metric Views — AI BI Layer

**Schema:** `genie_agents_poc.gold`  
**Technology:** `CREATE VIEW ... WITH METRICS LANGUAGE YAML` — Unity Catalog Metric Views

Metric views define measures and dimensions **once** so that Databricks Genie (AI BI) can answer
natural-language questions without the user writing SQL. The business logic (e.g. what is
"sell-through") lives in the view definition, not in Genie instructions.

| View | Source Fact | Key Measures | Key Dimensions |
|---|---|---|---|
| `mv_retail_sales` | `fact_sales` + `dim_store` + `dim_product` + `fact_store_stock` | revenue, gross_revenue, units_sold, units_returned, return_rate, avg_discount_pct, clearance_revenue_share, sell_through | bill_date/week/month, region, state, store, grade, category, style, size, season |
| `mv_store_stock` | `fact_store_stock` + `dim_store` + `dim_product` | stock_out_rate, stocked_out_positions, units_on_hand, units_in_transit, warehouse_available_units, units_needed | snapshot_date, region, store, grade, store_status, category, style, size, is_on_hold, is_clearance |
| `mv_warehouse_stock` | `fact_warehouse_stock` + `dim_product` | available_units, warehouse_stock_out_rate, zero_stock_product_days | snapshot_date, warehouse, category, style, size, is_pivotal |

**Example Genie questions these enable:**

- *"Which region had the highest return rate last week?"*
- *"Show me stock-out rates by category for South stores."*
- *"What is WH-SOUTH's availability for size M jackets?"*
- *"Compare sell-through for Grade A vs Grade B stores this month."*

---

## 8. Replenishment Engine Rules (Summary)

The engine checks **10 rules in order** — the first matching rule is the final reason:

| # | Reason Code | Condition |
|---|---|---|
| 1 | `STORE_NOT_OPEN` | Store is on Hold or Closed |
| 2 | `STYLE_ON_HOLD` | Style is blocked from replenishment |
| 3 | `DEPTH_MET` | Stock on hand ≥ depth target |
| 4 | `COVERED_BY_IN_TRANSIT` | Stock on hand + in transit ≥ depth target |
| 5 | `CAPACITY_FULL` | Store already at category capacity |
| 6 | `WH_REPLENISH` | Warehouse has stock and store gets an allocation |
| 6b | `WH_ALLOCATED_TO_HIGHER_GRADE` | Warehouse has stock but Grade A/B stores took it all |
| 7 | `EOSS_NO_TRANSFER` | Warehouse empty + style is on clearance list |
| 8 | `IST_FROM_DONOR` | Warehouse empty, donor store in same state has surplus ≥ 2 |
| 9 | `REPLACE_CANDIDATE` | No source, but size is pivotal → planner must choose substitute |
| 10 | `NO_SOURCE_NON_PIVOTAL` | No source, non-pivotal size → no action taken |

**Depth targets** (units a store must hold per product):

| Store Grade | Pivotal Size (M, L, 32, 34) | Non-Pivotal |
|---|---|---|
| A | 3 | 2 |
| B | 2 | 1 |
| C | 1 | 1 |

---

## 9. Why-Agent — AI Explanation Layer

**Technology:** Databricks App (Dash UI) + Foundation Model API + Unity Catalog tool functions  
**URL:** `why-agent-madhubabu-duggirala-3578034636076694.aws.databricksapps.com`

The Why-Agent answers the question *"Why did store X not receive product Y today?"* in plain
English. It is designed to be used by planners who need to understand and override the engine's
decisions.

### Architecture

```
  Planner types question
         │
         ▼
  ┌──────────────────────────────────────────────────────────┐
  │  GUARDRAILS                                              │
  │  • Scope check: must be about replenishment/stock        │
  │  • Forbidden: blocks any attempt to read eval/answer-key │
  └────────────────────────┬─────────────────────────────────┘
                           │
                           ▼
  ┌──────────────────────────────────────────────────────────┐
  │  EXTRACT  store code (S1002) + EAN (8900000000440)       │
  └────────────────────────┬─────────────────────────────────┘
                           │
                           ▼
  ┌──────────────────────────────────────────────────────────┐
  │  DETERMINISTIC RULES CASCADE                             │
  │  Calls UC tool functions → applies rules 1-10 in order  │
  │  Produces: reason_code + plain-English explanation       │
  └────────────────────────┬─────────────────────────────────┘
                           │
                           ▼
  ┌──────────────────────────────────────────────────────────┐
  │  LLM WORDING  (databricks-meta-llama-3-3-70b-instruct)  │
  │  Rewrites the deterministic answer in 2-4 friendly       │
  │  sentences, grounded only in the numbers from the tools  │
  │  + relevant engine rules retrieved from Vector Search    │
  └────────────────────────┬─────────────────────────────────┘
                           │
                           ▼
  Answer + REASON_CODE returned to planner
```

### 9 UC Tool Functions (in `genie_agents_poc.agent_tools`)

| Function | What it returns |
|---|---|
| `get_replenishment_decision(store, ean)` | Decision, units_needed, order_qty, source_location |
| `get_store_profile(store)` | Name, city, state, region, grade, format, status, serving_warehouse |
| `get_product(ean)` | Style, colour, size, is_pivotal, MRP, season |
| `get_stock_position(store, ean)` | On-hand, in-transit, wh_available, depth_target, hold/clearance flags |
| `get_in_transit(store, ean)` | Open transfers dispatched but not yet received |
| `get_warehouse_stock(warehouse, ean)` | Last 14 days of warehouse availability |
| `get_style_flags(ean)` | Hold reason + clearance date if applicable |
| `get_store_capacity(store, category)` | Max units per category |
| `get_donor_candidates(store, ean)` | Donor stores in same state with surplus ≥ 2 |

### Vector Search

Engine rules (`engine_rules.md`) are **chunked by section** (one chunk per rule) and indexed in a
Databricks Vector Search index (`genie_agents_poc.agent_tools.engine_rules_index`) using the
`databricks-gte-large-en` embedding model. When the agent needs to explain a decision, the most
relevant rule sections are retrieved and injected into the LLM prompt to prevent hallucination.

---

## 10. Full Data Flow — End to End

```
Day 0 (one-time setup)
──────────────────────
01_generate_retail_data.py     → creates 11 CSV feeds in the landing volume
                               → creates 9 UC tool functions in agent_tools schema
00_setup_and_vector_search.py  → creates Vector Search endpoint + index on engine_rules.md

Daily / on-demand
─────────────────
New CSV files arrive in /Volumes/genie_agents_poc/bronze/landing/{feed}/
         │
         │ Lakeflow Declarative Pipeline (genie_agents_poc_pipeline)
         │
         ├─► BRONZE: Auto Loader ingests new files → 11 _raw streaming tables
         │
         ├─► SILVER: Type-cast, deduplicate, validate → 11 clean tables + quarantine
         │
         └─► GOLD:   Star schema MVs → 3 dims + 4 facts + 6 private int_ MVs
                                     + 3 metric views (run once via SQL warehouse)

User interaction
────────────────
Genie (AI BI)        reads mv_retail_sales, mv_store_stock, mv_warehouse_stock
Why-Agent App        reads gold tables + agent_tools UC functions + Vector Search index
                     → answers "why?" questions about replenishment decisions
```

---

## 11. Technology Stack

| Component | Technology |
|---|---|
| **Platform** | Databricks on AWS (Free Edition / serverless) |
| **Pipeline** | Lakeflow Declarative Pipelines (Python, serverless) |
| **Storage** | Delta Lake tables in Unity Catalog |
| **Raw files** | Unity Catalog Volume (`/Volumes/genie_agents_poc/bronze/landing/`) |
| **Ingestion** | Auto Loader (`cloudFiles` format, CSV with schema enforcement) |
| **IaC** | Databricks Asset Bundles (DABs) — `databricks.yml` + `resources/*.yml` |
| **AI BI** | Databricks Genie (metric views) |
| **LLM** | Foundation Model API — `databricks-meta-llama-3-3-70b-instruct` |
| **Embeddings** | `databricks-gte-large-en` via Vector Search |
| **Vector Search** | Databricks Vector Search (DELTA_SYNC, TRIGGERED) |
| **App** | Databricks Apps — Dash (Flask) web UI |
| **Language** | Python 3.12 |

---

## 12. Project Folder Structure

```
Genie_agents/                      ← Databricks Asset Bundle root
├── databricks.yml                 ← Bundle definition (catalog, schema variables)
├── pyproject.toml                 ← Python project config
├── metric_views.sql               ← Run once on SQL warehouse to create metric views
│
├── resources/
│   ├── Genie_agents_etl.pipeline.yml   ← Lakeflow pipeline definition
│   ├── sample_job.job.yml              ← Daily scheduler job
│   └── why_agent_app.app.yml           ← Databricks App definition
│
└── src/
    ├── Genie_agents_etl/          ← Medallion pipeline Python code
    │   └── transformations/
    │       ├── common.py          ← Shared config & helper functions
    │       ├── bronze/            ← 11 Auto Loader ingestion files
    │       ├── silver/            ← 13 silver transformation files
    │       └── gold/              ← 13 gold MV files (dims + facts + int_)
    │
    ├── agent/                     ← Why-Agent core logic
    │   ├── why_agent.py           ← WhyAgent class (Spark-enabled, MLflow tracing)
    │   ├── setup_vector_search.py ← One-time Vector Search setup module
    │   ├── 00_setup_and_vector_search.py  ← Setup notebook
    │   ├── 01_ask_why.py          ← Interactive demo notebook
    │   └── evaluate.py            ← Scoring vs 33-question answer key
    │
    ├── app/                       ← Databricks App (Spark-free)
    │   ├── why_agent_app.py       ← Dash UI + agent logic
    │   ├── app.yaml               ← App runtime config (warehouse ID, catalog)
    │   └── requirements.txt
    │
    └── Documentation/
        ├── BRIEF.md               ← 2-week exercise brief (business context)
        ├── engine_rules.md        ← Replenishment engine rules 1-10
        ├── 01_generate_retail_data.py   ← Synthetic data generator
        └── 00_free_edition_smoke_test.py
```

---

## 13. Key Design Decisions

### Why separate bronze / silver / gold?

| Layer | Principle | Benefit |
|---|---|---|
| Bronze | Never lose raw data | Can re-derive silver/gold at any time; audit trail |
| Silver | Single source of truth for business entities | All downstream consumers share one cleaned version |
| Gold | Optimised for query performance | Pre-aggregated, surrogate-keyed, join-ready |

### Why materialized views for gold instead of streaming tables?

Gold tables (`dim_*`, `fact_*`) are defined as `@dp.materialized_view`. This means they are
**fully recomputed** on each pipeline run, ensuring consistency when historical silver data is
corrected. Streaming tables in silver are append-only, which is correct for event data (POS
transactions, stock snapshots) but wrong for slowly-changing dimensions like `dim_store`.

### Why is pos_sales special?

POS sales is the only feed that:
- Can have **duplicate rows** (the system re-sends failed transactions)
- Must be **validated against reference data** (product and store must exist)
- Has **returns** mixed with sales (negative qty rows)

It is therefore the only feed with its own quarantine table and reconciliation check.

### Why deterministic fallback in the agent?

The LLM is used only for **natural-language wording**, not for the reasoning itself.
The replenishment engine rules are deterministic and encoded in Python. If the LLM fails
(API error, hallucination risk), the agent always falls back to the deterministic explanation.
This makes the agent **production-safe** without sacrificing explainability.

---

*Document generated from the live bundle state at `genie_agents_poc` on Databricks workspace `dbc-cb8a7372-40bf.cloud.databricks.com`.*
