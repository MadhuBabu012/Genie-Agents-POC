# Genie and Agents — 2-Week Hands-On Exercise

---

## 1. Why this exercise


In two weeks you will build the two things every data and AI client now asks for:

1. **Week 1 — a governed "ask the data" layer.** A gold layer, business metrics defined once, and a
   Genie agent that answers business questions correctly.
2. **Week 2 — an agent that explains decisions.** It uses tools to look up facts, applies business
   rules, and is **measured** against a known answer key, not just demoed.

Everything you build is meant to be reused, so write it that way: parameterised, tested and
documented. It mirrors the patterns in our retail replenishment product.

## 2. The business

**Simbus Fashion** is a fashion retailer with 40 stores in four regions, about 50 styles, and two
warehouses (`WH-SOUTH` serves South stores, `WH-NORTH` serves everyone else). One **product** is
one barcode: a style, in one colour, in one size.

Every morning a replenishment engine decides, for each product in each store, whether to send
stock from the warehouse, move it from another store, flag it for replacement, or do nothing. The
rules are in **`engine_rules.md`**.

**Something went wrong.** About six weeks ago sales of some shirts in the South collapsed for two
weeks. Leadership wants to know what happened and what it cost. Planners also keep asking *"why
did my store get this, and not that?"*, and nobody can answer quickly.

## 3. What you are given

| File | What it is |
|---|---|
| `BRIEF.md` | This document |
| `engine_rules.md` | The engine's business rules — your agent's reference |
| `00_free_edition_smoke_test.py` | Day-1 check that your workspace supports everything (about 15 minutes) |
| `01_generate_retail_data.py` | Generates all the raw data and the answer key |

Import both `.py` files as notebooks (**Workspace → Import**). Run the smoke test **one cell at a
time** — step 5 is a manual Genie check that must happen before the clean-up cell.

### The raw feeds

The generator writes CSV files to `/Volumes/<catalog>/<schema>/landing/<feed>/`:

| Feed | Grain | Notes |
|---|---|---|
| `stores` | store | grade, status (`Open`/`Hold`/`Closed`), region, state, serving warehouse |
| `products` | barcode (EAN) | style, colour, size, pivotal or not, MRP, season |
| `pos_sales` | transaction line | 90 days. **Negative quantity = a return** |
| `warehouse_stock_daily` | day × warehouse × EAN | |
| `store_stock` | store × EAN | snapshot on the as-of date |
| `in_transit` | transfer | dispatched to a store, not yet received |
| `style_holds` | style | styles blocked from replenishment |
| `clearance_styles` | style | end-of-season styles being sold off |
| `store_capacity` | store × category | maximum units |
| `depth_targets` | grade × size class | how many units a store should hold |
| `replenishment_decisions` | store × EAN | **today's engine output — the decision, but not the reason** |

**The raw data is deliberately dirty**: duplicate sales rows, barcodes missing from the product
master, stray spaces in store codes, and missing discounts. Finding and handling these is part of
the work. Returns are **not** dirty data — they are real business events.

### The answer key

The generator also writes `<catalog>.<schema>_eval.ground_truth` (the real reason for every
decision) and `<catalog>.<schema>_eval.eval_questions` (33 questions, three per reason).

> 🔒 **Your agent must never be able to read the `_eval` schema.** You use it to score the agent,
> the same way a client's real answer would be hidden. Reading it from the agent is cheating, and
> we will check the traces.

## 4. Free Edition rules — read before you start

Free Edition has a daily quota. **If you exceed it, compute stops for the rest of the day**, and
in extreme cases for the rest of the month.

- **Run the generator once.** It takes a few minutes. Do not loop it.
- **Keep evaluation runs to the 33 questions.** Do not run the set over and over while you debug —
  debug on 2–3 questions, then run the full set.
- **Vector Search allows one endpoint per account.** Create it once, reuse it.
- **Apps stop 24 hours after a deploy.** Restart before your demo.
- **Knowledge Assistant is not available** on Free Edition, so you build the agent yourselves.
  That is the better learning anyway.
- **Commit everything to the team Git repo daily.** The workspace is not the source of truth.
- Free Edition is for **non-commercial use**. Use only this synthetic data — never client data.

## 5. Week 1 — Gold layer, metrics and Genie

| Day | Work | Output |
|---|---|---|
| 1 | Run the smoke test; record results. Read this brief and `engine_rules.md`. Run the generator with your own schema name | Smoke-test results posted; raw feeds landed |
| 2 | **Bronze:** ingest all feeds with Auto Loader in a Lakeflow (declarative) pipeline. Keep every column as text | Bronze tables |
| 3 | **Silver:** cast types, trim keys, de-duplicate, send bad rows to a quarantine table with the rule they failed | Silver tables, quarantine table, counts that reconcile |
| 4 | **Gold:** a daily sales fact (units, revenue, returns), a stock-position table, and dimensions. **Write the grain of every table in its table comment** | Gold tables |
| 5 | **Metric views** for revenue, units sold, return rate, sell-through and stock-out rate. **Genie agent** over them, with instructions and sample questions | Genie agent that answers the 10 questions below |

### The 10 business questions Genie must answer correctly

1. What was total revenue last month, and how did it compare with the month before?
2. Which region grew fastest over the last 30 days?
3. Which category has the highest return rate?
4. What was the week with the lowest shirt revenue in the South?
5. During that week, how did South shirt sales in sizes M and L compare with other sizes?
6. Was the South warehouse out of stock for any products during that period? Which ones?
7. Roughly how much shirt revenue did the South lose in that period, compared with its normal rate?
8. Which grade-A stores sell the most per day?
9. What share of sales came from clearance styles, and at what average discount?
10. How many stores are on Hold or Closed today, and which ones?

**Questions 4–7 together are the root-cause story.** If Genie can walk a manager from "sales
dropped" to "the warehouse ran out of core sizes" to "this is what it cost", week 1 is done.

### Week 1 — definition of done

- [ ] All bronze/silver/gold objects created by the pipeline, nothing created by hand
- [ ] Row counts reconcile: raw = silver + quarantine (after de-duplication), and you can explain every difference
- [ ] Every gold table states its grain in its comment
- [ ] Metrics defined **once**, in metric views — not repeated in Genie instructions
- [ ] Genie answers at least 9 of the 10 questions correctly, and you can show the SQL it ran

## 6. Week 2 — The "Why?" agent

Planners will ask questions like:

> *"Why did Simbus Fashion Chennai Central (S1007) not receive any Slim Oxford Shirt, Navy, size M today?"*

Your agent must find the facts with **tools**, apply **`engine_rules.md`**, and answer with the
**reason and the numbers behind it**, for example:

> *"WH-SOUTH has 0 units of that shirt in size M, and no other store in Tamil Nadu has at least 2
> units above its own depth target. M is a pivotal size, so it has been flagged for replacement."*

| Day | Work | Output |
|---|---|---|
| 6 | Design the tools: 6–8 **read-only** Unity Catalog SQL functions over your gold/silver tables (store profile, product, stock position, in-transit, warehouse stock, holds and clearance, capacity, donor candidates) | Tool list with inputs and outputs, reviewed before you build |
| 7 | Build the tools and the agent (tool calling on a foundation model). Turn on **MLflow tracing** | Agent answers 3 questions; traces visible |
| 8 | **Retrieval:** chunk `engine_rules.md`, index it in Vector Search, and let the agent look up the rule it needs instead of pasting the whole document into the prompt | Retrieval tool working |
| 9 | **Evaluation:** score the agent on all 33 `eval_questions` with MLflow. Score at least *correct reason* and *every number came from a tool*. Fix the worst failure, re-run, compare | Two evaluation runs, before and after |
| 10 | **Guardrails and memory:** refuse questions outside its scope, say "I don't know" rather than guess, and keep a short memory of each planner's store (a small Delta or Lakebase table you write yourself). Deploy as a **Databricks App**. Final demo | Deployed app and demo |

### Week 2 — definition of done

- [ ] Every tool is read-only and takes a store code and/or barcode — no free-text SQL from the model
- [ ] The agent has no access to the `_eval` schema (show the grants)
- [ ] **Correct reason on at least 30 of 33 evaluation questions**
- [ ] **Zero numbers that did not come from a tool**
- [ ] Every answer is traceable in MLflow: which tools, which inputs, which outputs
- [ ] Cost per question recorded (tokens in and out)
- [ ] README: how to point the agent at a different catalog and schema

## 7. Demo day (end of day 10, 15 minutes per person)

1. The business problem, in two sentences
2. Architecture: one diagram, bronze to app
3. Genie walking through the South shirt root cause
4. The agent answering two evaluation questions, with the trace
5. **Evaluation scores before and after your best fix, and what you changed**
6. One thing that went wrong and how you found it

## 8. Ground rules

- **Keep it generic.** No hardcoded catalog, schema or store codes in your code.
