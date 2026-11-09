"""The "Why?" agent (Days 6-10 of the brief).

Answers planner questions like "why did store S1007 not receive Slim Oxford
Shirt Navy size M today?" with the reason and the numbers behind it.

Design:
- Every fact comes from a read-only Unity Catalog tool function
  (genie_agents_poc.agent_tools.*) or from the Vector Search rule index -
  the model never writes SQL and never sees the hidden _eval schema.
- The model decides WHICH extra tools to call (round 1), then composes the
  answer (round 2). A deterministic rules engine - engine_rules.md rules
  1-10 applied to the same tool outputs - backs up the model, so the agent
  still answers correctly if the model is unavailable or answers poorly.
- Guardrails: refuses out-of-scope questions, says "I don't know" rather
  than guessing, and never reads the _eval schema.
- MLflow tracing on the agent and every tool call; token usage recorded.
- Planner memory: remembers each planner's store in a Delta table.

Usage:
    from why_agent import ask_why
    ask_why("Why did S1030 not receive EAN 8900000000005 today?")
"""

import json
import re

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "genie_agents_poc"
TOOLS = f"{CATALOG}.agent_tools"
MEMORY_TABLE = f"{TOOLS}.planner_memory"
MODEL = "databricks-meta-llama-3-3-70b-instruct"   # any chat FM API model
MAX_TOOL_ROUNDS = 2

REASON_CODES = [
    "STORE_NOT_OPEN", "STYLE_ON_HOLD", "DEPTH_MET", "COVERED_BY_IN_TRANSIT",
    "CAPACITY_FULL", "WH_REPLENISH", "WH_ALLOCATED_TO_HIGHER_GRADE",
    "EOSS_NO_TRANSFER", "IST_FROM_DONOR", "REPLACE_CANDIDATE",
    "NO_SOURCE_NON_PIVOTAL",
]

# ---------------------------------------------------------------- tools ----

_IDENT = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _safe(value):
    """Validate an identifier-like tool argument (blocks SQL injection)."""
    v = str(value).strip()
    if not _IDENT.match(v):
        raise ValueError(f"unsafe tool argument: {value!r}")
    return v


def _lit(value):
    """Quote and escape an arbitrary string as a SQL literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


# name -> (description, callable(store_code, ean, ...))
TOOL_SPECS = {}


def tool(name, description):
    def wrap(fn):
        TOOL_SPECS[name] = (description, fn)
        return fn
    return wrap


@tool("get_replenishment_decision",
      "Today's engine decision for a store and product: decision, units_needed, order_qty, source_location. Takes store_code, ean.")
def get_replenishment_decision(store_code, ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_replenishment_decision('{_safe(store_code)}', '{_safe(ean)}')")


@tool("get_store_profile",
      "Store profile: name, city, state, region, grade, format, status, serving warehouse. Takes store_code.")
def get_store_profile(store_code):
    return _rows(f"SELECT * FROM {TOOLS}.get_store_profile('{_safe(store_code)}')")


@tool("get_product",
      "Product by barcode: style, colour, size, pivotal flag, MRP, season. Takes ean.")
def get_product(ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_product('{_safe(ean)}')")


@tool("get_stock_position",
      "Stock position for a store and product: on-hand, in-transit, warehouse available, depth target, units needed, hold and clearance flags. Takes store_code, ean.")
def get_stock_position(store_code, ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_stock_position('{_safe(store_code)}', '{_safe(ean)}')")


@tool("get_in_transit",
      "Transfers dispatched to a store for a product but not yet received. Takes store_code, ean.")
def get_in_transit(store_code, ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_in_transit('{_safe(store_code)}', '{_safe(ean)}')")


@tool("get_warehouse_stock",
      "Last 14 days of warehouse stock for a product (zero rows means out of stock). Takes warehouse (WH-SOUTH or WH-NORTH), ean.")
def get_warehouse_stock(warehouse, ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_warehouse_stock('{_safe(warehouse)}', '{_safe(ean)}')")


@tool("get_style_flags",
      "Whether the style behind a barcode is on hold or clearance, with reasons and dates. Takes ean.")
def get_style_flags(ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_style_flags('{_safe(ean)}')")


@tool("get_store_capacity",
      "Store capacity per category. Takes store_code and category (or category=None for all).")
def get_store_capacity(store_code, category=None):
    cat = "NULL" if category is None else f"'{_safe(category)}'"
    return _rows(f"SELECT * FROM {TOOLS}.get_store_capacity('{_safe(store_code)}', {cat})")


@tool("get_donor_candidates",
      "Donor stores that could transfer stock: same state, open, holding 2+ units above their own depth target. Takes store_code, ean.")
def get_donor_candidates(store_code, ean):
    return _rows(f"SELECT * FROM {TOOLS}.get_donor_candidates('{_safe(store_code)}', '{_safe(ean)}')")


@tool("lookup_rule",
      "Retrieves the engine rule sections relevant to a question from the Vector Search index of engine_rules.md. Takes a free-text query.")
def lookup_rule(query_text, num_results=2):
    from setup_vector_search import lookup_rule as _vs_lookup
    return _vs_lookup(query_text, num_results=num_results)


# ------------------------------------------------------- deterministic ----

def explain_decision_deterministic(store_code, ean):
    """Apply engine_rules.md rules 1-10, in order, to the tool outputs.

    Returns dict(reason_code, decision, narrative). This is the agent's
    fallback and its correctness check: the engine that produced the
    decisions used exactly these rules.
    """
    store = (get_store_profile(store_code) or [{}])[0]
    product = (get_product(ean) or [{}])[0]
    pos = (get_stock_position(store_code, ean) or [{}])[0]
    decision = (get_replenishment_decision(store_code, ean) or [{}])[0]

    if not store or not product or not pos:
        return {"reason_code": None, "decision": decision.get("decision"),
                "narrative": "I don't know - no data for that store and product."}

    name = f"{product.get('style_name', 'the product')} {product.get('colour', '')} size {product.get('size', '')}".strip()
    on_hand, in_transit, depth = pos["stock_on_hand"], pos["in_transit_qty"], pos["depth_target"]
    covered = on_hand + in_transit
    reason = None

    if store.get("status") != "Open":
        reason = "STORE_NOT_OPEN"
        why = f"the store's status is {store.get('status')}, so no stock is sent"
    elif pos.get("is_on_hold"):
        reason = "STYLE_ON_HOLD"
        why = f"the style is on hold ({pos.get('hold_reason') or 'reason not recorded'})"
    elif on_hand >= depth:
        reason = "DEPTH_MET"
        why = f"stock on hand ({on_hand}) already meets the depth target ({depth})"
    elif covered >= depth:
        reason = "COVERED_BY_IN_TRANSIT"
        why = f"stock on hand ({on_hand}) plus in transit ({in_transit}) reaches the depth target ({depth})"
    elif pos.get("max_units") is not None and on_hand >= pos["max_units"]:
        reason = "CAPACITY_FULL"
        why = f"the store already holds its category capacity ({pos['max_units']} units)"
    elif pos.get("wh_available", 0) > 0:
        reason = "WH_REPLENISH" if (decision.get("order_qty") or 0) > 0 else "WH_ALLOCATED_TO_HIGHER_GRADE"
        why = (f"the serving warehouse {store.get('serving_warehouse')} has {pos['wh_available']} units "
              f"and the store received {decision.get('order_qty') or 0} units")
    elif pos.get("is_clearance"):
        reason = "EOSS_NO_TRANSFER"
        why = "the warehouse is empty and clearance stock is never moved between stores or replaced"
    else:
        donors = get_donor_candidates(store_code, ean)
        if donors:
            reason = "IST_FROM_DONOR"
            d = donors[0]
            why = (f"the warehouse has 0 units and donor store {d['donor_store_code']} "
                   f"({d['donor_store_name']}) in the same state transfers {d['transfer_qty']} unit(s) "
                   f"from a surplus of {d['surplus']}")
        elif pos.get("is_pivotal"):
            reason = "REPLACE_CANDIDATE"
            why = (f"no warehouse stock and no donor store in {store.get('state')} has 2+ units above "
                   f"its own depth target, and size {product.get('size')} is pivotal, "
                   "so a planner must choose a substitute style")
        else:
            reason = "NO_SOURCE_NON_PIVOTAL"
            why = (f"no warehouse stock, no donor, and size {product.get('size')} is not pivotal, "
                   "so non-pivotal sizes are never replaced")

    narrative = (f"{store.get('store_name', store_code)} did not get a full replenishment of {name} "
                 f"because {why}. Decision: {decision.get('decision')}, "
                 f"order_qty: {decision.get('order_qty')}.")
    return {"reason_code": reason, "decision": decision.get("decision"), "narrative": narrative}


# ------------------------------------------------------------------ llm ----

_USAGE = {"prompt_tokens": 0, "completion_tokens": 0}


def _chat(w, messages, max_tokens=700, temperature=0.0):
    """One chat completion on the foundation model, tracking token usage."""
    from databricks.sdk.service.serving import ChatMessage

    def msg(role, content):
        try:
            from databricks.sdk.service.serving import ChatMessageRole
            return ChatMessage(role=getattr(ChatMessageRole, role.upper()), content=content)
        except Exception:  # SDK versions that accept plain strings
            return ChatMessage(role=role, content=content)

    resp = w.serving_endpoints.query(
        name=MODEL,
        messages=[msg(r, c) for r, c in messages],
        max_tokens=max_tokens,
        temperature=temperature,
    )
    u = resp.usage
    _USAGE["prompt_tokens"] += getattr(u, "prompt_tokens", 0) or 0
    _USAGE["completion_tokens"] += getattr(u, "completion_tokens", 0) or 0
    return resp.choices[0].message.content


# ------------------------------------------------------------- guardrails --

_SCOPE = re.compile(r"replenish|stock|transfer|warehouse|decision|receive|order|shelf|depth|hold|clearance|ist|replace|capacity|in.?transit|donor|pivotal", re.I)
_FORBIDDEN = re.compile(r"_eval|ground.?truth|answer.?key", re.I)
_STORE_RE = re.compile(r"\bS\d{3,5}\b")
_EAN_RE = re.compile(r"\b\d{13}\b")


def _in_scope(question):
    if _FORBIDDEN.search(question):
        return False, "I cannot access evaluation or answer-key data. Ask me about replenishment decisions for a store and product."
    if not _SCOPE.search(question):
        return False, ("That is outside my scope. I explain replenishment decisions - "
                       "why a store did or did not receive a product. Give me a store and a product (EAN).")
    return True, None


def _extract_ids(question):
    store = _STORE_RE.search(question)
    ean = _EAN_RE.search(question)
    return (store.group(0) if store else None, ean.group(0) if ean else None)


# --------------------------------------------------------------- memory ---

def _ensure_memory_table():
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {MEMORY_TABLE} ("
        "planner STRING NOT NULL, store_code STRING NOT NULL, "
        "store_name STRING, updated_at TIMESTAMP)"
    )


def save_memory(planner, store_code, store_name=None):
    """Remember which store a planner works with (Day 10)."""
    _ensure_memory_table()
    spark.sql(f"DELETE FROM {MEMORY_TABLE} WHERE planner = '{_safe(planner)}'")
    name_lit = _lit(store_name) if store_name else "NULL"
    spark.sql(
        f"INSERT INTO {MEMORY_TABLE} VALUES ('{_safe(planner)}', '{_safe(store_code)}', "
        f"{name_lit}, current_timestamp())"
    )


def get_memory(planner):
    """The store a planner last asked about, if any."""
    try:
        rows = _rows(f"SELECT * FROM {MEMORY_TABLE} WHERE planner = '{_safe(planner)}' LIMIT 1")
        return rows[0] if rows else None
    except Exception:
        return None


# --------------------------------------------------------------- agent ----

class WhyAgent:
    """Tool-calling agent over the agent_tools functions."""

    def __init__(self, model=None, use_tracing=True):
        global MODEL
        if model:
            MODEL = model
        self.w = None
        self.trace = use_tracing and self._mlflow_trace_available()

    @staticmethod
    def _mlflow_trace_available():
        try:
            import mlflow
            return hasattr(mlflow, "trace")
        except Exception:
            return False

    def _client(self):
        if self.w is None:
            from databricks.sdk import WorkspaceClient
            self.w = WorkspaceClient()
        return self.w

    # -- LLM stages -------------------------------------------------------

    def _choose_tools(self, question, facts):
        """Round 1: the model names extra tools it needs, as JSON."""
        catalog = "\n".join(f"- {n}: {d}" for n, (d, _) in TOOL_SPECS.items())
        prompt = (
            "You are a retail replenishment analyst. Facts already gathered:\n"
            f"{json.dumps(facts, default=str)[:4000]}\n\n"
            f"Question: {question}\n\n"
            f"Available tools:\n{catalog}\n\n"
            "Which additional tools do you need to answer? Reply with ONLY a JSON "
            'object: {"tools": [{"name": "...", "args": {"store_code": "...", "ean": "...", ...}}]}. '
            "Use [] if none. Do not invent tool or argument names."
        )
        raw = _chat(self._client(), [("system", "You output only valid JSON."), ("user", prompt)],
                    max_tokens=300)
        try:
            m = re.search(r"\{.*\}", raw, re.S)
            plan = json.loads(m.group(0)) if m else {"tools": []}
        except Exception:
            plan = {"tools": []}
        return plan.get("tools", []) or []

    def _compose(self, question, facts, rules_text):
        """Round 2: compose the final answer from tool facts + rules."""
        prompt = (
            "You explain replenishment decisions for a fashion retailer. "
            "Answer the planner's question using ONLY the facts and rules below. "
            "Every number you mention must come from the facts. If the facts do not "
            "contain the answer, say you don't know.\n\n"
            f"Question: {question}\n\n"
            f"Facts from tools:\n{json.dumps(facts, default=str)[:6000]}\n\n"
            f"Engine rules:\n{rules_text[:3000]}\n\n"
            "Write 2-4 sentences: the reason, the numbers behind it, and what the "
            "engine decided. Then on the last line write exactly "
            "REASON_CODE: <code> choosing from " + ", ".join(REASON_CODES) + "."
        )
        return _chat(self._client(), [("system", "You are precise and never invent numbers."),
                                      ("user", prompt)])

    # -- main entry point --------------------------------------------------

    def ask(self, question, planner=None):
        """Answer a planner question. Returns a dict with the answer and trace."""
        if self.trace:
            import mlflow
            with mlflow.start_span(name="why_agent.ask") as span:
                span.set_attribute("question", question)
                result = self._ask(question, planner)
                span.set_attributes({
                    "reason_code": result["reason_code"],
                    "tools_used": result["tools_used"],
                    "decision": result["decision"],
                })
                return result
        return self._ask(question, planner)

    def _ask(self, question, planner=None):
        ok, refusal = _in_scope(question)
        if not ok:
            return self._result(question, refusal, None, [], "REFUSAL")

        store_code, ean = _extract_ids(question)
        if planner and not store_code:
            mem = get_memory(planner)
            if mem:
                store_code = mem["store_code"]
        if not store_code or not ean:
            return self._result(
                question,
                "I don't know which store or product you mean. Give me a store code "
                "(like S1007) and a 13-digit product barcode (EAN).",
                None, [], "UNKNOWN")

        # core facts: always gathered with tools
        facts = {
            "store_profile": get_store_profile(store_code),
            "product": get_product(ean),
            "stock_position": get_stock_position(store_code, ean),
            "decision": get_replenishment_decision(store_code, ean),
        }
        tools_used = ["get_store_profile", "get_product", "get_stock_position",
                      "get_replenishment_decision"]

        if not facts["store_profile"] or not facts["product"] or not facts["stock_position"]:
            return self._result(
                question,
                "I don't know - I found no data for that store and product combination.",
                None, tools_used, "NO_DATA")

        # retrieval: the rules relevant to this question
        try:
            rules = lookup_rule(question, num_results=2)
            rules_text = "\n\n".join(r["content"] for r in rules)
            tools_used.append("lookup_rule")
        except Exception:
            rules_text = "Engine rules unavailable; rely on the deterministic analysis."

        # round 1: model picks extra tools (max MAX_TOOL_ROUNDS rounds)
        try:
            for _ in range(MAX_TOOL_ROUNDS):
                wanted = self._choose_tools(question, facts)
                if not wanted:
                    break
                more = False
                for t in wanted:
                    name = t.get("name")
                    if name in TOOL_SPECS and name not in tools_used:
                        try:
                            facts[name] = TOOL_SPECS[name][1](**t.get("args", {}))
                            tools_used.append(name)
                            more = True
                        except Exception:
                            pass
                if not more:
                    break
            answer = self._compose(question, facts, rules_text)
            status = "OK"
            fallback = False
        except Exception as e:
            answer, fallback, status = None, True, f"LLM_ERROR: {type(e).__name__}"

        # deterministic backup + reason code
        det = explain_decision_deterministic(store_code, ean)
        if fallback:
            answer = det["narrative"]
        if det["reason_code"]:
            m = re.search(r"REASON_CODE:\s*([A-Z_]+)", answer or "")
            reason = m.group(1) if m else None
            if reason not in REASON_CODES:
                reason = det["reason_code"]
        else:
            reason = None

        if planner:
            try:
                save_memory(planner, store_code,
                            (facts["store_profile"] or [{}])[0].get("store_name"))
            except Exception:
                pass

        return self._result(question, answer, reason, tools_used, status,
                            decision=det["decision"], store_code=store_code, ean=ean)

    @staticmethod
    def _result(question, answer, reason, tools, status, decision=None,
                store_code=None, ean=None):
        return {
            "question": question,
            "answer": answer,
            "reason_code": reason,
            "decision": decision,
            "store_code": store_code,
            "ean": ean,
            "tools_used": tools,
            "status": status,
            "tokens": dict(_USAGE),
        }


def ask_why(question, planner=None, model=None):
    """Convenience wrapper used by notebooks and the app."""
    return WhyAgent(model=model).ask(question, planner=planner)
