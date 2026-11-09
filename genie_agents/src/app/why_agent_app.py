"""The "Why?" agent as a Databricks App (Week 2, Day 10).

A small chat front-end: ask "why did store S1002 not receive EAN
8900000000440 today?" and get the reason with the numbers behind it.

Runs without Spark: tool facts come from the read-only UC tool functions
(genie_agents_poc.agent_tools.*) through a SQL warehouse via the
databricks-sdk statement API, rule text from the Vector Search index, and
the final wording from a foundation model. The deterministic rules cascade
(engine_rules.md rules 1-10) always backs up the model.
"""

import os
import re

import dash
from dash import Input, Output, State, dcc, html
from databricks.sdk import WorkspaceClient

CATALOG = os.environ["DATABRICKS_CATALOG"]
SCHEMA = os.environ["DATABRICKS_SCHEMA"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
MODEL = "databricks-meta-llama-3-3-70b-instruct"
INDEX = f"{CATALOG}.{SCHEMA}.engine_rules_index"

w = WorkspaceClient()

REASON_CODES = [
    "STORE_NOT_OPEN", "STYLE_ON_HOLD", "DEPTH_MET", "COVERED_BY_IN_TRANSIT",
    "CAPACITY_FULL", "WH_REPLENISH", "WH_ALLOCATED_TO_HIGHER_GRADE",
    "EOSS_NO_TRANSFER", "IST_FROM_DONOR", "REPLACE_CANDIDATE",
    "NO_SOURCE_NON_PIVOTAL",
]

_IDENT = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _safe(value):
    v = str(value).strip()
    if not _IDENT.match(v):
        raise ValueError(f"unsafe tool argument: {value!r}")
    return v


def _sql_rows(sql):
    """Run one read-only statement on the warehouse, return rows as dicts."""
    resp = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="50s"
    )
    if str(resp.status.state).split(".")[-1] != "SUCCEEDED":
        raise RuntimeError(f"query failed: {resp.status.error.message}")
    cols = [c.name for c in resp.manifest.schema.columns]
    out = []
    for arr in resp.result.data_array or []:
        vals = arr if isinstance(arr, list) else [arr]
        out.append({c: v for c, v in zip(cols, vals)})
    return out


def _int(x):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return 0


def _bool(x):
    return str(x).lower() == "true"


def _chat(messages, max_tokens=600):
    """One foundation-model chat completion."""
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
        temperature=0.0,
    )
    return resp.choices[0].message.content


def lookup_rule_text(query_text, num_results=2):
    """Nearest engine-rule sections for a question (best effort)."""
    try:
        res = w.vector_search_indexes.query_index(
            index_name=INDEX,
            columns=["chunk_id", "section", "content"],
            query_text=query_text,
            num_results=num_results,
        )
        return "\n\n".join(r[2] for r in res.result.data_array)
    except Exception:
        return "Engine rules unavailable."


# ------------------------------------------------------------ rule cascade --

def explain(store_code, ean):
    """Apply engine_rules.md rules 1-10 to the tool facts (deterministic)."""
    pos = _sql_rows(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.get_stock_position("
        f"'{_safe(store_code)}', '{_safe(ean)}')"
    )
    if not pos:
        return None, "I don't know - I found no data for that store and product."
    p = pos[0]
    store = _sql_rows(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.get_store_profile('{_safe(store_code)}')"
    )[0]
    decision = _sql_rows(
        f"SELECT * FROM {CATALOG}.{SCHEMA}.get_replenishment_decision("
        f"'{_safe(store_code)}', '{_safe(ean)}')"
    )[0]

    on_hand, in_transit = _int(p["stock_on_hand"]), _int(p["in_transit_qty"])
    depth = _int(p["depth_target"])
    covered = on_hand + in_transit
    reason, why = None, ""

    if store.get("status") != "Open":
        reason = "STORE_NOT_OPEN"
        why = f"the store's status is {store.get('status')}"
    elif _bool(p["is_on_hold"]):
        reason = "STYLE_ON_HOLD"
        why = f"the style is on hold ({p.get('hold_reason')})"
    elif on_hand >= depth:
        reason = "DEPTH_MET"
        why = f"stock on hand ({on_hand}) already meets the depth target ({depth})"
    elif covered >= depth:
        reason = "COVERED_BY_IN_TRANSIT"
        why = (f"stock on hand ({on_hand}) plus in transit ({in_transit}) "
               f"reaches the depth target ({depth})")
    elif p.get("max_units") is not None and on_hand >= _int(p["max_units"]):
        reason = "CAPACITY_FULL"
        why = f"the store already holds its category capacity ({p['max_units']} units)"
    elif _int(p["wh_available"]) > 0:
        reason = "WH_REPLENISH" if _int(decision.get("order_qty") or 0) > 0 else "WH_ALLOCATED_TO_HIGHER_GRADE"
        why = (f"the serving warehouse {store.get('serving_warehouse')} has "
               f"{p['wh_available']} units and the store received "
               f"{decision.get('order_qty') or 0} units")
    elif _bool(p["is_clearance"]):
        reason = "EOSS_NO_TRANSFER"
        why = "the warehouse is empty and clearance stock is never transferred or replaced"
    else:
        donors = _sql_rows(
            f"SELECT * FROM {CATALOG}.{SCHEMA}.get_donor_candidates("
            f"'{_safe(store_code)}', '{_safe(ean)}')"
        )
        if donors:
            reason = "IST_FROM_DONOR"
            d = donors[0]
            why = (f"the warehouse has 0 units and donor store {d['donor_store_code']} "
                   f"({d['donor_store_name']}) in the same state transfers "
                   f"{d['transfer_qty']} unit(s) from a surplus of {d['surplus']}")
        elif _bool(p["is_pivotal"]):
            reason = "REPLACE_CANDIDATE"
            why = (f"no warehouse stock, no donor with 2+ units above its own depth "
                   f"target, and size {p.get('size')} is pivotal, so a planner "
                   "must choose a substitute style")
        else:
            reason = "NO_SOURCE_NON_PIVOTAL"
            why = (f"no warehouse stock, no donor, and size {p.get('size')} is not "
                   "pivotal, so non-pivotal sizes are never replaced")

    det = (f"{store.get('store_name', store_code)}: {why}. The engine's decision "
           f"was {decision.get('decision')} with order_qty {decision.get('order_qty')}.")
    return reason, det


# -------------------------------------------------------------- guardrails --

_SCOPE = re.compile(
    r"replenish|stock|transfer|warehouse|decision|receive|order|depth|hold|"
    r"clearance|ist|replace|capacity|in.?transit|donor|pivotal", re.I)
_FORBIDDEN = re.compile(r"_eval|ground.?truth|answer.?key", re.I)


def answer(question):
    """The app's agent: guardrails -> tools -> rule cascade -> LLM wording."""
    question = (question or "").strip()
    if _FORBIDDEN.search(question):
        return "I cannot access evaluation or answer-key data. Ask me about " \
               "replenishment decisions for a store and product."
    if not _SCOPE.search(question):
        return ("That is outside my scope. I explain replenishment decisions - "
                "why a store did or did not receive a product. Give me a store "
                "and a 13-digit product barcode (EAN).")

    store = re.search(r"\bS\d{3,5}\b", question)
    ean = re.search(r"\b\d{13}\b", question)
    if not store or not ean:
        return ("I don't know which store or product you mean. Give me a store "
                "code (like S1002) and a 13-digit product barcode (EAN).")
    store_code, ean_code = store.group(0), ean.group(0)

    reason, det = explain(store_code, ean_code)
    if reason is None:
        return det

    # Let the model phrase it, grounded in the deterministic analysis + rules.
    try:
        rules = lookup_rule_text(question)
        composed = _chat([
            ("system", "You are precise and never invent numbers."),
            ("user",
             f"Question: {question}\n\n"
             f"Deterministic analysis: {det}\n"
             f"Reason code: {reason}\n\n"
             f"Engine rules:\n{rules[:3000]}\n\n"
             "Rewrite the analysis in 2-4 friendly sentences. Use ONLY the "
             "numbers above. End with the line: REASON_CODE: " + reason),
        ])
        if reason in composed:
            return composed
    except Exception:
        pass
    return det + f"\nREASON_CODE: {reason}"


# -------------------------------------------------------------------- UI ---

app = dash.Dash(__name__, title="Why-agent")

app.layout = html.Div(
    style={"maxWidth": "760px", "margin": "40px auto", "fontFamily": "sans-serif"},
    children=[
        html.H2("Why-agent - replenishment explained"),
        html.P("Ask why a store did or did not receive a product, e.g. "
               "\"Why did store S1002 not receive EAN 8900000000440 today?\""),
        dcc.Input(
            id="question", type="text", value="",
            placeholder="store code (S....) + product barcode (13 digits)",
            style={"width": "100%", "padding": "8px", "marginBottom": "10px"},
        ),
        html.Button("Ask", id="ask", n_clicks=0,
                    style={"padding": "8px 24px"}),
        html.Div(id="answer", style={"whiteSpace": "pre-wrap", "marginTop": "20px",
                                     "lineHeight": "1.6"}),
    ],
)


@app.callback(
    Output("answer", "children"),
    Input("ask", "n_clicks"),
    State("question", "value"),
    prevent_initial_call=True,
)
def on_ask(n, question):
    if not question:
        return ""
    try:
        return answer(question)
    except Exception as e:
        return f"Something went wrong: {type(e).__name__}. Try another question."


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
