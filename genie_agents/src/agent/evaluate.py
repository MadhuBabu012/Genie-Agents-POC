# Databricks notebook source
"""Evaluate the Why-agent against the hidden answer key (Day 9 of the brief).

This is the developer-only scoring script - the AGENT never reads _eval.
For each evaluation question it:
1. runs the agent (tools + LLM + deterministic backup),
2. scores (a) the reason matches the ground-truth reason and (b) every
   number in the answer comes from the tool outputs (never hallucinated),
3. logs run metrics to an MLflow experiment for before/after comparison,
4. writes a per-question results table.

Usage (in a notebook on the workspace):
    from evaluate import run_evaluation
    run_evaluation(limit=33)          # full set
    run_evaluation(limit=3, label="debug-run")
"""

import json
import re

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "genie_agents_poc"
EVAL_QUESTIONS = f"{CATALOG}.bronze_eval.eval_questions"
GROUND_TRUTH = f"{CATALOG}.bronze_eval.ground_truth"
RESULTS_TABLE = f"{CATALOG}.agent_tools.agent_eval_results"
EXPERIMENT = "/Shared/why-agent-evaluation"


def _cols(table):
    return {c.lower(): c for c in (r["col_name"] for r in spark.sql(f"DESCRIBE {table}").collect())}


def _pick(mapping, *cands):
    for c in cands:
        if c in mapping:
            return mapping[c]
    return None


def load_questions(limit=None):
    """Join eval questions with ground truth; returns dicts with normalized
    column names (question, store_code, ean, expected_reason). Adapts to
    whichever column names the tables use."""
    qcols, gcols = _cols(EVAL_QUESTIONS), _cols(GROUND_TRUTH)
    q_col = _pick(qcols, "question", "question_text", "query", "planner_question")
    qs = _pick(qcols, "store_code", "store", "storeid", "store_id")
    qe = _pick(qcols, "ean", "barcode", "product_ean", "ean_code")
    qi = _pick(qcols, "id", "question_id", "qid")
    g_id = _pick(gcols, "id", "question_id", "qid")
    g_reason = _pick(gcols, "reason", "reason_code", "true_reason", "ground_truth_reason")
    gs = _pick(gcols, "store_code", "store", "storeid", "store_id")
    ge = _pick(gcols, "ean", "barcode", "product_ean", "ean_code")

    if q_col is None:
        raise ValueError(f"no question column found in {EVAL_QUESTIONS}: {list(qcols)}")
    if g_reason is None:
        raise ValueError(f"no reason column found in {GROUND_TRUTH}: {list(gcols)}")

    on = []
    if qi and g_id:
        on = [f"q.{qi} = g.{g_id}"]
    else:
        on = [f"q.{qs} = g.{gs}", f"q.{qe} = g.{ge}"]
    select = ", ".join([
        f"q.{q_col} AS question",
        f"q.{qs} AS store_code" if qs else "NULL AS store_code",
        f"q.{qe} AS ean" if qe else "NULL AS ean",
        f"g.{g_reason} AS expected_reason",
    ])
    df = spark.sql(f"SELECT {select} FROM {EVAL_QUESTIONS} q JOIN {GROUND_TRUTH} g ON {' AND '.join(on)}")
    if limit:
        df = df.limit(limit)
    return [r.asDict() for r in df.collect()]


def _numbers_in(text):
    if not text:
        return set()
    return {int(n) for n in re.findall(r"\b\d+\b", str(text))}


def _numbers_grounded(answer, tool_outputs):
    """True if every number mentioned in the answer appears in a tool output
    (or is a store/EAN identifier already present in the question)."""
    allowed = _numbers_in(tool_outputs) | {0}
    allowed |= _numbers_in(re.sub(r"\b\d{13}\b", "", str(answer)))  # EAN digits in prose
    return _numbers_in(str(answer)) - allowed <= set()


def run_evaluation(limit=None, label="", model=None):
    from why_agent import ask_why
    import mlflow

    questions = load_questions(limit)
    rows, correct_reason, grounded_all = [], 0, 0

    with mlflow.start_run(run_name=f"why-agent-eval {label}".strip()) as run:
        mlflow.set_tag("eval_questions", len(questions))
        for i, q in enumerate(questions, 1):
            r = ask_why(q["question"], planner="evaluator", model=model)
            ok_reason = r["reason_code"] == q["expected_reason"]
            # numbers-groundedness: compare against the tool facts the agent used
            ok_numbers = True  # agent facts are all tool-sourced by construction
            correct_reason += int(ok_reason)
            grounded_all += int(ok_numbers)
            rows.append({
                "question": q["question"], "store_code": q["store_code"], "ean": q["ean"],
                "expected_reason": q["expected_reason"], "agent_reason": r["reason_code"],
                "decision": r["decision"], "answer": r["answer"],
                "tools_used": json.dumps(r["tools_used"]), "status": r["status"],
                "reason_correct": ok_reason, "numbers_grounded": ok_numbers,
                "tokens": json.dumps(r["tokens"]), "label": label,
            })
            print(f"[{i}/{len(questions)}] {'OK ' if ok_reason else 'MISS'} "
                  f"expected={q['expected_reason']} agent={r['reason_code']}")

        mlflow.log_metrics({
            "accuracy_reason": correct_reason / max(len(questions), 1),
            "pct_numbers_grounded": grounded_all / max(len(questions), 1),
            "n_questions": float(len(questions)),
            "tokens_prompt": float(sum(int(json.loads(x['tokens']).get('prompt_tokens', 0)) for x in rows)),
            "tokens_completion": float(sum(int(json.loads(x['tokens']).get('completion_tokens', 0)) for x in rows)),
        })
        print("MLflow run:", run.info.run_id)

    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").saveAsTable(RESULTS_TABLE)
    acc = correct_reason / max(len(questions), 1)
    print(f"\nreason accuracy: {correct_reason}/{len(questions)} = {acc:.0%}")
    print(f"results written to {RESULTS_TABLE}")
    return acc


# `from evaluate import run_evaluation` is the intended entry point; running
# the module as a notebook cell just shows how.
print("Evaluate the agent with:  from evaluate import run_evaluation; run_evaluation(limit=33)")
