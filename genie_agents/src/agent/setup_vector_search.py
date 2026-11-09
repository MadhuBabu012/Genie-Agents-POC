"""One-time setup for the Why-agent's retrieval layer (Day 8 of the brief).

Chunks engine_rules.md into a Delta table and builds a Vector Search index
over it, so the agent can look up the rule it needs instead of carrying the
whole document in its prompt.

Idempotent: safe to re-run. Creates at most ONE endpoint (Free Edition
allows one Vector Search endpoint per account) and reuses it if present.

Run this once before using the agent:
    from setup_vector_search import setup
    setup()

The agent retrieves rules with:
    from setup_vector_search import lookup_rule
    lookup_rule("why was a pivotal size flagged for replacement?")
"""

import time

CATALOG = "genie_agents_poc"
SCHEMA = "agent_tools"
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.engine_rules_chunks"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.engine_rules_index"
VS_ENDPOINT_NAME = "genie-agents-poc-vs"          # the account's single endpoint
EMBEDDING_MODEL = "databricks-gte-large-en"

# Where engine_rules.md lives after a bundle deploy (first hit wins).
RULES_CANDIDATE_PATHS = [
    "/Workspace/Users/madhubabu.duggirala@simbustech.com/.bundle/Genie_agents/dev/files/src/Documentation/engine_rules.md",
    "/Workspace/Repos/Genie-Agents-POC/Genie_agents/src/Documentation/engine_rules.md",
    "/Workspace/Users/babyramya.yalagandula@simbustech.com/Genie and Agents/engine_rules.md",
]


def read_rules_text():
    """Return the text of engine_rules.md from the first path that exists."""
    for p in RULES_CANDIDATE_PATHS:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return f.read(), p
        except OSError:
            continue
    raise FileNotFoundError(
        "engine_rules.md not found in any of: " + ", ".join(RULES_CANDIDATE_PATHS)
    )


def chunk_rules(text):
    """Split the rules document into one chunk per markdown section.

    The document is naturally sectioned ('##' headings), which matches how
    the agent asks for rules ("what does rule 8 say about donors?").
    """
    chunks = []
    section = []
    title = "header"
    for line in text.splitlines():
        if line.startswith("## "):
            if section:
                chunks.append((title, "\n".join(section).strip()))
            title = line[3:].strip()
            section = [line]
        else:
            section.append(line)
    if section:
        chunks.append((title, "\n".join(section).strip()))
    return [
        (f"rule_{i:02d}", t, c) for i, (t, c) in enumerate(chunks, 1) if c
    ]


def write_chunks_table(spark, chunks):
    """(Re)create the chunks table with a primary key and change data feed,
    both required for a Delta Sync vector index."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    rows = [(cid, title, content) for cid, title, content in chunks]

    # DDL with NOT NULL (a plain DataFrame write leaves columns nullable,
    # which the PRIMARY KEY constraint rejects). Fully derived data, so
    # dropping and recreating on each run is safe.
    spark.sql(f"DROP TABLE IF EXISTS {CHUNKS_TABLE}")
    spark.sql(
        f"CREATE TABLE {CHUNKS_TABLE} ("
        "chunk_id STRING NOT NULL, "
        "section STRING NOT NULL, "
        "content STRING NOT NULL)"
    )
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("chunk_id", StringType(), False),
        StructField("section", StringType(), False),
        StructField("content", StringType(), False),
    ])
    spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(CHUNKS_TABLE)
    spark.sql(
        f"ALTER TABLE {CHUNKS_TABLE} SET TBLPROPERTIES "
        "(delta.enableChangeDataFeed = true)"
    )
    try:
        spark.sql(
            f"ALTER TABLE {CHUNKS_TABLE} ADD CONSTRAINT chunk_pk PRIMARY KEY (chunk_id)"
        )
    except Exception as e:  # idempotent: the constraint survives re-runs
        if "already exists" not in str(e):
            raise
    print(f"wrote {len(rows)} chunks to {CHUNKS_TABLE}")


def _endpoint_names(w):
    """Endpoint names, robust to SDK versions that return either a wrapper
    object (.endpoints) or a bare iterable from list_endpoints()."""
    res = w.vector_search_endpoints.list_endpoints()
    eps = getattr(res, "endpoints", None) or res
    return [e.name for e in eps]


def ensure_endpoint(w, timeout_min=10):
    """Create the vector search endpoint if needed and wait until ONLINE."""
    if VS_ENDPOINT_NAME in _endpoint_names(w):
        print(f"endpoint {VS_ENDPOINT_NAME} already exists")
    else:
        print(f"creating endpoint {VS_ENDPOINT_NAME} ...")
        from databricks.sdk.service import vectorsearch as _vs
        try:    # newer SDKs require the enum
            _type = _vs.EndpointType.STANDARD
        except AttributeError:  # older SDKs accept the string
            _type = "STANDARD"
        w.vector_search_endpoints.create_endpoint(
            name=VS_ENDPOINT_NAME, endpoint_type=_type
        )
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=VS_ENDPOINT_NAME)
        status = ep.endpoint_status
        # SDK versions return either a string or an EndpointStatus enum object
        state = getattr(getattr(status, "state", status), "value", status)
        print(f"  endpoint state: {state}")
        if state == "ONLINE":
            return
        time.sleep(30)
    raise TimeoutError(f"endpoint not ONLINE after {timeout_min} min")


def ensure_index(w, timeout_min=15):
    """Create the Delta Sync index (managed embeddings) if needed, trigger a
    sync and wait until it has indexed the chunks.

    TRIGGERED pipeline type: CONTINUOUS is not available on every workspace
    (e.g. Free Edition), and TRIGGERED keeps costs predictable anyway.
    """
    from databricks.sdk.service import vectorsearch as vs

    try:
        w.vector_search_indexes.get_index(index_name=INDEX_NAME)
        print(f"index {INDEX_NAME} already exists")
    except Exception:
        print(f"creating index {INDEX_NAME} ...")
        w.vector_search_indexes.create_index(
            name=INDEX_NAME,
            endpoint_name=VS_ENDPOINT_NAME,
            primary_key="chunk_id",
            index_type=vs.VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=vs.DeltaSyncVectorIndexSpecRequest(
                source_table=CHUNKS_TABLE,
                embedding_source_columns=[
                    vs.EmbeddingSourceColumn(
                        name="content",
                        embedding_model_endpoint_name=EMBEDDING_MODEL,
                    )
                ],
                pipeline_type=vs.PipelineType.TRIGGERED,
            ),
        )
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        status = w.vector_search_indexes.get_index(index_name=INDEX_NAME).status
        print(f"  index ready={status.ready} rows={status.indexed_row_count} "
              f"msg={status.message}")
        if status.ready:
            break
        time.sleep(20)
    else:
        raise TimeoutError(f"index not ready after {timeout_min} min")

    # A TRIGGERED index only picks up source changes when synced.
    w.vector_search_indexes.sync_index(index_name=INDEX_NAME)
    while time.time() < deadline:
        status = w.vector_search_indexes.get_index(index_name=INDEX_NAME).status
        print(f"  sync ready={status.ready} rows={status.indexed_row_count}")
        if status.indexed_row_count and status.indexed_row_count > 0:
            return
        time.sleep(20)
    raise TimeoutError(f"index did not index any rows after {timeout_min} min")


def setup(spark, w=None):
    """Run the whole one-time setup: chunks -> endpoint -> index."""
    if w is None:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
    text, path = read_rules_text()
    print(f"chunking {path}")
    write_chunks_table(spark, chunk_rules(text))
    ensure_endpoint(w)
    ensure_index(w)
    print("vector search setup complete")


def lookup_rule(query_text, num_results=3):
    """The agent's retrieval tool: nearest rule sections for a question."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    res = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["chunk_id", "section", "content"],
        query_text=query_text,
        num_results=num_results,
    )
    return [
        {"chunk_id": r[0], "section": r[1], "content": r[2]}
        for r in res.result.data_array
    ]


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    setup(spark)
