import streamlit as st

st.subheader("How it works")
st.caption("The LLM never sees raw sensor data. Code does the maths, and the LLM picks tools and explains the results.")

st.graphviz_chart(
    """
digraph G {
  rankdir=TB; node [shape=box, style="rounded,filled", fillcolor="#eef2ff", fontname="Helvetica", fontsize=11];
  vendors [label="WHOOP · Oura · Apple Watch · Polar\\n(vendor payloads, own units)", fillcolor="#f1f5f9"];
  ingest  [label="[1] Ingest + normalize\\ncanonical samples (tagged by source)"];
  facts   [label="[2] Health Facts (pandas, precomputed)\\ndaily · monthly · baselines · anomalies · change-points · readiness"];
  api     [label="[3] Metrics API / semantic layer\\n10 typed tools, compact JSON, explicit errors"];
  ctx     [label="[5] Context: profile card + mem0 memories"];
  router  [label="[4] Router (small model, structured output)\\n+ red-flag safety check", fillcolor="#fef3c7"];
  lookup  [label="lookup\\ncreate_agent · small"];
  insight [label="insight\\ncreate_agent · small"];
  coach   [label="coaching\\ncreate_agent · small"];
  deep    [label="deep_analysis\\nDeep Agent · large\\n→ data-scientist subagent\\n(pandas in sandbox)", fillcolor="#fce7f3"];
  val     [label="[7] Validator\\nevery number traced to tool output\\n(+1 repair pass)", fillcolor="#dcfce7"];
  mem     [label="mem0 (pgvector)\\nfacts extracted in background", fillcolor="#f1f5f9"];
  vendors -> ingest -> facts -> api;
  ctx -> router; router -> lookup; router -> insight; router -> coach; router -> deep;
  api -> lookup [style=dashed]; api -> insight [style=dashed]; api -> coach [style=dashed]; api -> deep [style=dashed];
  lookup -> val; insight -> val; coach -> val; deep -> val;
  val -> mem [style=dotted, label=" after response"];
}
"""
)

st.markdown(
    """
| Layer | Implementation in this POC |
|---|---|
| Canonical store | Postgres `samples` / `workouts`; vendor-specific scores kept separate |
| Health Facts | `facts/precompute.py`: rollups, 7/30/90-day baselines, z-scores, 30-day slope, anomalies (\\|z\\| ≥ 2), change-points, readiness tier |
| Semantic layer | `tools/metrics_tools.py`: metric catalog validation, user id from runtime context (never from the model) |
| Router | small model with structured output; deterministic red-flag check first |
| Paths | LangChain `create_agent` (lookup / insight / coaching), Deep Agents (deep analysis) |
| Code execution | `tools/sandbox.py`: isolated subprocess, no env/credentials, rlimits, timeout |
| Validator | regex number extraction vs evidence, tolerance ±2%, one LLM repair pass |
| Memory | mem0 + pgvector, written in a FastAPI background task (off the latency path) |
| Conversation state | LangGraph `PostgresSaver` per (user, thread) |
| Cost controls | model tiering, static (cacheable) system prompts, narrow toolsets, cached daily brief |
"""
)
