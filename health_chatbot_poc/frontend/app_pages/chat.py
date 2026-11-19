import streamlit as st

from lib import api

user_id = st.session_state.user_id
thread_id = st.session_state.threads[user_id]
messages: list[dict] = st.session_state.chats.setdefault(thread_id, [])
name = next(u["name"] for u in api.users() if u["id"] == user_id)

ROUTE_ICON = {
    "lookup": ":material/search:",
    "insight": ":material/insights:",
    "coaching": ":material/fitness_center:",
    "deep_analysis": ":material/science:",
    "safety": ":material/emergency:",
}

SUGGESTIONS = {
    ":material/event: When is my next event?": "When is my next event and how many days do I have?",
    ":material/fitness_center: Suggest exercises for today": "Based on my recent data, suggest exercises I can do today and for the next 3 days.",
    ":material/insights: Ups and downs, last 6 months": "Analyze my past 6 months of data and give me the ups and downs with insights.",
    ":material/science: Does training load affect my HRV?": "Does a high training-load day affect my HRV the next morning? Quantify it.",
    ":material/bedtime: How was my sleep this week?": "How has my sleep been this week compared to my normal?",
}


def render_trace(meta: dict) -> None:
    v = meta.get("validation", {})
    tokens = sum(u.get("total_tokens", 0) for u in meta.get("usage", {}).values())
    label = (f"{meta['intent']} · {len(meta.get('tool_calls', []))} tool calls · "
             f"{meta['latency_ms'] / 1000:.1f}s · {tokens:,} tokens")
    with st.expander(label, icon=ROUTE_ICON.get(meta["intent"]), type="compact"):
        st.caption(f"**Router:** {meta.get('route_reason', '')}")
        for tc in meta.get("tool_calls", []):
            st.markdown(f"- `{tc['tool']}` `{tc['args']}`")
        if "numbers_checked" in v:
            msg = f"{v['numbers_checked']} numbers checked, grounded ratio {v['grounded_ratio']}"
            if v.get("repaired"):
                msg += f" · repaired (removed {v.get('unverified_before_repair')})"
            if v.get("unverified"):
                msg += f" · still unverified: {v['unverified']}"
            st.caption(f"**Validator:** {msg}")
        if meta.get("memories_used"):
            st.caption("**Memories used:** " + " | ".join(meta["memories_used"]))
        st.caption("**Usage by model:** " + ", ".join(
            f"{m}: {u.get('input_tokens', 0):,} in / {u.get('output_tokens', 0):,} out" for m, u in meta.get("usage", {}).items()))


st.subheader(f"Coach chat · {name}")

for msg in messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("meta"):
            render_trace(msg["meta"])

prompt = st.chat_input(f"Ask about {name.split()[0]}'s data...", submit_mode="disable")

if not messages:
    slot = st.empty()
    picked = slot.pills("Try asking", list(SUGGESTIONS), label_visibility="collapsed", key=f"sugg_{thread_id}")
    if picked:
        prompt = SUGGESTIONS[picked]
    if prompt:
        slot.empty()

if prompt:
    messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.status(":shimmer[Routing and analysing your data]", type="compact") as status:
            try:
                res = api.chat(user_id, thread_id, prompt)
            except api.BackendError as e:
                status.update(label="Request failed", state="error")
                st.error(str(e))
                messages.pop()
                st.stop()
            status.update(label=f"Answered via the {res['intent']} path in {res['latency_ms'] / 1000:.1f}s", state="complete")
        st.markdown(res["answer"])
        meta = {k: res[k] for k in ("intent", "route_reason", "tool_calls", "validation", "memories_used", "usage", "latency_ms")}
        render_trace(meta)
    messages.append({"role": "assistant", "content": res["answer"], "meta": meta})
