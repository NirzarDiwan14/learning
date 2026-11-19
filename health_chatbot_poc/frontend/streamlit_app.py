"""Streamlit entrypoint.  Run (from health_chatbot_poc/frontend/):  uv run streamlit run streamlit_app.py"""

import uuid

import streamlit as st

from lib import api, auth

st.set_page_config(page_title="Health coach POC", page_icon=":material/monitor_heart:", layout="wide")

if not auth.is_logged_in():
    auth.login_form()
    st.stop()

# ---------------------------------------------------------------- session defaults
st.session_state.setdefault("threads", {})  # user_id -> thread_id
st.session_state.setdefault("chats", {})  # thread_id -> [messages]

# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.caption(f"Signed in as **{st.session_state.email}**")
    try:
        health = api.health()
        users = api.users()
    except api.BackendError as e:
        st.error(str(e), title="Backend unavailable")
        st.stop()

    if not health.get("llm_configured"):
        st.warning("Set `OPENAI_API_KEY` in `health_chatbot_poc/.env` and restart the backend to enable chat.",
                   title="LLM not configured")

    labels = {u["id"]: f"{u['name']} · {u['sport']}" for u in users}
    user_id = st.selectbox("Athlete", list(labels), format_func=labels.get, key="user_id")
    sel = next(u for u in users if u["id"] == user_id)
    tier_icon = {"green": ":green[:material/circle:]", "amber": ":orange[:material/circle:]", "red": ":red[:material/circle:]"}
    st.markdown(f"Readiness today: {tier_icon.get(sel['tier'], '')} **{sel['tier']}** ({sel['score']}/100)")
    st.caption("Devices: " + ", ".join(sel["devices"]))

    st.session_state.threads.setdefault(user_id, uuid.uuid4().hex[:12])
    if st.button("New conversation", icon=":material/add_comment:", width="stretch"):
        st.session_state.threads[user_id] = uuid.uuid4().hex[:12]
        st.rerun()
    st.divider()
    if st.button("Reseed demo data", icon=":material/restart_alt:", width="stretch",
                 help="Regenerates 180 days of synthetic WHOOP/Oura/Apple/Polar data ending today and recomputes all Health Facts"):
        with st.spinner("Reseeding and recomputing..."):
            api.reseed()
        st.cache_data.clear()
        st.rerun()
    if st.button("Sign out", icon=":material/logout:", width="stretch"):
        auth.logout()

pages = [
    st.Page("app_pages/chat.py", title="Coach chat", icon=":material/chat:", default=True),
    st.Page("app_pages/dashboard.py", title="Dashboard", icon=":material/monitoring:"),
    st.Page("app_pages/memory.py", title="My memory", icon=":material/psychology:"),
    st.Page("app_pages/architecture.py", title="How it works", icon=":material/account_tree:"),
]
st.navigation(pages, position="top").run()
