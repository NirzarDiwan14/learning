import streamlit as st

from lib import api

user_id = st.session_state.user_id
st.subheader("My memory")
st.caption("Long-term facts the coach remembers across conversations, extracted by mem0 after each chat turn "
           "and stored in pgvector. You can view, add and delete them, like WHOOP's My Memory.")

with st.form("add_memory", clear_on_submit=True, border=True):
    c = st.container(horizontal=True, vertical_alignment="bottom")
    text = c.text_input("Add a memory", placeholder="e.g. I prefer training in the morning before work")
    if c.form_submit_button("Add", icon=":material/add:") and text.strip():
        try:
            api.add_memory(user_id, text.strip())
            st.toast("Memory saved")
        except api.BackendError as e:
            st.error(str(e))

try:
    mems = api.memories(user_id)
except api.BackendError as e:
    st.error(str(e))
    st.stop()

if not mems:
    st.info("No memories yet. Chat with the coach (e.g. mention a niggle, a schedule change or a preference) "
            "and they'll appear here a few seconds later.")
for m in mems:
    with st.container(border=True, horizontal=True, vertical_alignment="center"):
        st.markdown(m["memory"], width="stretch")
        if st.button("Delete", icon=":material/delete:", key=f"del_{m['id']}"):
            api.delete_memory(user_id, m["id"])
            st.rerun()
