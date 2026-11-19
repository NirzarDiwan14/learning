"""Minimal POC login (single admin account from .env). Not production auth."""

from __future__ import annotations

import hmac
import os

import streamlit as st


def _creds() -> tuple[str, str]:
    return os.getenv("APP_ADMIN_EMAIL", "admin@gmail.com"), os.getenv("APP_ADMIN_PASSWORD", "admin")


def is_logged_in() -> bool:
    return bool(st.session_state.get("logged_in"))


def login_form() -> None:
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.title(":material/monitor_heart: Health coach POC")
        st.caption("Wearable-data chatbot: precomputed health facts, a metrics API, a router, path agents and a validator.")
        with st.form("login", border=True):
            email = st.text_input("Email", placeholder="admin@gmail.com", key="login_email")
            password = st.text_input("Password", type="password", key="login_password")
            if st.form_submit_button("Sign in", type="primary", width="stretch"):
                exp_email, exp_pw = _creds()
                if hmac.compare_digest(email.strip().lower(), exp_email.lower()) and hmac.compare_digest(password, exp_pw):
                    st.session_state.logged_in = True
                    st.session_state.email = email.strip()
                    st.rerun()
                else:
                    st.error("Invalid email or password.")


def logout() -> None:
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    st.rerun()
