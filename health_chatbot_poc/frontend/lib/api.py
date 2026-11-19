"""Thin HTTP client for the FastAPI backend."""

from __future__ import annotations

import os
from pathlib import Path

import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")  # health_chatbot_poc/.env (no-op in Docker)

BASE = os.getenv("BACKEND_URL", "http://localhost:8000").rstrip("/")


class BackendError(RuntimeError):
    pass


def _req(method: str, path: str, timeout: int = 30, **kw):
    try:
        r = requests.request(method, f"{BASE}{path}", timeout=timeout, **kw)
    except requests.ConnectionError as e:
        raise BackendError(f"Backend not reachable at {BASE}. Is it running?") from e
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except ValueError:
            detail = r.text
        raise BackendError(f"{r.status_code}: {detail}")
    return r.json()


@st.cache_data(ttl=60, show_spinner=False)
def health() -> dict:
    return _req("GET", "/health", timeout=5)


@st.cache_data(ttl=300, show_spinner=False)
def users() -> list[dict]:
    return _req("GET", "/users")


@st.cache_data(ttl=300, show_spinner=False)
def profile(user_id: str) -> dict:
    return _req("GET", f"/users/{user_id}/profile")


@st.cache_data(ttl=300, show_spinner=False)
def series(user_id: str, metrics: tuple[str, ...], days: int) -> list[dict]:
    return _req("GET", f"/users/{user_id}/series", params={"metrics": ",".join(metrics), "days": days})


@st.cache_data(ttl=300, show_spinner=False)
def footprint(user_id: str) -> dict:
    return _req("GET", f"/users/{user_id}/data-footprint")


def brief(user_id: str, force: bool = False) -> dict:
    return _req("GET", f"/users/{user_id}/brief", params={"force": force}, timeout=120)


def chat(user_id: str, thread_id: str, message: str) -> dict:
    return _req("POST", "/chat", json={"user_id": user_id, "thread_id": thread_id, "message": message}, timeout=600)


def memories(user_id: str) -> list[dict]:
    return _req("GET", f"/users/{user_id}/memories")


def add_memory(user_id: str, text: str) -> None:
    _req("POST", f"/users/{user_id}/memories", json={"text": text}, timeout=60)


def delete_memory(user_id: str, memory_id: str) -> None:
    _req("DELETE", f"/users/{user_id}/memories/{memory_id}")


def reseed() -> dict:
    return _req("POST", "/admin/reseed", timeout=300)
